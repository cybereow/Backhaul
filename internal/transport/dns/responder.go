package dnsx

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"
)

// envelopeOverhead is the fixed cost of the query/response framing (magic+ver+
// fields + MAC) that wraps the test payload before a codec encodes it.
const envelopeOverhead = respHdr + macLen

// patternBytes returns n deterministic pseudo-random bytes seeded by seed. Both
// ends generate the same sequence, so the prober can detect any byte a resolver
// mangles in transit (0x20 games, TXT re-escaping, truncation).
func patternBytes(seed uint64, n int) []byte {
	out := make([]byte, n)
	x := seed | 1 // avoid the all-zero xorshift fixed point
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x)
	}
	return out
}

// Responder is the inside authoritative server for one tunnel domain. It never
// recurses or forwards: a query whose QNAME does not carry a MAC-valid payload
// under the domain gets NXDOMAIN. See design §4.
type Responder struct {
	domain string
	key    []byte
	logger *logrus.Logger

	// Handler is an optional hook for the tunnel session.
	// If nil, the responder echoes patternBytes (diagnostic mode).
	Handler func(sid uint32, flags byte, in []byte, maxResp int) (out []byte, outFlags byte)
}

func NewResponder(domain, key string, logger *logrus.Logger) *Responder {
	return &Responder{domain: dns.Fqdn(domain), key: []byte(key), logger: logger}
}

// Serve listens on addr for both UDP and TCP until ctx is cancelled.
func (r *Responder) Serve(ctx context.Context, addr string) error {
	mux := dns.NewServeMux()
	mux.HandleFunc(r.domain, r.handle)

	servers := []*dns.Server{
		{Addr: addr, Net: "udp", Handler: mux},
		{Addr: addr, Net: "tcp", Handler: mux},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(servers))
	for _, s := range servers {
		wg.Add(1)
		go func(s *dns.Server) {
			defer wg.Done()
			r.logger.Infof("dns responder listening on %s/%s for %s", addr, s.Net, r.domain)
			if err := s.ListenAndServe(); err != nil {
				errCh <- fmt.Errorf("%s: %w", s.Net, err)
			}
		}(s)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		for _, s := range servers {
			_ = s.Shutdown()
		}
		wg.Wait()
		return err
	}
	for _, s := range servers {
		_ = s.Shutdown()
	}
	wg.Wait()
	return nil
}

func (r *Responder) handle(w dns.ResponseWriter, req *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true
	m.Compress = true // repeated owner names (esp. many A/AAAA fragments) must be compressed

	if len(req.Question) != 1 {
		m.Rcode = dns.RcodeFormatError
		_ = w.WriteMsg(m)
		return
	}
	q := req.Question[0]

	_, inTCP := w.RemoteAddr().(*net.TCPAddr)
	rrs, err := r.answer(q, inTCP)
	if err != nil {
		// Not our payload (foreign name, bad MAC, garbage): behave like an
		// ordinary authoritative server with nothing to say.
		r.logger.Tracef("no answer for %q/%s: %v", q.Name, dns.TypeToString[q.Qtype], err)
		m.Rcode = dns.RcodeNameError
		_ = w.WriteMsg(m)
		return
	}
	m.Answer = rrs

	// Respect the resolver's advertised UDP buffer; miekg truncates+sets TC if
	// the message overflows, which is itself a useful signal to the prober.
	if opt := req.IsEdns0(); opt != nil {
		m.SetEdns0(opt.UDPSize(), false)
	}
	if err := w.WriteMsg(m); err != nil {
		r.logger.Tracef("write reply to %s: %v", w.RemoteAddr(), err)
	}
}

// answer decodes the query payload out of the QNAME, and re-encodes a response
// payload of the requested size into answer records of the question's type.
// inTCP is the transport the query arrived on, echoed back so the prober can
// see the resolver->inside hop.
func (r *Responder) answer(q dns.Question, inTCP bool) ([]dns.RR, error) {
	raw, err := decodeName(q.Name, r.domain)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, errNoAnswer // bare domain / no payload labels
	}
	qy, err := parseQuery(r.key, raw)
	if err != nil {
		return nil, err
	}

	c := codecByType(r.domain, q.Qtype)
	if c == nil {
		return nil, fmt.Errorf("dnsx: unsupported qtype %s", dns.TypeToString[q.Qtype])
	}

	// Cap the requested response payload to what this record type can carry in
	// one message, leaving room for the response envelope.
	respLen := int(qy.RespLen)
	if max := c.capacity() - envelopeOverhead; respLen > max {
		respLen = max
	}
	if respLen < 0 {
		respLen = 0
	}

	var respData []byte
	if r.Handler != nil && len(qy.Data) >= sessionFrame {
		sid := binary.BigEndian.Uint32(qy.Data[0:])
		flags := qy.Data[4]

		maxResp := respLen - 1 // reserve 1 byte for flags
		if maxResp < 0 {
			maxResp = 0
		}

		out, outFlags := r.Handler(sid, flags, qy.Data[sessionFrame:], maxResp)
		respData = make([]byte, 1+len(out))
		respData[0] = outFlags
		copy(respData[1:], out)
	} else {
		respData = patternBytes(qy.Nonce, respLen)
	}

	resp := response{
		Nonce: qy.Nonce,
		QSeen: uint16(len(qy.Data)),
		InTCP: inTCP,
		Data:  respData,
	}
	return c.answer(q.Name, resp.marshal(r.key))
}

// LocalResolver is a tiny in-process "recursive" resolver used by tests: it
// forwards every query straight to a Responder handler with no caching, so a
// round-trip can be exercised without real DNS infrastructure. It is NOT a real
// recursive resolver and must never be used outside tests.
func newTestResolver(resp *Responder) (addr string, stop func(), err error) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(resp.handle)}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }, nil
}

// stripPort is a small helper for logging resolver identities.
func stripPort(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return strings.TrimSpace(addr)
}

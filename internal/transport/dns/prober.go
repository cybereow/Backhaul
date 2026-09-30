package dnsx

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"
)

// querySalt keeps the query-direction test pattern distinct from the
// response-direction one, so a loopback/echo can never masquerade as a real
// round-trip.
const querySalt = 0x9e3779b97f4a7c15

// Stage is how far one probe profile got. The three facts the design demands
// (query accepted, valid response, bytes each way) map onto these.
type Stage string

const (
	// StageUnknown: no reply at all. Absence of a reply is NOT proof the query
	// never arrived — the responder may have answered and the reply was dropped
	// on the way back (design §8 is emphatic about this).
	StageUnknown Stage = "unknown"
	// StageResolver: the resolver replied, but no valid payload came back (no
	// answer of our type, or it was stripped/blocked between resolver and us).
	StageResolver Stage = "resolver_replied"
	// StageBadPeer: answer records came back but failed MAC/nonce — a cached,
	// foreign, or mangled response, not our live peer.
	StageBadPeer Stage = "bad_response"
	// StageOK: a MAC-valid, nonce-matched payload returned. Real round-trip.
	StageOK Stage = "ok"
)

// ProfileResult is one row of the report: one (resolver, type, transport, edns,
// requested response size) combination, aggregated over Attempts repeats so the
// report reflects stability, not a single sample (design §8 warns a lone success
// is not proof of a usable path).
type ProfileResult struct {
	Resolver    string `json:"resolver"`
	RRType      string `json:"rr_type"`
	Transport   string `json:"transport"` // outside->resolver hop
	EDNS        bool   `json:"edns"`
	QueryBudget int    `json:"query_budget"`
	RespBudget  int    `json:"resp_budget"` // requested response payload size

	Attempts    int     `json:"attempts"`
	Successes   int     `json:"successes"` // stage==ok
	SuccessRate float64 `json:"success_rate"`
	Stage       Stage   `json:"stage"` // best stage reached across attempts

	QBytesThrough    int    `json:"q_bytes_through"`    // max confirmed received by responder
	RespBytesThrough int    `json:"resp_bytes_through"` // max returned byte-exact
	InsideTransport  string `json:"inside_transport"`   // resolver->inside hop, from a success ("" if none)

	RTTminMs int64 `json:"rtt_min_ms"`
	RTTp50Ms int64 `json:"rtt_p50_ms"`
	RTTp90Ms int64 `json:"rtt_p90_ms"`

	Err string `json:"err,omitempty"` // representative note from a failing attempt
}

// attempt is one exchange; Run aggregates many into a ProfileResult.
type attempt struct {
	ran       bool // the probe actually ran (slots never started stay zero)
	stage     Stage
	qBytes    int
	respBytes int
	insideTCP bool
	rttMs     int64
	err       string
}

// Prober is the outside side. It only ever sends to resolvers in its list; it
// never learns or dials the authoritative address (resolver-only path, §4).
type Prober struct {
	domain    string
	key       []byte
	resolvers []string
	rrTypes   []uint16
	qBudget   int
	respSizes []int
	repeat    int
	ednsModes []bool
	useTCP    bool
	timeout   time.Duration
	pacing    time.Duration
	conc      int
	logger    *logrus.Logger
}

// ProberParams is the flattened, already-validated config the prober needs.
type ProberParams struct {
	Domain      string
	Key         string
	Resolvers   []string // "ip:port"
	RRTypes     []uint16
	QueryBudget int
	RespSizes   []int // requested response sizes to sweep (>=1 entry)
	Repeat      int   // samples per profile (>=1)
	EDNSModes   []bool
	UseTCP      bool
	Timeout     time.Duration
	Pacing      time.Duration
	Concurrency int
}

func NewProber(p ProberParams, logger *logrus.Logger) *Prober {
	conc := p.Concurrency
	if conc < 1 {
		conc = 1
	}
	repeat := p.Repeat
	if repeat < 1 {
		repeat = 1
	}
	sizes := p.RespSizes
	if len(sizes) == 0 {
		sizes = []int{120}
	}
	return &Prober{
		domain:    dns.Fqdn(p.Domain),
		key:       []byte(p.Key),
		resolvers: p.Resolvers,
		rrTypes:   p.RRTypes,
		qBudget:   p.QueryBudget,
		respSizes: sizes,
		repeat:    repeat,
		ednsModes: p.EDNSModes,
		useTCP:    p.UseTCP,
		timeout:   p.Timeout,
		pacing:    p.Pacing,
		conc:      conc,
		logger:    logger,
	}
}

// maxQueryData is how many query-payload bytes fit in a QNAME under domain,
// after base32 expansion, label dots and the query envelope. Conservative.
func maxQueryData(domain string) int {
	budget := 253 - len(dns.Fqdn(domain)) - 1 // 253 printable octets, minus domain + dot
	raw := budget * 5 / 8                     // base32: 5 bytes per 8 chars
	raw = raw * 62 / 63                       // account for a dot every 63 chars
	if raw -= queryHdr + macLen; raw < 0 {
		return 0
	}
	return raw
}

func randNonce() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}

// profileKey identifies one probe profile; Run repeats each key p.repeat times.
type profileKey struct {
	resolver  string
	rrType    uint16
	transport string
	edns      bool
	respSize  int
}

// Run probes every profile p.repeat times and returns the aggregated report.
// Profiles are the cartesian product of resolvers × RR types × transports ×
// EDNS modes × requested response sizes.
func (p *Prober) Run(ctx context.Context) []ProfileResult {
	var profiles []profileKey
	for _, res := range p.resolvers {
		for _, t := range p.rrTypes {
			for _, edns := range p.ednsModes {
				for _, sz := range p.respSizes {
					profiles = append(profiles, profileKey{res, t, "udp", edns, sz})
					if p.useTCP {
						profiles = append(profiles, profileKey{res, t, "tcp", edns, sz})
					}
				}
			}
		}
	}

	// One attempt slot per (profile, repeat); the worker pool fills them and each
	// profile's attempts are then folded into a single ProfileResult.
	attempts := make([][]attempt, len(profiles))
	for i := range attempts {
		attempts[i] = make([]attempt, p.repeat)
	}
	sem := make(chan struct{}, p.conc)
	var wg sync.WaitGroup
	for i, prof := range profiles {
		for rep := 0; rep < p.repeat; rep++ {
			select {
			case <-ctx.Done():
				wg.Wait()
				return p.aggregate(profiles, attempts)
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(i, rep int, prof profileKey) {
				defer wg.Done()
				defer func() { <-sem }()
				a := p.probe(ctx, prof.resolver, prof.rrType, prof.transport, prof.edns, prof.respSize)
				a.ran = true
				attempts[i][rep] = a
			}(i, rep, prof)
			if p.pacing > 0 {
				time.Sleep(p.pacing)
			}
		}
	}
	wg.Wait()
	return p.aggregate(profiles, attempts)
}

// aggregate folds each profile's repeated attempts into one ProfileResult with
// success rate, RTT distribution (over successful attempts) and max bytes-through.
func (p *Prober) aggregate(profiles []profileKey, attempts [][]attempt) []ProfileResult {
	out := make([]ProfileResult, 0, len(profiles))
	for i, prof := range profiles {
		var ran []attempt
		for _, a := range attempts[i] {
			if a.ran {
				ran = append(ran, a)
			}
		}
		if len(ran) == 0 {
			continue // cancelled before this profile started: nothing to report
		}
		r := ProfileResult{
			Resolver:    prof.resolver,
			RRType:      dns.TypeToString[prof.rrType],
			Transport:   prof.transport,
			EDNS:        prof.edns,
			QueryBudget: p.effectiveQLen(),
			RespBudget:  prof.respSize,
			Attempts:    len(ran),
			Stage:       StageUnknown,
		}
		var okRTTs []int64
		for _, a := range ran {
			if stageRankLower(a.stage, r.Stage) {
				r.Stage = a.stage
			}
			if a.stage == StageOK {
				r.Successes++
				okRTTs = append(okRTTs, a.rttMs)
				if a.qBytes > r.QBytesThrough {
					r.QBytesThrough = a.qBytes
				}
				if a.respBytes > r.RespBytesThrough {
					r.RespBytesThrough = a.respBytes
				}
				if a.insideTCP {
					r.InsideTransport = "tcp"
				} else {
					r.InsideTransport = "udp"
				}
			} else if a.err != "" {
				r.Err = a.err // representative failure note
			}
		}
		if r.Attempts > 0 {
			r.SuccessRate = float64(r.Successes) / float64(r.Attempts)
		}
		if r.Successes > 0 {
			r.Err = "" // clean-enough profiles don't need a note
			r.RTTminMs, r.RTTp50Ms, r.RTTp90Ms = percentiles(okRTTs)
		}
		out = append(out, r)
	}
	return out
}

func (p *Prober) effectiveQLen() int {
	qLen := p.qBudget
	if m := maxQueryData(p.domain); qLen > m {
		qLen = m
	}
	if qLen < 0 {
		qLen = 0
	}
	return qLen
}

// stageRankLower reports whether stage a is "better" (closer to ok) than b.
func stageRankLower(a, b Stage) bool {
	rank := map[Stage]int{StageOK: 0, StageBadPeer: 1, StageResolver: 2, StageUnknown: 3}
	return rank[a] < rank[b]
}

// percentiles returns min, p50 and p90 of rtts (milliseconds). rtts need not be
// sorted; it is small (one entry per successful attempt).
func percentiles(rtts []int64) (min, p50, p90 int64) {
	if len(rtts) == 0 {
		return 0, 0, 0
	}
	s := append([]int64(nil), rtts...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q int) int64 {
		idx := (len(s)*q + 99) / 100
		if idx >= len(s) {
			idx = len(s) - 1
		}
		return s[idx]
	}
	return s[0], at(50), at(90)
}

func (p *Prober) probe(ctx context.Context, resolver string, rrType uint16, transport string, edns bool, respSize int) attempt {
	res := attempt{stage: StageUnknown}
	qLen := p.effectiveQLen()

	var reqNonce uint64
	qDataFunc := func(nonce uint64) []byte {
		reqNonce = nonce
		return patternBytes(nonce^querySalt, qLen)
	}

	respData, qSeen, insideTCP, rttMs, stage, err := Exchange(ctx, p.domain, p.key, resolver, rrType, transport, edns, respSize, qDataFunc, p.timeout)
	res.rttMs = rttMs
	res.stage = stage
	if err != nil {
		res.err = err.Error()
		return res
	}

	res.qBytes = int(qSeen)
	res.insideTCP = insideTCP
	want := patternBytes(reqNonce, len(respData))
	res.respBytes = matchingPrefix(respData, want)
	if res.respBytes != len(want) {
		res.err = fmt.Sprintf("response corrupted after %d/%d bytes", res.respBytes, len(want))
	}
	return res
}

func matchingPrefix(a, b []byte) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

// Exchange attempts a single query and response through the given resolver.
func Exchange(ctx context.Context, domain string, key []byte, resolver string, rrType uint16, transport string, edns bool, respSize int, qDataFunc func(uint64) []byte, timeout time.Duration) (respData []byte, qSeen uint16, insideTCP bool, rttMs int64, stage Stage, err error) {
	return exchangeVia(nil, ctx, domain, key, resolver, rrType, transport, edns, respSize, qDataFunc, timeout)
}

// exchangeVia is Exchange, optionally reusing TCP conns from pool.
func exchangeVia(pool *connPool, ctx context.Context, domain string, key []byte, resolver string, rrType uint16, transport string, edns bool, respSize int, qDataFunc func(uint64) []byte, timeout time.Duration) (respData []byte, qSeen uint16, insideTCP bool, rttMs int64, stage Stage, err error) {
	nonce := randNonce()

	q := query{
		RRType:  rrType,
		RespLen: uint16(respSize),
		Nonce:   nonce,
		Data:    qDataFunc(nonce),
	}

	name := encodeName(q.marshal(key))
	if name != "" {
		name += "."
	}
	name += dns.Fqdn(domain)

	msg := new(dns.Msg)
	msg.SetQuestion(name, rrType)
	msg.RecursionDesired = true
	client := &dns.Client{Net: transport, Timeout: timeout}
	if edns {
		msg.SetEdns0(1232, false)
		client.UDPSize = 1232
	}
	var (
		reply  *dns.Msg
		rtt    time.Duration
		netErr error
	)
	if pool != nil && transport == "tcp" {
		reply, rtt, netErr = pool.exchange(ctx, client, msg, resolver)
	} else {
		reply, rtt, netErr = client.ExchangeContext(ctx, msg, resolver)
	}
	rttMs = rtt.Milliseconds()
	if netErr != nil {
		return nil, 0, false, rttMs, StageUnknown, netErr
	}

	if reply.Rcode != dns.RcodeSuccess {
		return nil, 0, false, rttMs, StageResolver, fmt.Errorf("rcode=%s", dns.RcodeToString[reply.Rcode])
	}

	c := codecByType(domain, rrType)
	if c == nil {
		return nil, 0, false, rttMs, StageResolver, fmt.Errorf("no codec for type %s", dns.TypeToString[rrType])
	}

	blob, extErr := c.extract(name, reply.Answer)
	if extErr != nil {
		return nil, 0, false, rttMs, StageResolver, fmt.Errorf("no usable answer payload: %w", extErr)
	}

	resp, prsErr := parseResponse(key, blob)
	if prsErr != nil || resp.Nonce != nonce {
		errMsg := "nonce mismatch (cached or foreign answer)"
		if prsErr != nil {
			errMsg = prsErr.Error()
		}
		return nil, 0, false, rttMs, StageBadPeer, fmt.Errorf("%s", errMsg)
	}

	return resp.Data, resp.QSeen, resp.InTCP, rttMs, StageOK, nil
}

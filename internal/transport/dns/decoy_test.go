package dnsx

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestWithDecoy(t *testing.T) {
	if d, err := WithDecoy("t.example.com", ""); err != nil || d != "t.example.com" {
		t.Fatalf("empty decoy: %q, %v", d, err)
	}
	if d, err := WithDecoy("t.example.com.", " .Allowed.IR. "); err != nil || d != "allowed.ir.t.example.com" {
		t.Fatalf("got %q, %v", d, err)
	}
	if _, err := WithDecoy("t.example.com", "bad..label"); err == nil {
		t.Fatal("empty label accepted")
	}
}

// Every record type, including the name-based ones whose reply targets are built
// from the domain, must round-trip when the client asks under a decoy zone, and
// the plain domain must keep working beside it.
func TestDecoyRoundTrip(t *testing.T) {
	const dom = "t.example.com"
	resp := NewResponder(dom, "k", newTestLogger())
	if err := resp.SetDecoys([]string{"allowed.ir"}); err != nil {
		t.Fatal(err)
	}
	addr, stop, err := newTestResolver(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	for _, decoy := range []string{"", "allowed.ir"} {
		domain, _ := WithDecoy(dom, decoy)
		for _, c := range registry(domain) {
			_, qSeen, _, _, stage, err := Exchange(context.Background(), domain, []byte("k"), addr, c.qtype(), "udp", true, 40,
				func(n uint64) []byte { return patternBytes(n, 20) }, 2*time.Second)
			if stage != StageOK || qSeen != 20 {
				t.Errorf("decoy %q %s: stage=%s qSeen=%d err=%v", decoy, c.name(), stage, qSeen, err)
			}
		}
	}

	// A decoy the server was not told about is just payload labels that do not
	// decode: no answer.
	domain, _ := WithDecoy(dom, "other.example")
	_, _, _, _, stage, _ := Exchange(context.Background(), domain, []byte("k"), addr, dns.TypeTXT, "udp", true, 40,
		func(n uint64) []byte { return patternBytes(n, 20) }, time.Second)
	if stage == StageOK {
		t.Fatal("unlisted decoy was served")
	}
}

// A hijacker's forged reply (right ID, wrong payload) arrives before the real
// one: the exchange must wait for and return the real reply.
func TestUDPRaceSkipsForgedReply(t *testing.T) {
	const dom = "t.example.com"
	resp := NewResponder(dom, "k", newTestLogger())
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			req := new(dns.Msg)
			if req.Unpack(buf[:n]) != nil {
				continue
			}
			// Forged answer first: a bogus A record for the same question.
			fake := new(dns.Msg)
			fake.SetReply(req)
			fake.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: req.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.IPv4(10, 10, 34, 35)}}
			if b, err := fake.Pack(); err == nil {
				_, _ = pc.WriteTo(b, from)
			}
			time.Sleep(50 * time.Millisecond)
			rw := &memWriter{local: pc.LocalAddr(), remote: from, pc: pc}
			resp.handle(rw, req)
		}
	}()

	_, qSeen, _, _, stage, err := Exchange(context.Background(), dom, []byte("k"), pc.LocalAddr().String(), dns.TypeA, "udp", true, 40,
		func(n uint64) []byte { return patternBytes(n, 12) }, 2*time.Second)
	if stage != StageOK || qSeen != 12 {
		t.Fatalf("stage=%s qSeen=%d err=%v", stage, qSeen, err)
	}
}

// memWriter writes Responder replies straight to a UDP peer.
type memWriter struct {
	local, remote net.Addr
	pc            net.PacketConn
}

func (w *memWriter) LocalAddr() net.Addr  { return w.local }
func (w *memWriter) RemoteAddr() net.Addr { return w.remote }
func (w *memWriter) WriteMsg(m *dns.Msg) error {
	b, err := m.Pack()
	if err != nil {
		return err
	}
	_, err = w.pc.WriteTo(b, w.remote)
	return err
}
func (w *memWriter) Write(b []byte) (int, error) { return w.pc.WriteTo(b, w.remote) }
func (w *memWriter) Close() error                { return nil }
func (w *memWriter) TsigStatus() error           { return nil }
func (w *memWriter) TsigTimersOnly(bool)         {}
func (w *memWriter) Hijack()                     {}

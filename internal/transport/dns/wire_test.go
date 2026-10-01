package dnsx

import (
	"context"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"
)

const (
	testDomain = "ttt.example.com"
	testKey    = "shared-secret-key"
)

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.FatalLevel)
	return l
}

// TestCodecRoundTrip: every RR family must reconstruct an opaque payload
// byte-exact through answer()/extract(), including sizes that force A/AAAA
// fragmentation and reordering.
func TestCodecRoundTrip(t *testing.T) {
	qname := "abc123." + dns.Fqdn(testDomain)
	for _, c := range registry(testDomain) {
		for _, size := range []int{0, 1, 7, 40, 100} {
			if size+envelopeOverhead > c.capacity() {
				continue
			}
			payload := patternBytes(uint64(size)*7+1, size)
			rrs, err := c.answer(qname, payload)
			if err != nil {
				t.Fatalf("%s answer(%d): %v", c.name(), size, err)
			}
			got, err := c.extract(qname, rrs)
			if err != nil {
				t.Fatalf("%s extract(%d): %v", c.name(), size, err)
			}
			if string(got) != string(payload) {
				t.Fatalf("%s size %d: round-trip mismatch\n got %x\nwant %x", c.name(), size, got, payload)
			}
		}
	}
}

// TestFragReorder: A/AAAA reassembly must tolerate resolver reorder and dupes.
func TestFragReorder(t *testing.T) {
	qname := "x." + dns.Fqdn(testDomain)
	payload := patternBytes(99, 50)
	c := aCodec{}
	rrs, err := c.answer(qname, payload)
	if err != nil {
		t.Fatal(err)
	}
	// reverse + duplicate the first data record
	shuffled := append([]dns.RR{rrs[len(rrs)-1], rrs[1]}, rrs...)
	for i, j := 0, len(shuffled)-1; i < j; i, j = i+1, j-1 {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	got, err := c.extract(qname, shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("reorder mismatch\n got %x\nwant %x", got, payload)
	}
}

// TestQNameRoundTrip: the query envelope survives base32 QNAME encode/decode.
func TestQNameRoundTrip(t *testing.T) {
	key := []byte(testKey)
	q := query{RespLen: 120, Nonce: 0xdeadbeefcafe, Data: patternBytes(1, 40)}
	name := encodeName(q.marshal(key)) + "." + dns.Fqdn(testDomain)

	raw, err := decodeName(name, testDomain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseQuery(key, raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.RespLen != q.RespLen || got.Nonce != q.Nonce || string(got.Data) != string(q.Data) {
		t.Fatalf("query round-trip mismatch: %+v vs %+v", got, q)
	}
}

// TestMACRejectsWrongKey: a MAC computed with another key must not verify — the
// responder must ignore foreign/garbage names, and the prober must reject
// responses that are not from its peer.
func TestMACRejectsWrongKey(t *testing.T) {
	q := query{RespLen: 16, Nonce: 42, Data: []byte("hi")}
	raw := q.marshal([]byte("key-one"))
	if _, err := parseQuery([]byte("key-two"), raw); err != errMAC {
		t.Fatalf("expected errMAC, got %v", err)
	}
	r := response{Nonce: 42, QSeen: 2, Data: []byte("data")}
	if _, err := parseResponse([]byte("key-two"), r.marshal([]byte("key-one"))); err != errMAC {
		t.Fatalf("expected errMAC on response, got %v", err)
	}
}

// TestEndToEnd: full path prober -> (fake resolver == responder) -> back, for a
// spread of record types, asserting a clean round-trip and byte counts.
func TestEndToEnd(t *testing.T) {
	resp := NewResponder(testDomain, testKey, quietLogger())
	addr, stop, err := newTestResolver(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	params := ProberParams{
		Domain:      testDomain,
		Key:         testKey,
		Resolvers:   []string{addr},
		RRTypes:     []uint16{dns.TypeTXT, dns.TypeNULL, dns.TypeA, dns.TypeAAAA, dns.TypeCNAME, dns.TypeMX, dns.TypeSRV, dns.TypePTR},
		QueryBudget: 40,
		RespSizes:   []int{64},
		EDNSModes:   []bool{true}, // advertise a buffer so large UDP answers aren't dropped on loopback
		Timeout:     2 * time.Second,
		Concurrency: 4,
	}
	results := NewProber(params, quietLogger()).Run(context.Background())
	if len(results) != 8 {
		t.Fatalf("expected 8 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Stage != StageOK {
			t.Errorf("%s: stage %s (%s)", r.RRType, r.Stage, r.Err)
			continue
		}
		if r.Err != "" {
			t.Errorf("%s: unexpected err %q", r.RRType, r.Err)
		}
		if r.QBytesThrough != 40 {
			t.Errorf("%s: q bytes %d, want 40", r.RRType, r.QBytesThrough)
		}
		if r.RespBytesThrough != 64 {
			t.Errorf("%s: resp bytes %d, want 64", r.RRType, r.RespBytesThrough)
		}
	}
}

// TestSoak: the sustained driver completes real round-trips through the fake
// resolver and reports non-zero goodput both ways over a short window.
func TestSoak(t *testing.T) {
	resp := NewResponder(testDomain, testKey, quietLogger())
	addr, stop, err := newTestResolver(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	params := SoakParams{
		Domain:      testDomain,
		Key:         testKey,
		QueryBudget: 40,
		EDNS:        true,
		Timeout:     2 * time.Second,
		Duration:    400 * time.Millisecond,
		MaxInflight: 2,
		Streams: []SoakStream{
			{Resolver: addr, RRType: dns.TypeTXT, Transport: "udp", RespSize: 120},
		},
	}
	results := RunSoak(context.Background(), params, quietLogger())
	if len(results) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(results))
	}
	r := results[0]
	if r.Successes == 0 {
		t.Fatalf("soak completed no successful round-trips: %+v", r)
	}
	if r.UpBytes == 0 || r.DownBytes == 0 {
		t.Fatalf("soak reported zero goodput: up=%d down=%d", r.UpBytes, r.DownBytes)
	}
	// Note: we don't assert Recovered here — an in-process UDP fake saturates
	// under thousands of req/s and can die at the tail (a loopback-resource
	// artifact, not a path property). Real infra is RTT-bounded to a few req/s.
}

// responder (its QNAME MAC fails), so the stage is "resolver_replied", never ok.
func TestForeignKeyRejected(t *testing.T) {
	resp := NewResponder(testDomain, testKey, quietLogger())
	addr, stop, err := newTestResolver(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	params := ProberParams{
		Domain:      testDomain,
		Key:         "wrong-key",
		Resolvers:   []string{addr},
		RRTypes:     []uint16{dns.TypeNULL},
		QueryBudget: 20,
		RespSizes:   []int{20},
		EDNSModes:   []bool{false},
		Timeout:     2 * time.Second,
		Concurrency: 1,
	}
	results := NewProber(params, quietLogger()).Run(context.Background())
	if len(results) != 1 || results[0].Stage == StageOK {
		t.Fatalf("wrong key must not complete a round-trip: %+v", results)
	}
}

// A requested response size is never shortened by the wire encoding.
func TestRespLenIsNotFloored(t *testing.T) {
	key := []byte("k")
	for _, want := range []uint16{1, 7, 8, 100, 664, 1000} {
		q := query{RespLen: want, Nonce: 1, Data: []byte("x")}
		got, err := parseQuery(key, q.marshal(key))
		if err != nil {
			t.Fatal(err)
		}
		if got.RespLen < want || got.RespLen >= want+8 {
			t.Errorf("RespLen %d came back as %d, want [%d,%d)", want, got.RespLen, want, want+8)
		}
	}
}

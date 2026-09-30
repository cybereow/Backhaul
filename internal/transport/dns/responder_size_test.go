package dnsx

import (
	"strings"
	"testing"

	"github.com/miekg/dns"
)

// longName builds a valid name of about n characters from labels of at most 60.
func longName(n int) string {
	const suffix = "t.example.com."
	var b strings.Builder
	for b.Len()+len(suffix) < n {
		l := n - len(suffix) - b.Len() - 1
		if l > 60 {
			l = 60
		}
		if l < 1 {
			break
		}
		b.WriteString(strings.Repeat("a", l))
		b.WriteString(".")
	}
	return b.String() + suffix
}

// The computed TXT budget must always produce a reply that packs to at most the
// advertised size, for any question length, and use most of the room.
func TestTxtMaxPayloadFitsTheReply(t *testing.T) {
	for _, qlen := range []int{30, 60, 120, 200, 250} {
		name := longName(qlen)
		payload := make([]byte, txtMaxPayload(1232, len(name)))
		rrs, err := txtCodec{}.answer(name, payload)
		if err != nil {
			t.Fatal(err)
		}
		m := new(dns.Msg)
		m.SetQuestion(name, dns.TypeTXT)
		m.Answer = rrs
		m.Compress = true
		m.SetEdns0(1232, false)
		b, err := m.Pack()
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 1232 {
			t.Errorf("qname %d bytes: reply %d > 1232 with %d payload bytes", qlen, len(b), len(payload))
		}
		if len(b) < 1232-40 {
			t.Errorf("qname %d bytes: reply only %d of 1232 used", qlen, len(b))
		}
	}
	if short, long := txtMaxPayload(1232, 60), txtMaxPayload(1232, 250); short < 600 || long >= short {
		t.Errorf("a short question should leave more room: %d vs %d", short, long)
	}
}

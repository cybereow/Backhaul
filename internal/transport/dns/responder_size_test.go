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

// For every record type and any question length, the largest payload the budget
// allows must pack into a reply of at most the advertised size.
func TestReplyBudgetFitsEveryCodec(t *testing.T) {
	for _, c := range registry("t.example.com") {
		for _, qlen := range []int{30, 120, 250} {
			name := longName(qlen)
			n := maxReplyPayload(c, name, 1232)
			// a resolver that advertises no EDNS only takes 512 bytes: that budget must fit too
			if n512 := maxReplyPayload(c, name, 512); n512 > n {
				t.Errorf("%s: 512-byte budget %d above the 1232 one %d", c.name(), n512, n)
			} else if rrs, err := c.answer(name, make([]byte, n512)); err == nil {
				m := new(dns.Msg)
				m.SetQuestion(name, c.qtype())
				m.Answer = rrs
				m.Compress = true
				m.SetEdns0(512, false)
				if m.Len() > 512 {
					t.Errorf("%s qname %d: 512-byte reply is %d bytes", c.name(), qlen, m.Len())
				}
			}
			rrs, err := c.answer(name, make([]byte, n))
			if err != nil {
				t.Fatalf("%s: %v", c.name(), err)
			}
			m := new(dns.Msg)
			m.SetQuestion(name, c.qtype())
			m.Answer = rrs
			m.Compress = true
			m.SetEdns0(1232, false)
			b, err := m.Pack()
			if err != nil {
				t.Fatalf("%s qname %d: %v", c.name(), qlen, err)
			}
			if len(b) > 1232 {
				t.Errorf("%s qname %d bytes: reply %d > 1232 with %d payload bytes", c.name(), qlen, len(b), n)
			}
			if n > c.capacity() {
				t.Errorf("%s: budget %d above its capacity %d", c.name(), n, c.capacity())
			}
		}
	}
	// a short question leaves more room than a near-maximum one, for TXT
	txt := txtCodec{}
	if short, long := maxReplyPayload(txt, longName(60), 1232), maxReplyPayload(txt, longName(250), 1232); short < 600 || long >= short {
		t.Errorf("TXT: short question should leave more room: %d vs %d", short, long)
	}
	// A records are bulky: the old nominal capacity of 180 bytes does not fit beside a long question
	if n := maxReplyPayload(aCodec{}, longName(250), 1232); n >= 180 {
		t.Errorf("A budget beside a long question should be below the nominal 180, got %d", n)
	}
}

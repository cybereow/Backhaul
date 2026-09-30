package dnsx

import (
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/miekg/dns"
)

// codec is one RR-family encoding: it packs an opaque payload into answer
// records of a given type and pulls it back out byte-exact. Families are
// pluggable (design §3 table); the prober tries each to find one that survives
// the user's resolvers.
type codec interface {
	name() string
	qtype() uint16
	// answer builds the answer section for qname carrying payload.
	answer(qname string, payload []byte) ([]dns.RR, error)
	// extract reverses answer, tolerating record reorder and duplication.
	extract(qname string, ans []dns.RR) ([]byte, error)
	// capacity is a conservative per-message response payload ceiling (bytes),
	// used to cap the requested response budget. The prober measures the real
	// limit; this only avoids asking for the impossible.
	capacity() int
}

// registry builds the codec set for one tunnel domain (name-based families need
// the domain to form valid target names). Order is the default probe order.
func registry(domain string) []codec {
	return []codec{
		txtCodec{},
		nullCodec{},
		aCodec{},
		aaaaCodec{},
		nameCodec{domain, dns.TypeCNAME},
		nameCodec{domain, dns.TypeMX},
		nameCodec{domain, dns.TypeSRV},
		nameCodec{domain, dns.TypePTR},
	}
}

func codecByType(domain string, t uint16) codec {
	for _, c := range registry(domain) {
		if c.qtype() == t {
			return c
		}
	}
	return nil
}

func codecByName(domain, n string) codec {
	n = strings.ToUpper(n)
	for _, c := range registry(domain) {
		if c.name() == n {
			return c
		}
	}
	return nil
}

// --- TXT: base32 text in character-strings ---
//
// miekg returns TXT rdata as DDD-escaped presentation strings, so raw 8-bit
// bytes do NOT survive a pack/unpack round-trip. base32 keeps the payload in a
// presentation-safe alphabet ([a-z2-7]) that packs and unpacks byte-for-byte.

type txtCodec struct{}

func (txtCodec) name() string  { return "TXT" }
func (txtCodec) qtype() uint16 { return dns.TypeTXT }
func (txtCodec) capacity() int { return 700 } // raw bytes; base32 expands 8/5 on the wire
func (txtCodec) answer(qname string, payload []byte) ([]dns.RR, error) {
	s := strings.ToLower(b32.EncodeToString(payload))
	var chunks []string
	for i := 0; i < len(s); i += 255 {
		end := i + 255
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[i:end])
	}
	if chunks == nil {
		chunks = []string{""}
	}
	return []dns.RR{&dns.TXT{Hdr: hdr(qname, dns.TypeTXT), Txt: chunks}}, nil
}
func (txtCodec) extract(_ string, ans []dns.RR) ([]byte, error) {
	for _, rr := range ans {
		if t, ok := rr.(*dns.TXT); ok {
			return b32.DecodeString(strings.ToUpper(strings.Join(t.Txt, "")))
		}
	}
	return nil, errNoAnswer
}

// --- NULL (type 10): opaque RDATA, the cleanest carrier when it survives ---

type nullCodec struct{}

func (nullCodec) name() string  { return "NULL" }
func (nullCodec) qtype() uint16 { return dns.TypeNULL }
func (nullCodec) capacity() int { return 1200 }
func (nullCodec) answer(qname string, payload []byte) ([]dns.RR, error) {
	return []dns.RR{&dns.NULL{Hdr: hdr(qname, dns.TypeNULL), Data: string(payload)}}, nil
}
func (nullCodec) extract(_ string, ans []dns.RR) ([]byte, error) {
	for _, rr := range ans {
		if n, ok := rr.(*dns.NULL); ok {
			return []byte(n.Data), nil
		}
	}
	return nil, errNoAnswer
}

// --- A / AAAA: fixed-width address records, one payload fragment each ---
//
// Record 0 is a header [0x00, lenHi, lenLo, ...]; records 1..n carry
// [index, data...]. Reassembly is by index, so resolver reorder and duplicate
// records are tolerated (design §3 note). index is one byte: at most 255 data
// records per message.

func fragAnswer(qname string, payload []byte, width int, mk func(string, []byte) dns.RR) ([]dns.RR, error) {
	dataPer := width - 1
	if max := 255 * dataPer; len(payload) > max {
		return nil, fmt.Errorf("dnsx: payload %d exceeds %d for %d-byte records", len(payload), max, width)
	}
	head := make([]byte, width)
	binary.BigEndian.PutUint16(head[1:3], uint16(len(payload)))
	rrs := []dns.RR{mk(qname, head)}
	for i, idx := 0, 1; i < len(payload); i, idx = i+dataPer, idx+1 {
		end := i + dataPer
		if end > len(payload) {
			end = len(payload)
		}
		rec := make([]byte, width)
		rec[0] = byte(idx)
		copy(rec[1:], payload[i:end])
		rrs = append(rrs, mk(qname, rec))
	}
	return rrs, nil
}

func fragExtract(width int, recs [][]byte) ([]byte, error) {
	length := -1
	frags := map[int][]byte{}
	for _, r := range recs {
		if len(r) != width {
			continue
		}
		if idx := int(r[0]); idx == 0 {
			length = int(binary.BigEndian.Uint16(r[1:3]))
		} else {
			frags[idx] = append([]byte(nil), r[1:]...)
		}
	}
	if length < 0 {
		return nil, errNoAnswer // header fragment never arrived
	}
	idxs := make([]int, 0, len(frags))
	for k := range frags {
		idxs = append(idxs, k)
	}
	sort.Ints(idxs)
	var out []byte
	for _, k := range idxs {
		out = append(out, frags[k]...)
	}
	if len(out) < length {
		return nil, fmt.Errorf("dnsx: missing fragments (have %d want %d bytes)", len(out), length)
	}
	return out[:length], nil
}

type aCodec struct{}

func (aCodec) name() string  { return "A" }
func (aCodec) qtype() uint16 { return dns.TypeA }
func (aCodec) capacity() int { return 180 }
func (aCodec) answer(qname string, payload []byte) ([]dns.RR, error) {
	return fragAnswer(qname, payload, 4, func(q string, b []byte) dns.RR {
		return &dns.A{Hdr: hdr(q, dns.TypeA), A: net.IPv4(b[0], b[1], b[2], b[3])}
	})
}
func (aCodec) extract(_ string, ans []dns.RR) ([]byte, error) {
	var recs [][]byte
	for _, rr := range ans {
		if a, ok := rr.(*dns.A); ok {
			recs = append(recs, a.A.To4())
		}
	}
	return fragExtract(4, recs)
}

type aaaaCodec struct{}

func (aaaaCodec) name() string  { return "AAAA" }
func (aaaaCodec) qtype() uint16 { return dns.TypeAAAA }
func (aaaaCodec) capacity() int { return 600 }
func (aaaaCodec) answer(qname string, payload []byte) ([]dns.RR, error) {
	return fragAnswer(qname, payload, 16, func(q string, b []byte) dns.RR {
		return &dns.AAAA{Hdr: hdr(q, dns.TypeAAAA), AAAA: net.IP(append([]byte(nil), b...))}
	})
}
func (aaaaCodec) extract(_ string, ans []dns.RR) ([]byte, error) {
	var recs [][]byte
	for _, rr := range ans {
		if a, ok := rr.(*dns.AAAA); ok {
			recs = append(recs, a.AAAA.To16())
		}
	}
	return fragExtract(16, recs)
}

// --- CNAME / PTR / MX / SRV: payload base32-encoded into a target name ---
//
// Everything rides in the target name; the numeric fields (MX preference, SRV
// priority/weight/port) are fixed at 0. Stuffing payload into those fields was
// tried and dropped: they are fixed-width with no length marker, so a short
// final payload is ambiguous. Name-only is unambiguous and byte-exact.

type nameCodec struct {
	domain string
	t      uint16
}

func (c nameCodec) name() string {
	return map[uint16]string{dns.TypeCNAME: "CNAME", dns.TypePTR: "PTR", dns.TypeMX: "MX", dns.TypeSRV: "SRV"}[c.t]
}
func (c nameCodec) qtype() uint16 { return c.t }
func (c nameCodec) capacity() int {
	// base32 is 8 chars per 5 bytes; a name is ~255 octets minus the domain and
	// dots. Conservative and well below any single-name limit.
	budget := 255 - len(dns.Fqdn(c.domain)) - 8
	if budget < 0 {
		budget = 0
	}
	return budget * 5 / 8
}

func (c nameCodec) target(payload []byte) string {
	name := encodeName(payload)
	if name == "" {
		return dns.Fqdn(c.domain)
	}
	return name + "." + dns.Fqdn(c.domain)
}

func (c nameCodec) answer(qname string, payload []byte) ([]dns.RR, error) {
	h := hdr(qname, c.t)
	tgt := c.target(payload)
	switch c.t {
	case dns.TypeCNAME:
		return []dns.RR{&dns.CNAME{Hdr: h, Target: tgt}}, nil
	case dns.TypePTR:
		return []dns.RR{&dns.PTR{Hdr: h, Ptr: tgt}}, nil
	case dns.TypeMX:
		return []dns.RR{&dns.MX{Hdr: h, Preference: 0, Mx: tgt}}, nil
	case dns.TypeSRV:
		return []dns.RR{&dns.SRV{Hdr: h, Priority: 0, Weight: 0, Port: 0, Target: tgt}}, nil
	}
	return nil, fmt.Errorf("dnsx: unsupported name codec type %d", c.t)
}

func (c nameCodec) extract(_ string, ans []dns.RR) ([]byte, error) {
	for _, rr := range ans {
		var name string
		switch v := rr.(type) {
		case *dns.CNAME:
			name = v.Target
		case *dns.PTR:
			name = v.Ptr
		case *dns.MX:
			name = v.Mx
		case *dns.SRV:
			name = v.Target
		default:
			continue
		}
		return decodeName(name, c.domain)
	}
	return nil, errNoAnswer
}

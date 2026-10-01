// Package dnsx implements the wire codecs, authoritative responder and
// recursive-resolver prober for Backhaul's DNS transport. Phase 1 is a
// reachability/capacity prober: it proves whether a query reaches the inside
// responder and a response carrying real bytes returns through a recursive
// resolver, per RR type. See the design doc §3, §4 and §8.
//
// The package name is dnsx (not dns) so miekg/dns can be imported unqualified.
package dnsx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/miekg/dns"
)

const (
	magicQuery = 0xB6 // marks a backhaul-dns v2 query
	magicResp  = 0xB4 // v2 reply; the low bit carries inTCP
	macLen     = 10   // truncated HMAC-SHA256 (80 bits)

	maxLabel = 63 // RFC 1035 label octet limit

	sessionFrame = 5 // 4 bytes sid + 1 byte flags
	FlagFIN      = 1 << 0
	FlagRST      = 1 << 1
	FlagSYN      = 1 << 2 // first exchange(s) of a session, until the first reply arrives
	FlagCtrlReq  = 1 << 3 // query: send me the server's control block
	FlagCtrl     = 1 << 4 // reply: a control block precedes the packet
	FlagProbe    = 1 << 7 // capacity probe: answered in diagnostic mode (QSeen + pattern reply), never a session
)

var (
	errShort    = errors.New("dnsx: payload too short")
	errMagic    = errors.New("dnsx: bad magic/version")
	errMAC      = errors.New("dnsx: MAC mismatch (wrong key or not our peer)")
	errNoAnswer = errors.New("dnsx: no usable answer record of the expected type")

	// b32 is lowercase-on-wire (DNS-friendly), case-insensitive on decode.
	b32 = base32.StdEncoding.WithPadding(base32.NoPadding)
)

// encodeName base32-encodes data and splits it into dot-separated labels of at
// most maxLabel chars (no trailing dot, no domain suffix).
func encodeName(data []byte) string {
	s := strings.ToLower(b32.EncodeToString(data))
	var b strings.Builder
	for i := 0; i < len(s); i += maxLabel {
		end := i + maxLabel
		if end > len(s) {
			end = len(s)
		}
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(s[i:end])
	}
	return b.String()
}

// decodeName strips domain from fqdn, joins the remaining labels and
// base32-decodes them. Case-insensitive, so 0x20-randomizing resolvers are fine.
func decodeName(fqdn, domain string) ([]byte, error) {
	f := strings.ToLower(strings.TrimSuffix(dns.Fqdn(fqdn), "."))
	d := strings.ToLower(strings.TrimSuffix(dns.Fqdn(domain), "."))
	if f != d && !strings.HasSuffix(f, "."+d) {
		return nil, fmt.Errorf("dnsx: name %q not under domain %q", fqdn, domain)
	}
	prefix := strings.TrimSuffix(strings.TrimSuffix(f, d), ".")
	if prefix == "" {
		return nil, nil
	}
	joined := strings.ToUpper(strings.ReplaceAll(prefix, ".", ""))
	return b32.DecodeString(joined)
}

func macSum(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)[:macLen]
}

func hdr(name string, t uint16) dns.RR_Header {
	// TTL 0 reduces caching; a fresh nonce per exchange is what actually defeats
	// a stale cached answer (design §4).
	return dns.RR_Header{Name: dns.Fqdn(name), Rrtype: t, Class: dns.ClassINET, Ttl: 0}
}

// query is the outside->inside payload carried in the QNAME.
//
//	magic(1) respLen/8(1) nonce(6) data mac(10)
//
// Every byte here is paid for out of the ~140 that fit in a QNAME, so the header
// is as small as it can be: the record type is not carried (the responder reads
// it from the DNS question), data length is implicit, the nonce is 48 bits (it
// only has to defeat caching and pair a reply with its query) and the MAC is a
// truncated 80 bits. respLen is how many payload bytes to send back, in units of
// 8 rounded up (so a request is never silently shortened; the responder still
// caps every reply at the codec's and the UDP budget's limits).
type query struct {
	RespLen uint16
	Nonce   uint64 // low 48 bits are sent
	Data    []byte
}

const (
	queryHdr   = 8 // magic + respLen/8 + nonce(6)
	nonceBytes = 6
	nonceMask  = 1<<(8*nonceBytes) - 1
)

func putNonce(b []byte, n uint64) {
	for i := 0; i < nonceBytes; i++ {
		b[i] = byte(n >> (8 * (nonceBytes - 1 - i)))
	}
}

func getNonce(b []byte) uint64 {
	var n uint64
	for i := 0; i < nonceBytes; i++ {
		n = n<<8 | uint64(b[i])
	}
	return n
}

func (q query) marshal(key []byte) []byte {
	body := make([]byte, queryHdr+len(q.Data))
	body[0] = magicQuery
	body[1] = byte(min((int(q.RespLen)+7)/8, 255))
	putNonce(body[2:], q.Nonce)
	copy(body[queryHdr:], q.Data)
	return append(body, macSum(key, body)...)
}

func parseQuery(key, raw []byte) (query, error) {
	if len(raw) < queryHdr+macLen {
		return query{}, errShort
	}
	if raw[0] != magicQuery {
		return query{}, errMagic
	}
	body, mac := raw[:len(raw)-macLen], raw[len(raw)-macLen:]
	if !hmac.Equal(mac, macSum(key, body)) {
		return query{}, errMAC
	}
	return query{
		RespLen: uint16(body[1]) * 8,
		Nonce:   getNonce(body[2:]),
		Data:    append([]byte(nil), body[queryHdr:]...),
	}, nil
}

// response is the inside->outside payload carried in the answer RDATA.
//
//	magic|inTCP(1) nonce(6) qSeen(2) data mac(10)
//
// nonce echoes the query nonce (anti-cache + peer proof); qSeen is how many
// query-data bytes the responder actually received, so the prober learns the
// surviving capacity of the QNAME direction independently of the reply. inTCP
// (the low bit of the first byte) is the transport the responder saw from the
// recursive resolver, which recovers the otherwise-invisible resolver->inside
// hop for the report.
type response struct {
	Nonce uint64
	QSeen uint16
	InTCP bool
	Data  []byte
}

const respHdr = 9 // magic|inTCP + nonce(6) + qSeen(2)

func (r response) marshal(key []byte) []byte {
	body := make([]byte, respHdr+len(r.Data))
	body[0] = magicResp
	if r.InTCP {
		body[0] |= 1
	}
	putNonce(body[1:], r.Nonce)
	binary.BigEndian.PutUint16(body[7:], r.QSeen)
	copy(body[respHdr:], r.Data)
	return append(body, macSum(key, body)...)
}

func parseResponse(key, raw []byte) (response, error) {
	if len(raw) < respHdr+macLen {
		return response{}, errShort
	}
	if raw[0]&0xFE != magicResp {
		return response{}, errMagic
	}
	body, mac := raw[:len(raw)-macLen], raw[len(raw)-macLen:]
	if !hmac.Equal(mac, macSum(key, body)) {
		return response{}, errMAC
	}
	return response{
		Nonce: getNonce(body[1:]),
		QSeen: binary.BigEndian.Uint16(body[7:]),
		InTCP: body[0]&1 == 1,
		Data:  append([]byte(nil), body[respHdr:]...),
	}, nil
}

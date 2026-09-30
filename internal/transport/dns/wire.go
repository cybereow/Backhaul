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
	magicByte = 0xB4 // marks a backhaul-dns payload
	protoVer  = 1
	macLen    = 16 // truncated HMAC-SHA256

	maxLabel = 63 // RFC 1035 label octet limit
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
//	magic(1) ver(1) rrtype(2) respLen(2) nonce(8) dataLen(2) data mac(16)
//
// rrtype/respLen tell the responder which RR family to answer with and how many
// payload bytes to send back, so a single responder serves every probe profile.
type query struct {
	RRType  uint16
	RespLen uint16
	Nonce   uint64
	Data    []byte
}

const queryHdr = 16 // magic+ver+rrtype+respLen+nonce+dataLen

func (q query) marshal(key []byte) []byte {
	body := make([]byte, queryHdr+len(q.Data))
	body[0], body[1] = magicByte, protoVer
	binary.BigEndian.PutUint16(body[2:], q.RRType)
	binary.BigEndian.PutUint16(body[4:], q.RespLen)
	binary.BigEndian.PutUint64(body[6:], q.Nonce)
	binary.BigEndian.PutUint16(body[14:], uint16(len(q.Data)))
	copy(body[queryHdr:], q.Data)
	return append(body, macSum(key, body)...)
}

func parseQuery(key, raw []byte) (query, error) {
	if len(raw) < queryHdr+macLen {
		return query{}, errShort
	}
	if raw[0] != magicByte || raw[1] != protoVer {
		return query{}, errMagic
	}
	body, mac := raw[:len(raw)-macLen], raw[len(raw)-macLen:]
	if !hmac.Equal(mac, macSum(key, body)) {
		return query{}, errMAC
	}
	dlen := int(binary.BigEndian.Uint16(body[14:]))
	if queryHdr+dlen != len(body) {
		return query{}, errShort
	}
	return query{
		RRType:  binary.BigEndian.Uint16(body[2:]),
		RespLen: binary.BigEndian.Uint16(body[4:]),
		Nonce:   binary.BigEndian.Uint64(body[6:]),
		Data:    append([]byte(nil), body[queryHdr:]...),
	}, nil
}

// response is the inside->outside payload carried in the answer RDATA.
//
//	magic(1) ver(1) nonce(8) qSeen(2) inTCP(1) dataLen(2) data mac(16)
//
// nonce echoes the query nonce (anti-cache + peer proof); qSeen is how many
// query-data bytes the responder actually received, so the prober learns the
// surviving capacity of the QNAME direction independently of the reply. inTCP
// is the transport the responder saw from the recursive resolver (1=TCP), which
// recovers the otherwise-invisible resolver->inside hop for the report.
type response struct {
	Nonce uint64
	QSeen uint16
	InTCP bool
	Data  []byte
}

const respHdr = 15 // magic+ver+nonce+qSeen+inTCP+dataLen

func (r response) marshal(key []byte) []byte {
	body := make([]byte, respHdr+len(r.Data))
	body[0], body[1] = magicByte, protoVer
	binary.BigEndian.PutUint64(body[2:], r.Nonce)
	binary.BigEndian.PutUint16(body[10:], r.QSeen)
	if r.InTCP {
		body[12] = 1
	}
	binary.BigEndian.PutUint16(body[13:], uint16(len(r.Data)))
	copy(body[respHdr:], r.Data)
	return append(body, macSum(key, body)...)
}

func parseResponse(key, raw []byte) (response, error) {
	if len(raw) < respHdr+macLen {
		return response{}, errShort
	}
	if raw[0] != magicByte || raw[1] != protoVer {
		return response{}, errMagic
	}
	body, mac := raw[:len(raw)-macLen], raw[len(raw)-macLen:]
	if !hmac.Equal(mac, macSum(key, body)) {
		return response{}, errMAC
	}
	dlen := int(binary.BigEndian.Uint16(body[13:]))
	if respHdr+dlen != len(body) {
		return response{}, errShort
	}
	return response{
		Nonce: binary.BigEndian.Uint64(body[2:]),
		QSeen: binary.BigEndian.Uint16(body[10:]),
		InTCP: body[12] == 1,
		Data:  append([]byte(nil), body[respHdr:]...),
	}, nil
}

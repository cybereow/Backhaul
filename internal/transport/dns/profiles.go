package dnsx

import (
	"strings"

	"github.com/musix/backhaul/internal/transport/dns/sel"
)

// DefaultProfiles builds the candidate list for Dial: every resolver x record
// type x {udp,tcp}. recordTypes empty means all built-in codecs. Resolvers
// without a port get :53. Unknown record type names are skipped; the record type
// is never chosen here, sel picks it from live measurements.
func DefaultProfiles(domain string, resolvers, recordTypes []string) []sel.Profile {
	var codecs []codec
	if len(recordTypes) == 0 {
		codecs = registry(domain)
	}
	for _, n := range recordTypes {
		if c := codecByName(domain, strings.TrimSpace(n)); c != nil {
			codecs = append(codecs, c)
		}
	}

	var out []sel.Profile
	seen := map[string]bool{}
	for _, r := range resolvers {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		r = withPort(r)
		if seen[r] { // "1.2.3.4" and "1.2.3.4:53" are the same resolver
			continue
		}
		seen[r] = true
		for _, c := range codecs {
			for _, t := range []string{"udp", "tcp"} {
				out = append(out, sel.Profile{Resolver: r, RRType: c.qtype(), Transport: t, Cap: c.capacity()})
			}
		}
	}
	return out
}

// RecordTypeCodes maps record type names (empty: every built-in codec, the same
// set DefaultProfiles uses) to their DNS type codes, skipping unknown names.
func RecordTypeCodes(domain string, names []string) []uint16 {
	var out []uint16
	if len(names) == 0 {
		for _, c := range registry(domain) {
			out = append(out, c.qtype())
		}
		return out
	}
	for _, n := range names {
		if c := codecByName(domain, strings.TrimSpace(n)); c != nil {
			out = append(out, c.qtype())
		}
	}
	return out
}

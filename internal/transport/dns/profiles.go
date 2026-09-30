package dnsx

import (
	"net"
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
	for _, r := range resolvers {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(r); err != nil {
			r = net.JoinHostPort(r, "53")
		}
		for _, c := range codecs {
			for _, t := range []string{"udp", "tcp"} {
				out = append(out, sel.Profile{Resolver: r, RRType: c.qtype(), Transport: t, Cap: c.capacity()})
			}
		}
	}
	return out
}

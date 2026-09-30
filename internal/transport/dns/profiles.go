package dnsx

import (
	"strings"

	"github.com/musix/backhaul/internal/transport/dns/sel"
)

// DefaultProfiles returns every requested resolver/record-type/transport
// combination. An empty recordTypes list selects every codec supported by the
// carrier. Unknown record type names are ignored; configuration validation is
// responsible for reporting them to users.
func DefaultProfiles(resolvers []string, recordTypes []string) []sel.Profile {
	selected := make(map[string]struct{}, len(recordTypes))
	for _, name := range recordTypes {
		selected[strings.ToUpper(strings.TrimSpace(name))] = struct{}{}
	}

	var profiles []sel.Profile
	for _, resolver := range resolvers {
		for _, codec := range registry("") {
			if len(selected) != 0 {
				if _, ok := selected[codec.name()]; !ok {
					continue
				}
			}
			for _, transport := range []string{"tcp", "udp"} {
				profiles = append(profiles, sel.Profile{
					Resolver: resolver, RRType: codec.qtype(),
					Transport: transport, Cap: codec.capacity(),
				})
			}
		}
	}
	return profiles
}

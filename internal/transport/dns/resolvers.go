package dnsx

import "strings"

// DefaultResolvers are public recursive resolvers operated inside Iran that were
// measured to accept queries from abroad and to reach an Iranian authoritative
// server (probe of 2026-09-30: ordered by observed reliability). The client
// tests every one at startup and keeps only those that work from its own
// vantage point, so the list is a set of candidates, not a promise.
var DefaultResolvers = []string{
	"2.189.44.44",
	"178.22.122.100", "178.22.122.101", // Shecan
	"185.51.200.1", "185.51.200.2",
	"78.157.42.100", "78.157.42.101", // Electro
	"185.55.226.26",
	"217.218.127.127", "217.218.155.155", // TCI
}

// ExpandResolvers returns the resolver list to test: the configured entries, plus
// DefaultResolvers when the list is empty or contains "auto". Duplicates are
// dropped, order kept.
func ExpandResolvers(configured []string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(r string) {
		r = strings.TrimSpace(r)
		if r != "" && !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	auto := len(configured) == 0
	for _, r := range configured {
		if strings.EqualFold(strings.TrimSpace(r), "auto") {
			auto = true
			continue
		}
		add(r)
	}
	if auto {
		for _, r := range DefaultResolvers {
			add(r)
		}
	}
	return out
}

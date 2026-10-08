package dnsx

import (
	"fmt"
	"strings"

	"github.com/miekg/dns"
)

// WithDecoy returns the name the client tunnels through when decoy is set:
// "<decoy>.<domain>". Payload labels go in front of it, so a query reads
// "<payload>.<decoy>.<domain>". The NS delegation is still for domain (the
// server must list the decoy in dns_decoys), which is what routes the query to
// us; the decoy only changes what the name looks like to whatever matches it
// against an allow-list on the way. An empty decoy returns domain unchanged.
func WithDecoy(domain, decoy string) (string, error) {
	decoy = strings.Trim(strings.TrimSpace(decoy), ".")
	if decoy == "" {
		return domain, nil
	}
	name := dns.Fqdn(strings.ToLower(decoy) + "." + strings.Trim(domain, "."))
	if _, ok := dns.IsDomainName(name); !ok || len(name) > 253 {
		return "", fmt.Errorf("decoy %q does not form a valid name under %q", decoy, domain)
	}
	return strings.TrimSuffix(name, "."), nil
}

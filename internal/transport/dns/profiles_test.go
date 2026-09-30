package dnsx

import (
	"testing"

	"github.com/miekg/dns"
)

func TestDefaultProfiles(t *testing.T) {
	profiles := DefaultProfiles([]string{"one:53", "two:53"}, []string{"TXT", "aaaa"})
	if len(profiles) != 8 {
		t.Fatalf("got %d profiles, want 8", len(profiles))
	}
	wantCap := map[uint16]int{dns.TypeTXT: txtCodec{}.capacity(), dns.TypeAAAA: aaaaCodec{}.capacity()}
	for _, p := range profiles {
		cap, ok := wantCap[p.RRType]
		if !ok || p.Cap != cap {
			t.Fatalf("unexpected profile: %+v", p)
		}
		if p.Transport != "tcp" && p.Transport != "udp" {
			t.Fatalf("unexpected transport: %q", p.Transport)
		}
	}

	all := DefaultProfiles([]string{"one:53"}, nil)
	if got, want := len(all), len(registry(""))*2; got != want {
		t.Fatalf("all profile count = %d, want %d", got, want)
	}
}

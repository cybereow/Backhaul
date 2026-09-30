package dnsx

import "testing"

func TestDefaultProfiles(t *testing.T) {
	all := DefaultProfiles("t.example.com", []string{"1.1.1.1", "8.8.8.8:5353"}, nil)
	if want := 2 * len(registry("t.example.com")) * 2; len(all) != want {
		t.Fatalf("got %d profiles, want %d", len(all), want)
	}
	if all[0].Resolver != "1.1.1.1:53" || all[len(all)-1].Resolver != "8.8.8.8:5353" {
		t.Fatalf("resolver ports not normalized: %v", all)
	}
	for _, p := range all {
		if p.Cap <= 0 {
			t.Fatalf("profile without capacity: %+v", p)
		}
	}

	some := DefaultProfiles("t.example.com", []string{"1.1.1.1"}, []string{"txt", "MX", "bogus"})
	if len(some) != 4 { // TXT+MX x udp+tcp
		t.Fatalf("got %d profiles, want 4: %v", len(some), some)
	}
}

func TestEmptyRecordTypesMeansEveryCodec(t *testing.T) {
	codes := RecordTypeCodes("t.example.com", nil)
	if len(codes) != len(registry("t.example.com")) {
		t.Fatalf("empty list gave %d types, want all %d codecs", len(codes), len(registry("t.example.com")))
	}
	o := DiscoverOpts{Domain: "t.example.com"}
	o.defaults()
	if len(o.RRTypes) != len(codes) {
		t.Fatalf("discovery default covers %d types, profiles cover %d", len(o.RRTypes), len(codes))
	}
}

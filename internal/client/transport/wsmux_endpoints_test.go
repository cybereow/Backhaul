package transport

import (
	"testing"
)

func TestBuildEndpointsFallsBackToSingle(t *testing.T) {
	eps := buildEndpoints(nil, nil, "ir.aosky.ir:443", "1.2.3.4")
	if len(eps) != 1 {
		t.Fatalf("want 1 endpoint, got %d", len(eps))
	}
	if eps[0].addr != "ir.aosky.ir:443" || eps[0].edgeIP != "1.2.3.4" {
		t.Fatalf("unexpected endpoint: %+v", eps[0])
	}
}

func TestBuildEndpointsAlignsEdgeIPs(t *testing.T) {
	addrs := []string{"aosky.ir:443", "nekocafe.sbs:443", "onionchips.sbs:443"}
	edges := []string{"10.0.0.1"} // only the first has an edge IP
	eps := buildEndpoints(addrs, edges, "", "")
	if len(eps) != 3 {
		t.Fatalf("want 3 endpoints, got %d", len(eps))
	}
	if eps[0].addr != "aosky.ir:443" || eps[0].edgeIP != "10.0.0.1" {
		t.Errorf("endpoint 0 wrong: %+v", eps[0])
	}
	if eps[1].edgeIP != "" || eps[2].edgeIP != "" {
		t.Errorf("endpoints without an edge IP should be empty: %+v %+v", eps[1], eps[2])
	}
}

func TestNextEndpointRoundRobinsEvenly(t *testing.T) {
	addrs := []string{"a:443", "b:443", "c:443"}
	c := &WsMuxTransport{endpoints: buildEndpoints(addrs, nil, "", "")}

	counts := map[string]int{}
	const per = 100
	for i := 0; i < per*len(addrs); i++ {
		counts[c.nextEndpoint().addr]++
	}
	for _, a := range addrs {
		if counts[a] != per {
			t.Errorf("endpoint %q dialed %d times, want %d (uneven spread)", a, counts[a], per)
		}
	}
}

func TestNextEndpointSingleIsStable(t *testing.T) {
	c := &WsMuxTransport{endpoints: buildEndpoints(nil, nil, "only:443", "")}
	for i := 0; i < 5; i++ {
		if got := c.nextEndpoint().addr; got != "only:443" {
			t.Fatalf("single endpoint returned %q", got)
		}
	}
}

// One dial's round covers every entry point exactly once, whatever other dials
// do to the shared turn in between: with one dead and one live entry point, a
// dial that starts on the dead one always gets to the live one.
func TestEndpointRoundCoversEveryEndpointOnce(t *testing.T) {
	addrs := []string{"a:443", "b:443", "c:443"}
	c := &WsMuxTransport{endpoints: buildEndpoints(addrs, nil, "", "")}
	starts := map[string]int{}
	for i := 0; i < 30; i++ {
		round := c.endpointRound()
		c.nextEndpoint() // another dial takes a turn while this one is on its round
		if len(round) != len(addrs) {
			t.Fatalf("round has %d entry points, want %d", len(round), len(addrs))
		}
		seen := map[string]bool{}
		for _, ep := range round {
			seen[ep.addr] = true
		}
		if len(seen) != len(addrs) {
			t.Fatalf("round %v repeats an entry point", round)
		}
		starts[round[0].addr]++
	}
	if len(starts) != len(addrs) {
		t.Fatalf("rounds only ever start on %v: the dials are not spread", starts)
	}
	single := &WsMuxTransport{endpoints: buildEndpoints(nil, nil, "only:443", "")}
	if r := single.endpointRound(); len(r) != 1 || r[0].addr != "only:443" {
		t.Fatalf("single entry point: %v", r)
	}
}

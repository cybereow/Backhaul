package transport

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
)

// Test ports are picked by binding, closing and re-binding later, so anything that
// grabs the port in between breaks the test with "address already in use" (the
// server's listeners are fatal on a failed bind). The usual culprit is the kernel
// itself: ":0" hands out ports from the ephemeral range, the same range outgoing
// connections of every other test process draw from. So test ports are taken
// below that range (Linux's default starts at 32768), at random, and never reused
// within this process.
const (
	testPortLow  = 20000
	testPortHigh = 32000
)

var (
	testPortsMu   sync.Mutex
	testPortsUsed = map[int]bool{}
)

// testPort returns a currently bindable port number for network ("tcp" or "udp").
func testPort(t testing.TB, network string) int {
	t.Helper()
	testPortsMu.Lock()
	defer testPortsMu.Unlock()
	for i := 0; i < 200; i++ {
		port := testPortLow + rand.Intn(testPortHigh-testPortLow)
		if testPortsUsed[port] {
			continue
		}
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		var closeFn func() error
		if network == "udp" {
			pc, err := net.ListenPacket("udp", addr)
			if err != nil {
				continue
			}
			closeFn = pc.Close
		} else {
			l, err := net.Listen("tcp", addr)
			if err != nil {
				continue
			}
			closeFn = l.Close
		}
		closeFn()
		testPortsUsed[port] = true
		return port
	}
	t.Fatal("no free test port found")
	return 0
}

func testAddr(t testing.TB, network string) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", testPort(t, network))
}

package dnsx

import (
	"net"
	"testing"
	"time"
)

func authPair(t *testing.T, clientTok, serverTok string) (cerr, serr error) {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	_ = a.SetDeadline(time.Now().Add(2 * time.Second))
	_ = b.SetDeadline(time.Now().Add(2 * time.Second))
	done := make(chan error, 1)
	go func() { done <- ServerAuth(b, serverTok) }()
	cerr = ClientAuth(a, clientTok)
	if cerr != nil {
		a.Close() // unblock the server side
	}
	return cerr, <-done
}

func TestAuth(t *testing.T) {
	if c, s := authPair(t, "tok", "tok"); c != nil || s != nil {
		t.Fatalf("matching tokens rejected: client=%v server=%v", c, s)
	}
	if c, s := authPair(t, "tok", "other"); c == nil || s == nil {
		t.Fatalf("mismatched tokens accepted: client=%v server=%v", c, s)
	}
}

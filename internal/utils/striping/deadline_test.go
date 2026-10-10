package striping

import (
	"bytes"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// A read deadline interrupts a Read and nothing else: the legs stay up, nothing
// in flight is lost, and with the deadline cleared the reader carries on to the
// last byte. A flow being moved off a striped connection wakes its reader this
// way while megabytes are still on their way to it.
func TestReadDeadlineInterruptsReadNotTheConn(t *testing.T) {
	build := map[string]func(legs, peers []net.Conn) (w, r net.Conn){
		"striped": func(legs, peers []net.Conn) (net.Conn, net.Conn) {
			return New(legs, 997), New(peers, 997)
		},
		"fec": func(legs, peers []net.Conn) (net.Conn, net.Conn) {
			w, err := NewFEC(legs, 991, 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			r, err := NewFEC(peers, 991, 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			return w, r
		},
	}
	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			legs, peers := pipePair(4)
			w, r := mk(legs, peers)
			defer w.Close()
			defer r.Close()

			// With nothing to read, a deadline in the past ends the Read at once.
			_ = r.SetReadDeadline(time.Unix(1, 0))
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("idle Read past its deadline: %v, want os.ErrDeadlineExceeded", err)
			}
			// A Read already waiting is woken by a deadline set under it.
			_ = r.SetReadDeadline(time.Time{})
			woken := make(chan error, 1)
			go func() {
				_, err := r.Read(make([]byte, 1))
				woken <- err
			}()
			time.Sleep(50 * time.Millisecond)
			_ = r.SetReadDeadline(time.Unix(1, 0))
			select {
			case err := <-woken:
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("waiting Read: %v, want os.ErrDeadlineExceeded", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a deadline set under a waiting Read did not wake it")
			}
			_ = r.SetReadDeadline(time.Time{})

			payload := make([]byte, 4*1024*1024+13)
			if _, err := rand.Read(payload); err != nil {
				t.Fatal(err)
			}
			werr := make(chan error, 1)
			go func() {
				_, err := w.Write(payload)
				werr <- err
			}()

			// Interrupt the reader again and again while the transfer runs.
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				for {
					select {
					case <-stop:
						return
					case <-time.After(2 * time.Millisecond):
						_ = r.SetReadDeadline(time.Unix(1, 0))
					}
				}
			}()
			got := make([]byte, 0, len(payload))
			buf := make([]byte, 64*1024)
			timeouts := 0
			for len(got) < len(payload) {
				n, err := r.Read(buf)
				got = append(got, buf[:n]...)
				if errors.Is(err, os.ErrDeadlineExceeded) {
					timeouts++
					_ = r.SetReadDeadline(time.Time{})
					continue
				}
				if err != nil {
					t.Fatalf("after %d of %d bytes and %d interruptions: %v", len(got), len(payload), timeouts, err)
				}
			}
			if err := <-werr; err != nil {
				t.Fatalf("write: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("payload differs after interrupted reads")
			}
			if timeouts == 0 {
				t.Fatal("the reader was never interrupted: the test proved nothing")
			}
		})
	}
}

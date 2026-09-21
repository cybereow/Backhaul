package handlers

import (
	"io"
	"net"
	"testing"
)

// muxSink stands in for the destination of a socket -> tunnel hop: an
// *smux.Stream or a *striping.Conn. Like both of those it is a net.Conn that
// implements NEITHER io.ReaderFrom nor io.WriterTo, so no kernel-side fast path
// can ever apply to it.
type muxSink struct {
	net.Conn
	n int64
}

func (m *muxSink) Write(p []byte) (int, error) { m.n += int64(len(p)); return len(p), nil }
func (m *muxSink) Close() error                { return nil }

// muxSource stands in for the source of a tunnel -> socket hop. smux.Stream
// implements io.WriterTo and hands over buffers it already holds, so that path
// must keep being preferred over copying through a buffer of ours.
type muxSource struct {
	net.Conn
	r io.Reader
}

func (m *muxSource) Read(p []byte) (int, error) { return m.r.Read(p) }
func (m *muxSource) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, m.r)
}

type nopReader struct{}

func (nopReader) Read(p []byte) (int, error) { return 0, io.EOF }

func tcpConnPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { dialed.Close(); r.c.Close() })
	return dialed.(*net.TCPConn), r.c.(*net.TCPConn)
}

// TestPickCopyMode pins the routing decision for the conn pairs this tunnel
// actually builds. The socket -> mux case is the one that regressed silently
// before: *net.TCPConn satisfies io.WriterTo unconditionally, so a check for
// the interface alone sent it into net.genericWriteTo, which allocates its own
// 32KB buffer per connection and never touches the pooled one.
func TestPickCopyMode(t *testing.T) {
	a, b := tcpConnPair(t)
	sink := &muxSink{}
	src := &muxSource{r: new(nopReader)}

	tests := []struct {
		name string
		from net.Conn
		to   net.Conn
		want copyMode
	}{
		{"tcp to tcp splices (tcp/tcpmux forwarding)", a, b, copySplice},
		{"socket to mux uses the pooled buffer (upload ingress)", a, sink, copyPooled},
		{"mux to socket keeps the source's WriteTo (download egress)", src, a, copyWriteTo},
		{"mux to mux keeps the source's WriteTo", src, sink, copyWriteTo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pickCopyMode(tt.from, tt.to); got != tt.want {
				t.Fatalf("pickCopyMode = %d, want %d", got, tt.want)
			}
		})
	}
}

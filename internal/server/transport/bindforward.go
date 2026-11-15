package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

// bindForwardPort returns the port of the server's bind_addr when it can serve as
// the implicit forwarding port: `ports` is empty and no decoy `fallback` was
// configured. ok is false when the operator listed ports (or a fallback) or
// bind_addr has no usable port, so behaviour is then unchanged.
//
// With this on, every request that reaches the tunnel's own port but is not
// tunnel traffic is carried through the tunnel to the same port on the client.
// A single port such as 443 can then be both the tunnel endpoint and a public
// service that lives behind the client.
func bindForwardPort(bindAddr string, ports []string, fallback string) (port int, ok bool) {
	if len(ports) > 0 || fallback != "" {
		return 0, false
	}
	_, p, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return 0, false
	}
	return n, true
}

// pipeLocalConn is one end of an in-memory pipe that stands in for an accepted
// user socket, so it can ride the normal local-connection pipeline. That
// pipeline reads LocalAddr().(*net.TCPAddr) to label the flow, hence the
// explicit addresses.
type pipeLocalConn struct {
	net.Conn
	local, remote *net.TCPAddr
}

func (c *pipeLocalConn) LocalAddr() net.Addr  { return c.local }
func (c *pipeLocalConn) RemoteAddr() net.Addr { return c.remote }

// newBindForwardProxy builds the handler for non-tunnel requests on the bind
// port. The first request of a connection is replayed into an in-memory pipe
// whose far end is handed to enqueue as a local connection destined for the same
// port on the client; from then on the user's socket is spliced to that pipe as
// raw bytes. So the connection costs one tunnel stream, exactly like a mapped
// port, and keep-alive, pipelining and WebSocket upgrades all just flow through
// without a per-request HTTP hop. Every existing tunnel feature (pool, striping,
// resume) is reused as is. enqueue returns false when the connection could not be
// queued.
//
// The tunnel carries plain HTTP: with wss/wssmux the server has already
// terminated TLS, so the service behind the client sees decrypted traffic.
func newBindForwardProxy(port int, enqueue func(conn net.Conn, remoteAddr string) bool) http.Handler {
	remoteAddr := strconv.Itoa(port)

	dial := func() (net.Conn, error) {
		near, far := net.Pipe()
		lc := &pipeLocalConn{
			Conn:   far,
			local:  &net.TCPAddr{IP: net.IPv4zero, Port: port},
			remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
		}
		if !enqueue(lc, remoteAddr) {
			near.Close()
			far.Close()
			return nil, errors.New("tunnel is busy or not connected")
		}
		return near, nil
	}

	// Used only when the connection cannot be hijacked (HTTP/2): one request per
	// upstream pipe.
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", remoteAddr)}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		DialContext:         func(context.Context, string, string) (net.Conn, error) { return dial() },
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			proxy.ServeHTTP(w, r)
			return
		}
		near, err := dial()
		if err != nil {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		conn, brw, err := hj.Hijack()
		if err != nil {
			near.Close()
			return
		}
		// Request.Write adds a default User-Agent when none is set; an empty value
		// keeps the request as the user sent it.
		if _, has := r.Header["User-Agent"]; !has {
			r.Header["User-Agent"] = []string{""}
		}
		_ = near.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := r.Write(near); err != nil {
			near.Close()
			conn.Close()
			return
		}
		_ = near.SetWriteDeadline(time.Time{})

		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(near, brw.Reader); done <- struct{}{} }()
		go func() { _, _ = io.Copy(conn, near); done <- struct{}{} }()
		<-done
		near.Close()
		conn.Close()
		<-done
	})
}

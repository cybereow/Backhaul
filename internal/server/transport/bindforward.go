package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
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
		conn, brw, err := hj.Hijack()
		if err != nil {
			return
		}
		// Re-create the request head exactly as parsed, without touching the body:
		// the body (and anything pipelined behind it) is still raw bytes in the
		// server's read buffer and on the socket, so it streams through as is.
		// Reading it here (Request.Write) would buffer a whole upload in memory
		// before the first byte reached the tunnel.
		var pre bytes.Buffer
		fmt.Fprintf(&pre, "%s %s %s\r\nHost: %s\r\n", r.Method, r.RequestURI, r.Proto, r.Host)
		_ = r.Header.Write(&pre)
		for _, te := range r.TransferEncoding {
			fmt.Fprintf(&pre, "Transfer-Encoding: %s\r\n", te)
		}
		pre.WriteString("\r\n")
		if n := brw.Reader.Buffered(); n > 0 {
			rest, _ := brw.Reader.Peek(n)
			pre.Write(rest)
		}
		// The server would have answered "Expect: 100-continue" once the body was
		// read; nobody reads it here, so say it now or the client waits a second.
		if strings.EqualFold(r.Header.Get("Expect"), "100-continue") {
			_, _ = io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n")
		}
		// The user's own socket goes into the tunnel, like one accepted on a
		// mapped port; only the replayed prefix is served ahead of it.
		if !enqueue(&prefixConn{Conn: conn, pre: &pre}, remoteAddr) {
			_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			conn.Close()
		}
	})
}

// prefixConn serves pre first, then the connection itself. LocalAddr and
// RemoteAddr are the real socket's, so the flow is labelled with the bind port.
type prefixConn struct {
	net.Conn
	pre *bytes.Buffer
}

// CloseWrite and CloseRead keep the half-close a mapped port's socket has: the
// pumps send a clean FIN with CloseWrite so the peer can finish what it is still
// sending; without it they fall back to closing the whole connection, which cuts
// a proxied exchange short.
func (c *prefixConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Conn.Close()
}

func (c *prefixConn) CloseRead() error {
	if cr, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return cr.CloseRead()
	}
	return nil
}

// TakePrefix hands out what is left of the prefix and the socket itself, for a
// raw flow that moves bytes with splice(2) (handlers.PrefixedConn). c is not to
// be read again afterwards.
func (c *prefixConn) TakePrefix() ([]byte, net.Conn) {
	var pre []byte
	if c.pre != nil && c.pre.Len() > 0 {
		pre = append(pre, c.pre.Bytes()...)
	}
	c.pre = nil
	return pre, c.Conn
}

// NetConn exposes the socket underneath, so socket tuning can reach it.
func (c *prefixConn) NetConn() net.Conn { return c.Conn }

func (c *prefixConn) Read(p []byte) (int, error) {
	if c.pre != nil && c.pre.Len() > 0 {
		return c.pre.Read(p)
	}
	c.pre = nil
	return c.Conn.Read(p)
}

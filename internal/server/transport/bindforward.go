package transport

import (
	"context"
	"errors"
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
// port. Each upstream connection of the reverse proxy is an in-memory pipe whose
// far end is handed to enqueue as a local connection destined for the same port
// on the client, so every existing tunnel feature (pool, striping, resume) is
// reused as is. enqueue returns false when the connection could not be queued.
//
// The tunnel carries the request as plain HTTP: with wss/wssmux the server has
// already terminated TLS, so the service behind the client sees decrypted HTTP
// (WebSocket upgrades are proxied too).
func newBindForwardProxy(port int, enqueue func(conn net.Conn, remoteAddr string) bool) http.Handler {
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
	remoteAddr := strconv.Itoa(port)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
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
		},
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return proxy
}

package handlers

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/musix/backhaul/internal/web"
	"github.com/sirupsen/logrus"
)

// copyBufferSize bounds how much data moves per Read/Write syscall pair when
// a zero-copy path isn't available (see transferData). Bigger than the
// previous 16KB to cut syscall/goroutine-wake count per byte transferred,
// which matters more on low-core-count hosts where there's little spare
// parallelism to hide that overhead behind.
const copyBufferSize = 64 * 1024

// copyBufferPool avoids allocating a fresh buffer for every single
// connection - this handler runs per tunneled connection, and backhaul's
// own stated design goal is handling many of those concurrently.
var copyBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufferSize)
		return &b
	},
}

func TCPConnectionHandler(ctx context.Context, proxyProtocol bool, from net.Conn, to net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	// Write Proxy Protocol V2 Header
	if proxyProtocol {
		err := WriteProxyProtocol(from, to)
		if err != nil {
			logger.Error(err)
			from.Close()
			to.Close()
			return
		}
	}

	done := make(chan struct{})

	// Close both connections as soon as the transport context is cancelled
	// (e.g. on a tunnel restart). The io.CopyBuffer calls below block until the
	// peer sends EOF, so without this watcher an idle tunnelled connection would
	// linger long past the restart. Those leaked sockets pile up across repeated
	// restarts and keep splitting traffic with the fresh generation. Closing the
	// connections unblocks both copy directions at once.
	go func() {
		select {
		case <-ctx.Done():
			// A half-close envelope conn must abort BEFORE its peer conn is
			// closed: closing the plain side first makes transferData see
			// net.ErrClosed, treat it as a clean end and send END, so the remote
			// end would read a clean EOF for a cancelled flow. Deliberately only
			// this type (plan 024): other conns keep today's cancel behavior.
			for _, c := range [2]net.Conn{from, to} {
				if hc, ok := c.(*halfCloseConn); ok {
					hc.AbortWrite()
				}
			}
			from.Close()
			to.Close()
		case <-done:
		}
	}()

	// Pump both directions independently and only tear the pair down once BOTH
	// have finished. The old code fully closed both conns the moment either
	// direction ended - so a client that half-closed its upload (a clean FIN,
	// the normal end of a request) would trip a full close on the peer while its
	// reply was still in flight. On a socket with data still unread that full
	// close is an RST, discarding the buffered tail: silent reply truncation on
	// tcp/tcpmux/ws/wsmux. Each direction now half-closes only the write side it
	// finished feeding (see transferData), leaving the reverse copy free to
	// drain; the pair is fully closed here once both are done.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		transferData(from, to, logger, usage, remotePort, sniffer)
	}()
	go func() {
		defer wg.Done()
		transferData(to, from, logger, usage, remotePort, sniffer)
	}()
	wg.Wait()

	close(done)
	from.Close()
	to.Close()
}

// onlyReader hides every method but Read, the mirror of onlyWriter. Together
// they stop io.CopyBuffer from re-deriving a WriteTo/ReadFrom fast path that
// pickCopyMode has already ruled out.
type onlyReader struct {
	io.Reader
}

// copyMode is how transferData should move bytes between one particular pair of
// conns. See pickCopyMode for what each one costs.
type copyMode int

const (
	// copySplice: both ends are raw kernel sockets, so io.Copy reaches
	// splice(2) on Linux and the bytes never enter this process at all. This
	// is tcp/tcpmux forwarding, where neither end is multiplexed.
	copySplice copyMode = iota
	// copyWriteTo: the source hands the destination buffers it already holds.
	// smux.Stream's WriteTo does exactly this - the frames it received are
	// written straight out, with no intermediate buffer to copy through or
	// allocate. This is the tunnel -> socket hop of every muxed flow.
	copyWriteTo
	// copyReadFrom: the mirror of copyWriteTo, for a destination that pulls
	// into buffers of its own.
	copyReadFrom
	// copyPooled: no genuine fast path exists, so copy through the shared 64KB
	// buffer. This is the socket -> tunnel hop, which on the server carries the
	// user's upload.
	copyPooled
)

// spliceable reports whether c is a raw kernel socket, i.e. one end of a pair
// Go can move with splice(2) without the bytes ever entering this process.
// *net.TCPConn and *net.UnixConn advertise io.ReaderFrom/io.WriterTo
// unconditionally, but those methods only reach the kernel fast path when the
// OTHER end is a socket too; against anything else they fall back to a generic
// buffered copy of their own. Telling those two cases apart is the whole job of
// pickCopyMode.
func spliceable(c net.Conn) bool {
	switch c.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

// pickCopyMode chooses the cheapest copy path that actually applies to this
// pair of conns.
//
// The previous rule - "if either end implements WriteTo/ReadFrom, hand
// io.CopyBuffer a nil buffer and let it pick" - looked like a zero-copy fast
// path but was a pessimisation on every muxed transport. *net.TCPConn always
// satisfies both interfaces, so the test passed for every wsmux/wssmux flow even
// though the peer is an smux stream or a striping.Conn and no kernel path
// exists. io.Copy then called (*TCPConn).WriteTo -> net.genericWriteTo, which
// allocates its OWN 32KB buffer per connection and loops on that - so the pooled
// 64KB buffer above was unreachable dead code for exactly the flows it was added
// for. Measured on the server's upload ingress hop (user socket -> smux stream):
// 32777 B/op, 2 allocs/op, every read capped at 32KB instead of 64KB.
func pickCopyMode(from net.Conn, to net.Conn) copyMode {
	fromSock, toSock := spliceable(from), spliceable(to)
	if fromSock && toSock {
		return copySplice
	}
	if _, ok := from.(io.WriterTo); ok && !fromSock {
		return copyWriteTo
	}
	if _, ok := to.(io.ReaderFrom); ok && !toSock {
		return copyReadFrom
	}
	return copyPooled
}

// copyStream moves from -> to over whichever path pickCopyMode selected.
func copyStream(to net.Conn, from net.Conn) (int64, error) {
	switch pickCopyMode(from, to) {
	case copySplice:
		return io.Copy(to, from)
	case copyWriteTo:
		return from.(io.WriterTo).WriteTo(to)
	case copyReadFrom:
		return to.(io.ReaderFrom).ReadFrom(from)
	}

	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)
	// Both interfaces are hidden so io.CopyBuffer cannot route around the
	// pooled buffer and back into a generic copy that allocates its own.
	return io.CopyBuffer(onlyWriter{to}, onlyReader{from}, *bufPtr)
}

// transferData pumps from -> to until either side closes or errors, over
// whichever copy path copyStream picks for this pair of conns.
func transferData(from net.Conn, to net.Conn, logger *logrus.Logger, usage *web.Usage, remotePort int, sniffer bool) {
	n, err := copyStream(to, from)

	if err != nil && !errors.Is(err, net.ErrClosed) {
		// A copy that ends on a real transport error - not a clean EOF, and not
		// our own side being closed by the other direction's teardown
		// (net.ErrClosed) - means the byte stream feeding `to` was truncated at
		// the source. For a striped destination, tell it so it does not emit the
		// end-of-stream marker that would certify the partial stream as
		// complete: the far end's Read then reports io.ErrUnexpectedEOF instead
		// of a clean EOF that silently hides the lost tail. The connection is
		// broken, so tear both directions down.
		if aw, ok := to.(interface{ AbortWrite() }); ok {
			aw.AbortWrite()
		}
		from.Close()
		to.Close()
	} else {
		// Clean EOF (err == nil), or our own side was already closed by the
		// other direction (net.ErrClosed). Propagate the EOF by half-closing
		// only `to`'s write side, so an in-flight reply on the reverse copy is
		// not truncated by a full close. Conn types that can't half-close
		// (smux.Stream, striping.Conn) fall back to a full close - no worse than
		// the previous behavior for them; the reverse copy is torn down and the
		// pair is fully closed by the caller once both directions return.
		closeWrite(to)
	}

	switch {
	case err == nil:
		logger.Trace("reader stream closed or EOF received")
	case errors.Is(err, net.ErrClosed):
		logger.Trace("writer stream closed or EOF received")
	default:
		logger.Trace("unable to transfer data: ", err)
	}

	logger.Tracef("transferred data: %d bytes", n)
	if sniffer && n > 0 {
		usage.AddOrUpdatePort(remotePort, uint64(n))
	}
}

// closeWrite shuts down only the write half of c when the conn supports it
// (*net.TCPConn, *tls.Conn, and the WS-over-raw legs whose NetConn is one of
// those), sending a clean FIN that lets the peer finish replying. Conn types
// without a half-close (smux.Stream, striping.Conn) fall back to a full close.
func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	c.Close()
}

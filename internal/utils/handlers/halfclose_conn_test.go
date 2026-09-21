package handlers

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// The type must satisfy the hooks the handler already uses, with no handler change.
var (
	_ net.Conn                        = (*halfCloseConn)(nil)
	_ interface{ CloseWrite() error } = (*halfCloseConn)(nil)
	_ interface{ AbortWrite() }       = (*halfCloseConn)(nil)
)

func rawRecord(typ byte, payload []byte) []byte {
	r := []byte{typ, 0, 0}
	binary.BigEndian.PutUint16(r[1:], uint16(len(payload)))
	return append(r, payload...)
}

// readRecord parses one envelope record off the raw peer stream.
func readRecord(t *testing.T, s *smux.Stream) (typ byte, payload []byte) {
	t.Helper()
	s.SetReadDeadline(time.Now().Add(hcTestTimeout))
	var h [hcHeaderLen]byte
	if _, err := io.ReadFull(s, h[:]); err != nil {
		t.Fatalf("read record header: %v", err)
	}
	payload = make([]byte, binary.BigEndian.Uint16(h[1:]))
	if _, err := io.ReadFull(s, payload); err != nil {
		t.Fatalf("read record payload: %v", err)
	}
	return h[0], payload
}

func dieClosed(s *smux.Stream) bool {
	select {
	case <-s.GetDieCh():
		return true
	default:
		return false
	}
}

func readAllDeadline(c net.Conn) ([]byte, error) {
	c.SetReadDeadline(time.Now().Add(hcTestTimeout))
	return io.ReadAll(c)
}

func TestHalfCloseHookShape(t *testing.T) {
	a, _ := streamPair(t)
	c := NewHalfCloseConn(a)
	// Neither fast path may exist, or pickCopyMode would bypass the envelope.
	if _, ok := c.(io.WriterTo); ok {
		t.Fatal("must not implement io.WriterTo")
	}
	if _, ok := c.(io.ReaderFrom); ok {
		t.Fatal("must not implement io.ReaderFrom")
	}
	if got := pickCopyMode(c, c); got != copyPooled {
		t.Fatalf("pickCopyMode = %v, want copyPooled", got)
	}
}

// closeWrite (the existing hook) must pick CloseWrite up and must NOT close the stream.
func TestHalfCloseUsedByCloseWriteHook(t *testing.T) {
	a, b := streamPair(t)
	c := NewHalfCloseConn(a)
	closeWrite(c)
	if typ, p := readRecord(t, b); typ != hcEnd || len(p) != 0 {
		t.Fatalf("record = 0x%02x/%d bytes, want END", typ, len(p))
	}
	if dieClosed(a) {
		t.Fatal("closeWrite closed the stream; it must leave the reverse direction open")
	}
}

func TestHalfCloseWriteRecords(t *testing.T) {
	for _, size := range []int{0, 1, hcMaxData, hcMaxData + 1, 100000} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			a, b := streamPair(t)
			c := newHalfCloseConn(a)
			payload := genPayload(size)
			errc := make(chan error, 1)
			go func() {
				n, err := c.Write(payload)
				if err == nil && n != size {
					err = fmt.Errorf("Write returned n=%d, want %d", n, size)
				}
				errc <- err
			}()
			var got []byte
			var lens []int
			for len(got) < size {
				typ, p := readRecord(t, b)
				if typ != hcData || len(p) < 1 || len(p) > hcMaxData {
					t.Fatalf("record type 0x%02x len %d, want DATA 1..%d", typ, len(p), hcMaxData)
				}
				got = append(got, p...)
				lens = append(lens, len(p))
			}
			select {
			case err := <-errc:
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
			case <-time.After(hcTestTimeout):
				t.Fatal("Write did not return")
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch (%d bytes)", len(got))
			}
			var wantLens []int
			for rest := size; rest > 0; rest -= min(rest, hcMaxData) {
				wantLens = append(wantLens, min(rest, hcMaxData))
			}
			if fmt.Sprint(lens) != fmt.Sprint(wantLens) {
				t.Fatalf("record lengths %v, want %v", lens, wantLens)
			}
			// An empty write produced no record: the next one on the wire is END.
			if err := c.CloseWrite(); err != nil {
				t.Fatalf("CloseWrite: %v", err)
			}
			if typ, p := readRecord(t, b); typ != hcEnd || len(p) != 0 {
				t.Fatalf("record 0x%02x/%d bytes, want END", typ, len(p))
			}
		})
	}
}

// Round trip through two envelope conns, reading one byte at a time across a
// record boundary.
func TestHalfCloseRoundTripOneByteReads(t *testing.T) {
	a, b := streamPair(t)
	w, r := newHalfCloseConn(a), newHalfCloseConn(b)
	payload := genPayload(hcMaxData + 5)
	go func() {
		w.Write(payload)
		w.CloseWrite()
	}()
	r.SetReadDeadline(time.Now().Add(hcTestTimeout))
	var got []byte
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		got = append(got, one[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d matching", len(got), len(payload))
	}
	for i := 0; i < 3; i++ {
		if n, err := r.Read(one); n != 0 || err != io.EOF {
			t.Fatalf("Read after END = %d, %v; want 0, io.EOF repeatedly", n, err)
		}
	}
}

// A record whose bytes trickle in one at a time (header split across reads).
func TestHalfCloseBytewiseDelivery(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	wire := append(rawRecord(hcData, []byte("hello world")), rawRecord(hcEnd, nil)...)
	go func() {
		for i := range wire {
			b.Write(wire[i : i+1])
		}
	}()
	got, err := readAllDeadline(c)
	if err != nil || string(got) != "hello world" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestHalfCloseReceiverViolations(t *testing.T) {
	cases := []struct {
		name     string
		raw      []byte
		closeRaw bool // peer closes its stream after writing raw
		wantData string
	}{
		{name: "unknown type", raw: []byte{0x09, 0, 0}},
		{name: "DATA length zero", raw: []byte{hcData, 0, 0}},
		{name: "DATA above 32768", raw: []byte{hcData, 0x80, 0x01}},
		{name: "END with nonzero length", raw: []byte{hcEnd, 0, 1, 'x'}},
		{name: "ABORT with nonzero length", raw: []byte{hcAbort, 0, 1, 'x'}},
		{name: "EOF without END", closeRaw: true},
		{name: "EOF after DATA without END", raw: rawRecord(hcData, []byte("hi")), closeRaw: true, wantData: "hi"},
		{name: "EOF inside header", raw: []byte{hcData, 0}, closeRaw: true},
		{name: "EOF inside record", raw: append([]byte{hcData, 0, 10}, "abcd"...), closeRaw: true, wantData: "abcd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := streamPair(t)
			c := newHalfCloseConn(a)
			if len(tc.raw) > 0 {
				if _, err := b.Write(tc.raw); err != nil {
					t.Fatalf("raw write: %v", err)
				}
			}
			if tc.closeRaw {
				b.Close()
			}
			got, err := readAllDeadline(c)
			if string(got) != tc.wantData {
				t.Fatalf("data = %q, want %q", got, tc.wantData)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				t.Fatalf("Read error = %v, want one wrapping io.ErrUnexpectedEOF and never io.EOF", err)
			}
			if _, err2 := c.Read(make([]byte, 1)); err2 != err {
				t.Fatalf("second Read = %v, want the same sticky error %v", err2, err)
			}
			if _, err := c.Write([]byte("x")); err == nil {
				t.Fatal("Write after a protocol error succeeded")
			}
			if !dieClosed(a) {
				t.Fatal("stream not closed after a protocol error")
			}
		})
	}
}

func TestHalfCloseAbortReceived(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	b.Write(append(rawRecord(hcData, []byte("part")), rawRecord(hcAbort, nil)...))
	got, err := readAllDeadline(c)
	if string(got) != "part" {
		t.Fatalf("data = %q, want the bytes before ABORT", got)
	}
	if !errors.Is(err, errHalfCloseAborted) || errors.Is(err, io.EOF) {
		t.Fatalf("Read error = %v, want the abort error, never io.EOF", err)
	}
	if !dieClosed(a) {
		t.Fatal("stream not closed after ABORT")
	}
}

// Bytes after END are never delivered as data. (A receiver that stops reading at
// END cannot report "DATA after END" as an error: see the Phase A report.)
func TestHalfCloseNothingDeliveredAfterEND(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	b.Write(bytes.Join([][]byte{rawRecord(hcData, []byte("ok")), rawRecord(hcEnd, nil), rawRecord(hcData, []byte("xx"))}, nil))
	got, err := readAllDeadline(c)
	if err != nil || string(got) != "ok" {
		t.Fatalf("got %q, %v; want ok, nil", got, err)
	}
	if n, err := c.Read(make([]byte, 8)); n != 0 || err != io.EOF {
		t.Fatalf("Read after END = %d, %v", n, err)
	}
}

// CloseWrite sends END once, keeps the reverse direction usable, and the stream
// closes only when both directions ended.
func TestHalfCloseWriteEndKeepsReverse(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	if err := c.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatalf("second CloseWrite (idempotent): %v", err)
	}
	if typ, _ := readRecord(t, b); typ != hcEnd {
		t.Fatalf("first record 0x%02x, want END", typ)
	}
	if dieClosed(a) {
		t.Fatal("CloseWrite closed the stream while the receive side is open")
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after CloseWrite = %v, want net.ErrClosed", err)
	}
	b.Write(append(rawRecord(hcData, []byte("reply")), rawRecord(hcEnd, nil)...))
	got, err := readAllDeadline(c)
	if err != nil || string(got) != "reply" {
		t.Fatalf("reply = %q, %v", got, err)
	}
	if !dieClosed(a) {
		t.Fatal("both directions ended but the stream is still open")
	}
	// Exactly one END was sent: after it the peer sees the stream FIN, not a second END.
	b.SetReadDeadline(time.Now().Add(hcTestTimeout))
	if n, err := b.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("peer read after END = %d, %v; want FIN (io.EOF)", n, err)
	}
}

// The other order: receive side ends first, CloseWrite completes the pair.
func TestHalfCloseRecvEndThenCloseWrite(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	b.Write(rawRecord(hcEnd, nil))
	if got, err := readAllDeadline(c); err != nil || len(got) != 0 {
		t.Fatalf("got %q, %v", got, err)
	}
	if dieClosed(a) {
		t.Fatal("stream closed with the send side still open")
	}
	if _, err := c.Write([]byte("still writable")); err != nil {
		t.Fatalf("Write after peer END: %v", err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if !dieClosed(a) {
		t.Fatal("both directions ended but the stream is still open")
	}
	if typ, p := readRecord(t, b); typ != hcData || string(p) != "still writable" {
		t.Fatalf("record 0x%02x %q", typ, p)
	}
	if typ, _ := readRecord(t, b); typ != hcEnd {
		t.Fatalf("record 0x%02x, want END (it must not be lost to the close)", typ)
	}
}

func TestHalfCloseCloseSendsAbortAndIsIdempotent(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	c.AbortWrite() // also idempotent
	if typ, p := readRecord(t, b); typ != hcAbort || len(p) != 0 {
		t.Fatalf("record 0x%02x/%d bytes, want ABORT", typ, len(p))
	}
	b.SetReadDeadline(time.Now().Add(hcTestTimeout))
	if n, err := b.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("peer read after ABORT = %d, %v; want a single ABORT then FIN", n, err)
	}
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read after Close = %v, want net.ErrClosed", err)
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Close = %v, want net.ErrClosed", err)
	}
	if err := c.CloseWrite(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("CloseWrite after Close = %v, want net.ErrClosed", err)
	}
	if !dieClosed(a) {
		t.Fatal("Close left the stream open")
	}
}

// After CloseWrite the send side is done, so Close has nothing to abort: the
// peer sees END then the FIN, no ABORT.
func TestHalfCloseCloseAfterCloseWriteSendsNoAbort(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	c.CloseWrite()
	c.Close()
	if typ, _ := readRecord(t, b); typ != hcEnd {
		t.Fatalf("record 0x%02x, want END", typ)
	}
	b.SetReadDeadline(time.Now().Add(hcTestTimeout))
	if n, err := b.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("peer read = %d, %v; want FIN with no ABORT", n, err)
	}
	// The peer's Read sees END and then a stream that ended without END from its
	// own send side: it can still finish reading cleanly.
}

func TestHalfCloseCloseUnblocksRead(t *testing.T) {
	a, _ := streamPair(t)
	c := newHalfCloseConn(a)
	started := make(chan struct{})
	errc := make(chan error, 1)
	go func() {
		close(started)
		c.SetReadDeadline(time.Now().Add(hcTestTimeout))
		_, err := c.Read(make([]byte, 8))
		errc <- err
	}()
	<-started
	c.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked Read returned %v, want net.ErrClosed", err)
		}
	case <-time.After(hcTestTimeout):
		t.Fatal("Close did not unblock Read")
	}
}

// A Write blocked on the peer's flow-control window is released by Close (which
// skips the ABORT because a Write is in flight).
func TestHalfCloseCloseUnblocksWrite(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	big := genPayload(16 << 20) // far beyond any smux window; the peer stops reading
	type res struct {
		n   int
		err error
	}
	resc := make(chan res, 1)
	go func() {
		c.SetWriteDeadline(time.Now().Add(2 * hcTestTimeout))
		n, err := c.Write(big)
		resc <- res{n, err}
	}()
	readRecord(t, b) // progress has started; the peer now stops consuming
	c.Close()
	select {
	case r := <-resc:
		if !errors.Is(r.err, net.ErrClosed) || r.n >= len(big) {
			t.Fatalf("blocked Write = %d, %v; want a short write and net.ErrClosed", r.n, r.err)
		}
	case <-time.After(hcTestTimeout):
		t.Fatal("Close did not unblock Write")
	}
}

// Both directions concurrently, from several goroutines including racing
// CloseWrite calls.
func TestHalfCloseConcurrent(t *testing.T) {
	a, b := streamPair(t)
	ca, cb := newHalfCloseConn(a), newHalfCloseConn(b)
	pa, pb := genPayload(300000), genPayload(250001)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	send := func(c *halfCloseConn, p []byte) {
		defer wg.Done()
		if n, err := c.Write(p); err != nil || n != len(p) {
			errs <- fmt.Errorf("Write = %d, %v", n, err)
			return
		}
		var cw sync.WaitGroup
		for i := 0; i < 4; i++ {
			cw.Add(1)
			go func() {
				defer cw.Done()
				if err := c.CloseWrite(); err != nil {
					errs <- fmt.Errorf("CloseWrite: %v", err)
				}
			}()
		}
		cw.Wait()
	}
	recv := func(c *halfCloseConn, want []byte) {
		defer wg.Done()
		got, err := readAllDeadline(c)
		if err != nil || !bytes.Equal(got, want) {
			errs <- fmt.Errorf("received %d bytes (%v), want %d matching", len(got), err, len(want))
		}
	}
	wg.Add(4)
	go send(ca, pa)
	go send(cb, pb)
	go recv(ca, pb)
	go recv(cb, pa)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !dieClosed(a) || !dieClosed(b) {
		t.Fatal("both directions ended on both sides but a stream is still open")
	}
}

// A read timeout is not fatal and loses no bytes, mid-header or mid-record.
func TestHalfCloseReadTimeoutIsResumable(t *testing.T) {
	a, b := streamPair(t)
	c := newHalfCloseConn(a)
	wire := append(rawRecord(hcData, []byte("0123456789")), rawRecord(hcEnd, nil)...)
	isTO := func(err error) bool {
		var ne net.Error
		return errors.As(err, &ne) && ne.Timeout()
	}
	// Mid-header: one byte of the header only.
	b.Write(wire[:1])
	c.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := c.Read(make([]byte, 4)); !isTO(err) {
		t.Fatalf("Read = %v, want a timeout", err)
	}
	// Mid-record: rest of the header plus 4 payload bytes.
	b.Write(wire[1:7])
	got := make([]byte, 0, 10)
	buf := make([]byte, 4)
	c.SetReadDeadline(time.Now().Add(hcTestTimeout))
	for len(got) < 4 {
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, buf[:n]...)
	}
	c.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := c.Read(buf); !isTO(err) {
		t.Fatalf("Read = %v, want a timeout", err)
	}
	b.Write(wire[7:])
	rest, err := readAllDeadline(c)
	if err != nil || string(got)+string(rest) != "0123456789" {
		t.Fatalf("got %q + %q, %v", got, rest, err)
	}
}

// partialConn fails writes the way a deadline hit mid-frame would.
type partialConn struct {
	net.Conn
	n   int
	err error
}

func (p *partialConn) Write([]byte) (int, error) { return p.n, p.err }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "write timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// After a record was cut short, further records would corrupt the stream, so
// the send side must stay failed; a write that put nothing on the wire may retry.
func TestHalfCloseShortWritePoisonsSendSide(t *testing.T) {
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	pc := &partialConn{Conn: x, n: 5, err: timeoutErr{}}
	c := newHalfCloseConn(pc)
	if _, err := c.Write([]byte("hello")); err == nil {
		t.Fatal("expected the partial write to fail")
	}
	pc.n, pc.err = 0, nil
	if _, err := c.Write([]byte("more")); err == nil {
		t.Fatal("Write after a cut-short record succeeded")
	}
	if err := c.CloseWrite(); err == nil {
		t.Fatal("CloseWrite after a cut-short record succeeded")
	}

	pc2 := &partialConn{Conn: x, n: 0, err: timeoutErr{}}
	c2 := newHalfCloseConn(pc2)
	if _, err := c2.Write([]byte("hello")); err == nil {
		t.Fatal("expected the zero-byte write to time out")
	}
	pc2.err = nil
	pc2.n = hcHeaderLen + 4
	if n, err := c2.Write([]byte("more")); err != nil || n != 4 {
		t.Fatalf("retry after a write that put nothing on the wire = %d, %v", n, err)
	}
}

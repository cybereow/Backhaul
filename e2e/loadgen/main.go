// Command loadgen drives traffic for the end-to-end tunnel tests.
//
// It runs as either the origin service or the client. The client measures two
// workloads, because they stress different things:
//
//   - bulk: one long stream. Dominated by copy loops and socket buffer sizes;
//     this is what a file transfer or a speedtest looks like.
//   - rr:   small request/response exchanges. Dominated by per-message overhead
//   - syscalls, framing, wakeups - which is where a tunnel's framing layer
//     actually shows up, and what interactive traffic (SSH, gaming, HTTP APIs)
//     really looks like.
//
// Every byte is verified against a deterministic pattern, so a run that reports
// numbers has also proved the tunnel did not corrupt or truncate the stream.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"
)

// patternBlock is the repeating body of every stream. Verification is a
// comparison against this block at the right offset, which is cheap enough not
// to become the bottleneck being measured.
const patternSize = 64 * 1024

func buildPattern() []byte {
	b := make([]byte, patternSize)
	// A cheap deterministic fill; any position-dependent value works, it only
	// has to catch truncation, reordering and corruption.
	x := uint32(0x9e3779b9)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

var pattern = buildPattern()

// verify checks buf against the pattern starting at stream offset off.
func verify(buf []byte, off int64) error {
	for i := 0; i < len(buf); {
		p := int((off + int64(i)) % patternSize)
		n := len(buf) - i
		if n > patternSize-p {
			n = patternSize - p
		}
		for j := 0; j < n; j++ {
			if buf[i+j] != pattern[p+j] {
				return fmt.Errorf("payload mismatch at stream offset %d", off+int64(i+j))
			}
		}
		i += n
	}
	return nil
}

// fill writes the pattern for stream offset off into buf.
func fill(buf []byte, off int64) {
	for i := 0; i < len(buf); {
		p := int((off + int64(i)) % patternSize)
		n := copy(buf[i:], pattern[p:])
		i += n
	}
}

type report struct {
	Mode      string  `json:"mode"`
	Bytes     int64   `json:"bytes"`
	Seconds   float64 `json:"seconds"`
	MBPerSec  float64 `json:"mb_per_sec"`
	Requests  int     `json:"requests,omitempty"`
	ReqPerSec float64 `json:"req_per_sec,omitempty"`
	P50Micros int64   `json:"p50_micros,omitempty"`
	P99Micros int64   `json:"p99_micros,omitempty"`
	Verified  bool    `json:"verified"`
}

func main() {
	role := flag.String("role", "client", "origin or client")
	listen := flag.String("listen", "127.0.0.1:9000", "origin listen address")
	connect := flag.String("connect", "127.0.0.1:9000", "client target address")
	mode := flag.String("mode", "bulk", "bulk or rr")
	nbytes := flag.Int64("bytes", 64<<20, "bulk: bytes to transfer")
	requests := flag.Int("requests", 2000, "rr: number of exchanges")
	reqSize := flag.Int("size", 512, "rr: bytes per exchange")
	timeout := flag.Duration("timeout", 120*time.Second, "overall client deadline")
	flag.Parse()

	var err error
	switch *role {
	case "origin":
		err = runOrigin(*listen)
	case "client":
		err = runClient(*connect, *mode, *nbytes, *requests, *reqSize, *timeout)
	default:
		err = fmt.Errorf("unknown role %q", *role)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

// runOrigin is the service the tunnel forwards to. It speaks a one-line
// command protocol so a single origin serves both workloads.
func runOrigin(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Fprintln(os.Stderr, "origin listening on", addr)

	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go serveOrigin(c)
	}
}

func serveOrigin(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}

	var n int64
	switch {
	case line == "ECHO\n":
		// Echo every byte back until the peer half-closes. Used by rr.
		buf := make([]byte, 64*1024)
		io.CopyBuffer(c, br, buf)
		return
	default:
		if _, err := fmt.Sscanf(line, "BULK %d", &n); err != nil {
			return
		}
	}

	buf := make([]byte, 64*1024)
	var off int64
	for off < n {
		chunk := int64(len(buf))
		if remaining := n - off; remaining < chunk {
			chunk = remaining
		}
		fill(buf[:chunk], off)
		if _, err := c.Write(buf[:chunk]); err != nil {
			return
		}
		off += chunk
	}
}

func runClient(addr, mode string, nbytes int64, requests, reqSize int, timeout time.Duration) error {
	switch mode {
	case "bulk":
		return clientBulk(addr, nbytes, timeout)
	case "rr":
		return clientRR(addr, requests, reqSize, timeout)
	}
	return fmt.Errorf("unknown mode %q", mode)
}

// clientBulk pulls one long stream and verifies every byte.
func clientBulk(addr string, nbytes int64, timeout time.Duration) error {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprintf(c, "BULK %d\n", nbytes); err != nil {
		return err
	}

	buf := make([]byte, 64*1024)
	var off int64
	start := time.Now()
	for off < nbytes {
		n, err := c.Read(buf)
		if n > 0 {
			if err := verify(buf[:n], off); err != nil {
				return err
			}
			off += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	elapsed := time.Since(start)

	if off != nbytes {
		return fmt.Errorf("stream truncated: got %d of %d bytes", off, nbytes)
	}

	emit(report{
		Mode:     "bulk",
		Bytes:    off,
		Seconds:  elapsed.Seconds(),
		MBPerSec: float64(off) / elapsed.Seconds() / (1 << 20),
		Verified: true,
	})
	return nil
}

// clientRR runs small request/response exchanges over one connection, the
// workload where per-message overhead dominates.
func clientRR(addr string, requests, size int, timeout time.Duration) error {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprint(c, "ECHO\n"); err != nil {
		return err
	}

	out := make([]byte, size)
	in := make([]byte, size)
	lat := make([]time.Duration, 0, requests)

	start := time.Now()
	var off int64
	for i := 0; i < requests; i++ {
		fill(out, off)
		t0 := time.Now()
		if _, err := c.Write(out); err != nil {
			return err
		}
		if _, err := io.ReadFull(c, in); err != nil {
			return fmt.Errorf("exchange %d: %w", i, err)
		}
		lat = append(lat, time.Since(t0))
		if err := verify(in, off); err != nil {
			return fmt.Errorf("exchange %d: %w", i, err)
		}
		off += int64(size)
	}
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	total := int64(requests) * int64(size)

	emit(report{
		Mode:      "rr",
		Bytes:     total * 2, // each byte travels out and back
		Seconds:   elapsed.Seconds(),
		MBPerSec:  float64(total*2) / elapsed.Seconds() / (1 << 20),
		Requests:  requests,
		ReqPerSec: float64(requests) / elapsed.Seconds(),
		P50Micros: lat[len(lat)*50/100].Microseconds(),
		P99Micros: lat[len(lat)*99/100].Microseconds(),
		Verified:  true,
	})
	return nil
}

func emit(r report) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

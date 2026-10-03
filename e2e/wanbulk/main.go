// wanbulk measures bulk throughput through a backhaul tunnel whose inter-site
// link has a fixed one-way delay, without needing tc/netem: a userspace proxy
// delays every chunk by -delay in each direction (RTT = 2*delay) and does not limit
// bandwidth. It starts a source/sink origin, the proxy, and a backhaul server and
// client (the binary given by -bin) joined through the proxy, then moves -mb MiB
// from the origin to a user (download) and from a user to the origin (upload),
// several flows at once if -flows > 1, and prints MB/s for each direction.
//
//	go build -o /tmp/backhaul . && go run ./e2e/wanbulk -bin /tmp/backhaul -delay 40ms \
//	    -server-extra 'cdn_max_age = 3600' -client-extra ''
//
// Not part of CI.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

var (
	bin      = flag.String("bin", "", "backhaul binary")
	delay    = flag.Duration("delay", 40*time.Millisecond, "one-way delay of the tunnel link (0 = direct)")
	mb       = flag.Int("mb", 256, "MiB per flow and direction")
	flows    = flag.Int("flows", 1, "parallel flows")
	base     = flag.Int("base", 24000, "port base")
	tr       = flag.String("tr", "wsmux", "transport")
	srvExtra = flag.String("server-extra", "", "extra [server] config lines, \\n separated")
	cliExtra = flag.String("client-extra", "", "extra [client] config lines, \\n separated")
	label    = flag.String("label", "", "printed with the result")
)

type chunk struct {
	b   []byte
	due time.Time
}

// pipe copies src to dst, delivering each chunk d after it was read.
func pipe(dst, src net.Conn, d time.Duration) {
	q := make(chan chunk, 8192)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range q {
			if w := time.Until(c.due); w > 0 {
				time.Sleep(w)
			}
			if _, err := dst.Write(c.b); err != nil {
				for range q { // drain
				}
				return
			}
		}
	}()
	for {
		b := make([]byte, 64<<10)
		n, err := src.Read(b)
		if n > 0 {
			q <- chunk{b[:n], time.Now().Add(d)}
		}
		if err != nil {
			break
		}
	}
	close(q)
	<-done
	if tc, ok := dst.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
}

func proxy(l net.Listener, target string, d time.Duration) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				return
			}
			if d == 0 {
				go func() { io.Copy(s, c); s.Close() }()
				io.Copy(c, s)
				c.Close()
				return
			}
			go pipe(s, c, d)
			pipe(c, s, d)
			c.Close()
			s.Close()
		}()
	}
}

// origin: a connection starts with a mode byte and an 8-byte size: 'D' is sent that
// many bytes (download); 'U' has that many read and discarded, then answered with
// a byte.
func origin(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			var m [9]byte
			if _, err := io.ReadFull(c, m[:]); err != nil {
				return
			}
			total := int64(binary.BigEndian.Uint64(m[1:]))
			if m[0] == 'D' {
				buf := make([]byte, 256<<10)
				for left := total; left > 0; {
					n := int64(len(buf))
					if n > left {
						n = left
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
					left -= n
				}
				return
			}
			io.CopyN(io.Discard, c, total)
			c.Write([]byte{1})
		}()
	}
}

func run(pub string, mode byte, total int64) float64 {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *flows; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", pub)
			if err != nil {
				fmt.Println("dial:", err)
				return
			}
			defer c.Close()
			var hdr [9]byte
			hdr[0] = mode
			binary.BigEndian.PutUint64(hdr[1:], uint64(total))
			c.Write(hdr[:])
			if mode == 'D' {
				n, err := io.Copy(io.Discard, c)
				if err != nil || n != total {
					fmt.Printf("download: %d of %d bytes, %v\n", n, total, err)
				}
				return
			}
			buf := make([]byte, 256<<10)
			for left := total; left > 0; {
				n := int64(len(buf))
				if n > left {
					n = left
				}
				if _, err := c.Write(buf[:n]); err != nil {
					fmt.Println("upload:", err)
					return
				}
				left -= n
			}
			var b [1]byte
			if _, err := io.ReadFull(c, b[:]); err != nil {
				fmt.Println("upload ack:", err)
			}
		}()
	}
	wg.Wait()
	return float64(total*int64(*flows)) / time.Since(start).Seconds() / 1e6
}

func main() {
	flag.Parse()
	O, P, T, PUB := *base, *base+1, *base+2, *base+3
	total := int64(*mb) << 20
	dir, _ := os.MkdirTemp("", "wanbulk")
	defer os.RemoveAll(dir)
	eo, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", O))
	go origin(eo)
	pl, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", P))
	go proxy(pl, fmt.Sprintf("127.0.0.1:%d", T), *delay)

	nl := func(s string) string {
		if s == "" {
			return ""
		}
		return fmt.Sprintf("%s\n", unescape(s))
	}
	os.WriteFile(filepath.Join(dir, "s.toml"), []byte(fmt.Sprintf(`[server]
bind_addr = "127.0.0.1:%d"
transport = "%s"
token = "t"
channel_size = 2048
keepalive_period = 75
heartbeat = 10
nodelay = true
mux_version = 2
log_level = "error"
ports = ["%d=127.0.0.1:%d"]
%s`, T, *tr, PUB, O, nl(*srvExtra))), 0644)
	os.WriteFile(filepath.Join(dir, "c.toml"), []byte(fmt.Sprintf(`[client]
remote_addr = "127.0.0.1:%d"
transport = "%s"
token = "t"
connection_pool = 4
keepalive_period = 75
dial_timeout = 5
retry_interval = 1
nodelay = true
mux_version = 2
log_level = "error"
%s`, P, *tr, nl(*cliExtra))), 0644)
	sv := exec.Command(*bin, "-c", filepath.Join(dir, "s.toml"))
	sv.Start()
	time.Sleep(time.Second)
	cl := exec.Command(*bin, "-c", filepath.Join(dir, "c.toml"))
	cl.Start()
	defer func() { sv.Process.Kill(); cl.Process.Kill() }()
	time.Sleep(4 * time.Second)

	pub := fmt.Sprintf("127.0.0.1:%d", PUB)
	run(pub, 'D', 8<<20) // warm up the pool
	down := run(pub, 'D', total)
	up := run(pub, 'U', total)
	fmt.Printf("RESULT %s delay=%v flows=%d: download %.1f MB/s, upload %.1f MB/s\n", *label, *delay, *flows, down, up)
}

func unescape(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && s[i+1] == 'n' {
			out = append(out, '\n')
			i++
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

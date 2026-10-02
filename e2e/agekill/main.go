// agekill measures what a CDN's maximum connection age does to flows through a
// backhaul tunnel. It starts an echo origin, a proxy that closes every
// connection through it at a fixed age (the "CDN"), a backhaul server and client
// (the binary given by -bin) joined through that proxy, and then drives load:
// short request/response flows, and a few long-lived flows that ping every 250ms
// and are reopened whenever they break.
//
// The interesting result is "long flows broken": without cdn_max_age every long
// flow dies at the proxy's age; with cdn_max_age set to the same age (and
// mux_version 2) the flows are moved to a fresh pool connection before the cut
// and none breaks.
//
//	go build -o /tmp/backhaul . && go run ./e2e/agekill -bin /tmp/backhaul -cdn 30 -rot 30 -dur 120
//
// Not part of CI: a meaningful run takes minutes (cdn_max_age has a 30s floor).
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	bin    = flag.String("bin", "", "backhaul binary")
	cdnAge = flag.Int("cdn", 30, "proxy kills every connection at this age (s)")
	rot    = flag.Int("rot", 0, "cdn_max_age on the server (0 = off: flows die at the proxy age)")
	dur    = flag.Int("dur", 150, "test duration (s)")
	base   = flag.Int("base", 20000, "port base")
	tr     = flag.String("tr", "wsmux", "transport")
	label  = flag.String("label", "", "")
)

func echo(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() { defer c.Close(); io.Copy(c, c) }()
	}
}

func proxy(l net.Listener, target string, age time.Duration) {
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
			t := time.AfterFunc(age, func() { c.Close(); s.Close() })
			go func() { io.Copy(s, c); s.Close(); c.Close() }()
			io.Copy(c, s)
			t.Stop()
			c.Close()
			s.Close()
		}()
	}
}

func main() {
	flag.Parse()
	O, P, T, PUB := *base, *base+1, *base+2, *base+3
	dir, _ := os.MkdirTemp("", "exp")
	eo, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", O))
	go echo(eo)
	pl, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", P))
	go proxy(pl, fmt.Sprintf("127.0.0.1:%d", T), time.Duration(*cdnAge)*time.Second)

	rotLine := ""
	if *rot > 0 {
		rotLine = fmt.Sprintf("cdn_max_age = %d\n", *rot)
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
log_level = "warn"
ports = ["%d=127.0.0.1:%d"]
%s`, T, *tr, PUB, O, rotLine)), 0644)
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
log_level = "warn"
`, P, *tr)), 0644)
	sv := exec.Command(*bin, "-c", filepath.Join(dir, "s.toml"))
	sv.Stdout, sv.Stderr = os.Stdout, os.Stderr
	sv.Start()
	time.Sleep(time.Second)
	cl := exec.Command(*bin, "-c", filepath.Join(dir, "c.toml"))
	cl.Stdout, cl.Stderr = os.Stdout, os.Stderr
	cl.Start()
	defer func() { sv.Process.Kill(); cl.Process.Kill() }()
	time.Sleep(4 * time.Second)

	pub := fmt.Sprintf("127.0.0.1:%d", PUB)
	var ok, fail int64
	stop := time.Now().Add(time.Duration(*dur) * time.Second)
	var wg sync.WaitGroup
	// short flows, 10/s
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for time.Now().Before(stop) {
			<-tk.C
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", pub, 3*time.Second)
				if err != nil {
					atomic.AddInt64(&fail, 1)
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(5 * time.Second))
				buf := make([]byte, 256)
				if _, err := c.Write(buf); err != nil {
					atomic.AddInt64(&fail, 1)
					return
				}
				if _, err := io.ReadFull(c, buf); err != nil {
					atomic.AddInt64(&fail, 1)
					return
				}
				atomic.AddInt64(&ok, 1)
			}()
		}
	}()
	// long flows
	var mu sync.Mutex
	var lifes []time.Duration
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Duration(*cdnAge) * time.Second / 4)
			for time.Now().Before(stop) {
				c, err := net.DialTimeout("tcp", pub, 3*time.Second)
				if err != nil {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				start := time.Now()
				buf := make([]byte, 64)
				for time.Now().Before(stop) {
					c.SetDeadline(time.Now().Add(3 * time.Second))
					if _, err := c.Write(buf); err != nil {
						break
					}
					if _, err := io.ReadFull(c, buf); err != nil {
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
				c.Close()
				if time.Now().Before(stop) {
					mu.Lock()
					lifes = append(lifes, time.Since(start))
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	mu.Lock()
	var sum time.Duration
	for _, l := range lifes {
		sum += l
	}
	avg := time.Duration(0)
	if len(lifes) > 0 {
		avg = sum / time.Duration(len(lifes))
	}
	fmt.Printf("RESULT %s tr=%s cdn=%ds rot=%d: short ok=%d fail=%d | long flows broken=%d avgLife=%s\n", *label, *tr, *cdnAge, *rot, ok, fail, len(lifes), avg.Round(time.Second))
	mu.Unlock()
}

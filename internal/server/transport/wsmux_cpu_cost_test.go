package transport

import (
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	client_transport "github.com/musix/backhaul/internal/client/transport"
	"github.com/sirupsen/logrus"
)

func cpuCostEnv(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// TestWsMuxCPUCost measures what a byte costs: bulk flows through a real server
// and a real client over loopback, reported as throughput and CPU seconds per
// GiB. The figure covers both ends of the tunnel and the load on either side of
// it, so it is for comparing one build with another, not an absolute. Run it on
// one thread to see the ceiling of a single core:
//
//	BACKHAUL_CPU_COST=1 GOMAXPROCS=1 go test ./internal/server/transport -run TestWsMuxCPUCost -v
//
// CPU_COST_DIR is D (to the user), U (from the user) or B; CPU_COST_FLOWS and
// CPU_COST_SECS size the run; CPU_COST_RESUME_S=0 turns flow resumption off.
// Add -cpuprofile to see where the time goes.
func TestWsMuxCPUCost(t *testing.T) {
	if os.Getenv("BACKHAUL_CPU_COST") == "" {
		t.Skip("a measurement, not a check: set BACKHAUL_CPU_COST=1 to run it")
	}
	flows := cpuCostEnv("CPU_COST_FLOWS", 8)
	secs := cpuCostEnv("CPU_COST_SECS", 10)
	resume := time.Duration(cpuCostEnv("CPU_COST_RESUME_S", 30)) * time.Second
	dir := os.Getenv("CPU_COST_DIR")
	if dir == "" {
		dir = "D"
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var down, up atomic.Int64
	serverAddr, userAddr := lcFreeAddr(t), lcFreeAddr(t)
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				mode := make([]byte, 1)
				if _, err := io.ReadFull(c, mode); err != nil {
					return
				}
				b := make([]byte, 64<<10)
				if mode[0] == 'U' {
					for {
						n, err := c.Read(b)
						up.Add(int64(n))
						if err != nil {
							return
						}
					}
				}
				for {
					if _, err := c.Write(b); err != nil {
						return
					}
				}
			}()
		}
	}()

	maxAge, maxDrain := RotationPlan(180 * time.Second)
	NewWSMuxServer(ctx, &WsMuxConfig{
		BindAddr: serverAddr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2, MuxCon: 32,
		ChannelSize: 8192, MaxFrameSize: 32768, MaxReceiveBuffer: 64 << 20, MaxStreamBuffer: 16 << 20,
		KeepAlive: 30 * time.Second, Heartbeat: 20 * time.Second, Nodelay: true,
		MaxConnAge: maxAge, MaxDrain: maxDrain, ResumeWindow: resume, WSFraming: true,
		Ports: []string{userAddr + "=" + backend.Addr().String()},
	}, logger).Start()
	lcWaitFor(t, "server", func() bool {
		c, err := net.Dial("tcp", serverAddr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	go client_transport.NewWSMuxClient(ctx, &client_transport.WsMuxConfig{
		RemoteAddr: serverAddr, Token: lcToken, Mode: config.WSMUX, Path: "/", MuxVersion: 2,
		ConnPoolSize: 16, DialTimeOut: 5 * time.Second, RetryInterval: 100 * time.Millisecond,
		KeepAlive: 30 * time.Second, Nodelay: true, MaxFrameSize: 32768, MaxReceiveBuffer: 64 << 20,
		MaxStreamBuffer: 16 << 20, WSFraming: true, ResumeWindow: resume,
	}, logger).Start()
	lcWaitFor(t, "tunnel", func() bool {
		x, err := net.DialTimeout("tcp", userAddr, time.Second)
		if err != nil {
			return false
		}
		defer x.Close()
		_, err = x.Write([]byte("U"))
		return err == nil
	})
	time.Sleep(2 * time.Second) // the pool fills

	flow := func(mode byte) {
		c, err := net.Dial("tcp", userAddr)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		if _, err := c.Write([]byte{mode}); err != nil {
			return
		}
		b := make([]byte, 64<<10)
		if mode == 'U' {
			for ctx.Err() == nil {
				if _, err := c.Write(b); err != nil {
					return
				}
			}
			return
		}
		for {
			n, err := c.Read(b)
			down.Add(int64(n))
			if err != nil {
				return
			}
		}
	}
	for i := 0; i < flows; i++ {
		if dir == "D" || dir == "B" {
			go flow('D')
		}
		if dir == "U" || dir == "B" {
			go flow('U')
		}
	}
	time.Sleep(3 * time.Second) // warm up
	c0, d0, u0, t0 := processCPU(), down.Load(), up.Load(), time.Now()
	time.Sleep(time.Duration(secs) * time.Second)
	cpu, d, u, el := processCPU()-c0, down.Load()-d0, up.Load()-u0, time.Since(t0).Seconds()
	if d+u == 0 {
		t.Fatal("nothing got through")
	}
	t.Logf("flows=%d dir=%s resume=%v: to the user %.0f Mbit/s, from the user %.0f Mbit/s, %.2f cores, %.2f CPU seconds per GiB",
		flows, dir, resume, float64(d)*8/el/1e6, float64(u)*8/el/1e6, cpu.Seconds()/el, cpu.Seconds()/(float64(d+u)/(1<<30)))
}

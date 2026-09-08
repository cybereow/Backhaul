package cmd

import (
	"fmt"
	"os/exec"
	"runtime"
	"syscall"
)

// applyTCPTuning applies temporary TCP optimizations for Linux to handle massive connections
func ApplyTCPTuning() {
	if runtime.GOOS == "linux" {
		logger.Info("Applying TCP optimizations for Linux...")

		// Define the buffer sizes to try
		bufferSizes := []int{
			256 * 1024 * 1024, // 256MB
			128 * 1024 * 1024, // 128MB
			64 * 1024 * 1024,  // 64MB
			32 * 1024 * 1024,  // 32MB
			16 * 1024 * 1024,  // 16MB
		}

		// Loop through buffer sizes and attempt to apply them
		for _, size := range bufferSizes {
			// Build the command with the current buffer size
			cmd := []string{"sysctl", "-w", fmt.Sprintf("net.core.rmem_max=%d", size)}
			if err := exec.Command(cmd[0], cmd[1:]...).Run(); err == nil {
				logger.Printf("Successfully set rmem_max to %d\n", size)
				break // Exit the loop if successful
			} else {
				logger.Debugf("Failed to set rmem_max to %d, trying next lower value...\n", size)
			}
		}

		// Same for wmem_max
		for _, size := range bufferSizes {
			// Build the command with the current buffer size for wmem_max
			cmd := []string{"sysctl", "-w", fmt.Sprintf("net.core.wmem_max=%d", size)}
			if err := exec.Command(cmd[0], cmd[1:]...).Run(); err == nil {
				logger.Printf("Successfully set wmem_max to %d\n", size)
				break // Exit the loop if successful
			} else {
				logger.Debugf("Failed to set wmem_max to %d, trying next lower value...\n", size)
			}
		}

		// Commands for optimizing TCP parameters
		commands := [][]string{
			{"sysctl", "-w", "net.ipv4.ip_local_port_range=1024 65535"}, // Increase ephemeral ports
			{"sysctl", "-w", "net.ipv4.tcp_tw_reuse=1"},                 // Reuse TIME_WAIT sockets
			{"sysctl", "-w", "net.ipv4.tcp_fin_timeout=15"},             // Reduce TCP FIN timeout
			{"sysctl", "-w", "net.core.somaxconn=65536"},                // Increase max queue length of incoming connections
			{"sysctl", "-w", "net.ipv4.tcp_max_syn_backlog=20480"},      // Increase SYN request backlog
			{"sysctl", "-w", "net.ipv4.tcp_window_scaling=1"},           // Enable TCP window scaling
			{"sysctl", "-w", "net.ipv4.tcp_fastopen=3"},                 // Enable TCP Fast Open
			// BBR congestion control + the fq qdisc it needs. On a
			// high-RTT, mildly-lossy intercontinental link - the exact
			// shape of a backhaul tunnel between a censored region and an
			// exit node - the default (cubic/reno) collapses its window on
			// every stray packet loss and never fills the pipe, so a 1Gbps
			// path stalls out well below line rate. BBR paces to the
			// measured bottleneck bandwidth instead of reacting to loss,
			// which is the single biggest throughput win on these links and
			// costs no extra CPU. If the tcp_bbr module isn't loadable the
			// sysctl just fails and is logged (non-fatal), same as any other
			// entry here.
			{"sysctl", "-w", "net.core.default_qdisc=fq"},
			{"sysctl", "-w", "net.ipv4.tcp_congestion_control=bbr"},
			// Raise the TCP autotuning ceilings (the third value of each triple).
			// This is the fix for asymmetric upload throughput on the server side.
			//
			// The wsmux/wssmux server accepts its tunnel sockets through net/http
			// (and tls.Listen), which never calls setsockopt(SO_SNDBUF) on them, so
			// their send window is left entirely to kernel autotuning - bounded by
			// tcp_wmem[2]. The kernel default for that ceiling is only 4 MB, while
			// tcp_rmem[2] defaults to 32 MB (8x larger). On a high-RTT tunnel that
			// asymmetry caps the server->client direction (the user's *upload*) hard:
			// at ~94 ms RTT a 4 MB send window tops out at 4 MB / 0.094 s ~= 357 Mbps
			// no matter how fast the link is, while the download direction - governed
			// by the 32 MB receive ceiling - runs unthrottled. Raising net.core.wmem_max
			// above does NOT help here: that clamp only bounds an *explicit* SO_SNDBUF,
			// and an autotuned socket never sets one, so tcp_wmem[2] is the only lever.
			//
			// Raising only the max (leaving min/default modest) lets autotuning grow a
			// socket's buffer to fill the BDP when a bulk flow actually needs it, while
			// idle sockets still start small - so this costs no memory on the many
			// short-lived proxied connections, unlike a forced fixed buffer.
			{"sysctl", "-w", "net.ipv4.tcp_rmem=4096 131072 67108864"}, // recv autotuning: max 64 MB
			{"sysctl", "-w", "net.ipv4.tcp_wmem=4096 16384 67108864"},  // send autotuning: max 64 MB (was 4 MB default - the upload cap)
			// tcp_notsent_lowat deliberately left at the OS default (unlimited).
			// A low value trades throughput for latency by forcing small,
			// frequent writes - the right tradeoff for many concurrent
			// short-lived proxied connections, the wrong one for the few
			// sustained, bulk-throughput connections a tunnel pool actually
			// carries. It's also a system-wide sysctl, so setting it here was
			// silently capping every other process on the box too (nginx's
			// TLS connections included), not just backhaul's own sockets.
			//{"sysctl", "-w", "net.core.rmem_max=26214400"},       // Set maximum TCP receive buffer size
			//{"sysctl", "-w", "net.core.wmem_max=26214400"},       // Set maximum TCP send buffer size
			{"sysctl", "-w", "net.core.rmem_default=1048576"}, // Set default TCP receive buffer size
			{"sysctl", "-w", "net.core.wmem_default=1048576"}, // Set default TCP send buffer size
		}

		// Execute the sysctl commands
		for _, cmd := range commands {
			err := exec.Command(cmd[0], cmd[1:]...).Run()
			if err != nil {
				logger.Errorf("Failed to apply TCP tuning: %s", cmd)
			} else {
				logger.Debugf("Successfully applied: %s", cmd)
			}
		}

		// Set file descriptor limit programmatically
		var rLimit syscall.Rlimit
		err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			logger.Errorf("Error getting Rlimit: %v", err)
		} else {
			logger.Debugf("Current file descriptor limit: %d", rLimit.Cur)

			// Set the maximum and current file descriptor limits to 1048576
			rLimit.Max = 1048576
			rLimit.Cur = 1048576
			err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
			if err != nil {
				logger.Errorf("Error setting Rlimit: %v", err)
			} else {
				logger.Debugf("Successfully set file descriptor limit to: %d", rLimit.Cur)
			}
		}
	} else {
		logger.Info("Non-Linux system detected, skipping TCP optimizations.")
	}
}

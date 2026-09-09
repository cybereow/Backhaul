//go:build linux
// +build linux

package network

import "golang.org/x/sys/unix"

// setRecvBuf sets the socket receive buffer to size bytes.
//
// It prefers SO_RCVBUFFORCE, which sets the buffer WITHOUT clamping it to the
// net.core.rmem_max sysctl ceiling - but requires CAP_NET_ADMIN (i.e. running
// as root, as a tunnel typically does). A plain SO_RCVBUF is silently capped at
// rmem_max by the kernel, so on a default host a requested 32MB buffer collapses
// to a few MB, throttling a high-BDP (long-RTT, high-bandwidth) leg no matter
// how large the smux window above it is. When the process lacks the capability,
// SO_RCVBUFFORCE returns EPERM and we fall back to the clamped SO_RCVBUF so the
// buffer is still set as large as the host policy allows.
func setRecvBuf(fd int, size int) error {
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUFFORCE, size); err == nil {
		return nil
	}
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, size)
}

// setSendBuf is the send-side counterpart of setRecvBuf: SO_SNDBUFFORCE bypasses
// the net.core.wmem_max clamp when privileged, with a plain SO_SNDBUF fallback.
func setSendBuf(fd int, size int) error {
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUFFORCE, size); err == nil {
		return nil
	}
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, size)
}

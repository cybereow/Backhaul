//go:build !linux
// +build !linux

package network

import "syscall"

// setRecvBuf / setSendBuf on non-Linux platforms: the SO_*BUFFORCE flags are a
// Linux extension, so fall back to the portable SO_RCVBUF / SO_SNDBUF (which are
// subject to whatever the OS caps them at).
func setRecvBuf(fd int, size int) error {
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, size)
}

func setSendBuf(fd int, size int) error {
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, size)
}

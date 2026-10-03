package network

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// ReusePortControl is a net.ListenConfig.Control hook that sets SO_REUSEADDR and
// SO_REUSEPORT, so a restarted instance can bind an address whose previous
// listener is still closing.
func ReusePortControl(network, address string, s syscall.RawConn) error {
	var controlErr error
	err := s.Control(func(fd uintptr) {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
			controlErr = fmt.Errorf("failed to set SO_REUSEADDR: %v", err)
			return
		}
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
			controlErr = fmt.Errorf("failed to set SO_REUSEPORT: %v", err)
		}
	})
	if err != nil {
		return err
	}
	return controlErr
}

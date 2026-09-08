package network

import (
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockRawConn struct {
	ControlFunc func(f func(fd uintptr)) error
	ReadFunc    func(f func(fd uintptr) (done bool)) error
	WriteFunc   func(f func(fd uintptr) (done bool)) error
}

func (m *MockRawConn) Control(f func(fd uintptr)) error {
	if m.ControlFunc != nil {
		return m.ControlFunc(f)
	}
	return nil
}

func (m *MockRawConn) Read(f func(fd uintptr) (done bool)) error {
	if m.ReadFunc != nil {
		return m.ReadFunc(f)
	}
	return nil
}

func (m *MockRawConn) Write(f func(fd uintptr) (done bool)) error {
	if m.WriteFunc != nil {
		return m.WriteFunc(f)
	}
	return nil
}

func TestReusePortControl_ControlError(t *testing.T) {
	mockErr := errors.New("mock control error")
	conn := &MockRawConn{
		ControlFunc: func(f func(fd uintptr)) error {
			return mockErr
		},
	}

	err := ReusePortControl("tcp", "127.0.0.1:0", conn)
	assert.Equal(t, mockErr, err)
}

func TestReusePortControl_SetsockoptError(t *testing.T) {
	conn := &MockRawConn{
		ControlFunc: func(f func(fd uintptr)) error {
			// Pass an invalid fd to intentionally trigger SetsockoptInt error
			f(^uintptr(0))
			return nil
		},
	}

	err := ReusePortControl("tcp", "127.0.0.1:0", conn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set SO_REUSEADDR")
}

func TestReusePortControl_Success(t *testing.T) {
	conn := &MockRawConn{
		ControlFunc: func(f func(fd uintptr)) error {
			// Create a real socket to test the success path of SetsockoptInt
			fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
			if err != nil {
				return err
			}
			defer syscall.Close(fd)
			f(uintptr(fd))
			return nil
		},
	}

	err := ReusePortControl("tcp", "127.0.0.1:0", conn)
	assert.NoError(t, err)
}

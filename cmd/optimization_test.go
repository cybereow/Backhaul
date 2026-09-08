package cmd

import (
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyTCPTuning(t *testing.T) {
	// Backup originals
	origExecCommandRun := execCommandRun
	origSysGetrlimit := sysGetrlimit
	origSysSetrlimit := sysSetrlimit
	origOsName := osName

	// Restore originals after tests
	defer func() {
		execCommandRun = origExecCommandRun
		sysGetrlimit = origSysGetrlimit
		sysSetrlimit = origSysSetrlimit
		osName = origOsName
	}()

	t.Run("non-linux OS skips optimizations", func(t *testing.T) {
		osName = "windows"
		ranCommands := false

		execCommandRun = func(name string, arg ...string) error {
			ranCommands = true
			return nil
		}

		ApplyTCPTuning()
		assert.False(t, ranCommands, "expected no commands to run on non-linux OS")
	})

	t.Run("linux happy path", func(t *testing.T) {
		osName = "linux"
		var commandsRan []string

		execCommandRun = func(name string, arg ...string) error {
			commandsRan = append(commandsRan, name+" "+strings.Join(arg, " "))
			return nil
		}

		getrlimitCalled := false
		setrlimitCalled := false

		sysGetrlimit = func(resource int, rlim *syscall.Rlimit) error {
			getrlimitCalled = true
			return nil
		}

		sysSetrlimit = func(resource int, rlim *syscall.Rlimit) error {
			setrlimitCalled = true
			return nil
		}

		ApplyTCPTuning()

		assert.Contains(t, commandsRan, "sysctl -w net.core.rmem_max=268435456")
		assert.Contains(t, commandsRan, "sysctl -w net.core.wmem_max=268435456")
		assert.Contains(t, commandsRan, "sysctl -w net.ipv4.tcp_tw_reuse=1")
		assert.True(t, getrlimitCalled)
		assert.True(t, setrlimitCalled)
	})

	t.Run("linux rmem_max and wmem_max fallback", func(t *testing.T) {
		osName = "linux"
		var commandsRan []string

		execCommandRun = func(name string, arg ...string) error {
			cmd := name + " " + strings.Join(arg, " ")
			commandsRan = append(commandsRan, cmd)

			// Fail 256MB and 128MB
			if strings.Contains(cmd, "268435456") || strings.Contains(cmd, "134217728") {
				return fmt.Errorf("failed")
			}
			return nil
		}

		sysGetrlimit = func(resource int, rlim *syscall.Rlimit) error { return nil }
		sysSetrlimit = func(resource int, rlim *syscall.Rlimit) error { return nil }

		ApplyTCPTuning()

		assert.Contains(t, commandsRan, "sysctl -w net.core.rmem_max=268435456")
		assert.Contains(t, commandsRan, "sysctl -w net.core.rmem_max=134217728")
		assert.Contains(t, commandsRan, "sysctl -w net.core.rmem_max=67108864")

		assert.Contains(t, commandsRan, "sysctl -w net.core.wmem_max=268435456")
		assert.Contains(t, commandsRan, "sysctl -w net.core.wmem_max=134217728")
		assert.Contains(t, commandsRan, "sysctl -w net.core.wmem_max=67108864")

		// Ensure it didn't try the smaller ones after succeeding
		assert.NotContains(t, commandsRan, "sysctl -w net.core.rmem_max=33554432")
		assert.NotContains(t, commandsRan, "sysctl -w net.core.wmem_max=33554432")
	})

	t.Run("linux errors in limits", func(t *testing.T) {
		osName = "linux"
		execCommandRun = func(name string, arg ...string) error { return nil }

		t.Run("getrlimit error", func(t *testing.T) {
			sysGetrlimit = func(resource int, rlim *syscall.Rlimit) error {
				return fmt.Errorf("getrlimit failed")
			}
			setrlimitCalled := false
			sysSetrlimit = func(resource int, rlim *syscall.Rlimit) error {
				setrlimitCalled = true
				return nil
			}

			ApplyTCPTuning()
			assert.False(t, setrlimitCalled, "should not set limit if get fails")
		})

		t.Run("setrlimit error", func(t *testing.T) {
			sysGetrlimit = func(resource int, rlim *syscall.Rlimit) error { return nil }
			setrlimitCalled := false
			sysSetrlimit = func(resource int, rlim *syscall.Rlimit) error {
				setrlimitCalled = true
				return fmt.Errorf("setrlimit failed")
			}

			ApplyTCPTuning()
			assert.True(t, setrlimitCalled, "should try to set limit")
		})
	})
}

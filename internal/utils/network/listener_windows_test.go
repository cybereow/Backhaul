//go:build windows

package network

import (
	"testing"
)

func TestListenWithBuffersWindows(t *testing.T) {
	t.Skip("Skipping listener test on Windows as it is not implemented for Windows in this package")
}

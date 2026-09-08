package utils

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBToMb(t *testing.T) {
	assert.Equal(t, uint64(1), bToMb(1048576))
	assert.Equal(t, uint64(0), bToMb(1000000))
	assert.Equal(t, uint64(2), bToMb(2097152))
}

func TestPrintStats(t *testing.T) {
	// Intercept stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run function
	assert.NotPanics(t, func() {
		printStats()
	})

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	assert.NoError(t, err)
	r.Close()

	output := buf.String()

	// Verify output
	assert.Contains(t, output, "Go Runtime Stats")
	assert.Contains(t, output, "================\n")
	assert.Contains(t, output, "Alloc:")
	assert.Contains(t, output, "TotalAlloc:")
	assert.Contains(t, output, "Sys:")
	assert.Contains(t, output, "NumGC:")
	assert.Contains(t, output, "NumGoroutine:")
}

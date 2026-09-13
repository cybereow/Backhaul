package striping

import (
	"io"
	"testing"
)

// BenchmarkConnWrite measures per-write allocation on the striped write path.
// Each peer leg is drained continuously so Write never blocks on backpressure;
// what remains is the cost of framing and queueing chunks. With the chunk
// buffer pool in place this should report ~0 allocs/op for the payload buffers.
func BenchmarkConnWrite(b *testing.B) {
	const chunkSize = 16 * 1024
	legs, peers := pipePair(4)
	for _, p := range peers {
		go io.Copy(io.Discard, p)
	}
	c := New(legs, chunkSize)
	defer c.Close()

	// One full chunk per Write keeps the accounting simple: one buffer borrowed
	// and returned per iteration.
	payload := make([]byte, chunkSize)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

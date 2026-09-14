# Bolt's Journal ⚡

Critical, codebase-specific performance learnings only.

## 2026-09-14 - Striping data plane: read path was the un-pooled mirror of the write path
**Learning:** `internal/utils/striping` is the throughput-critical data plane (one flow split across N legs). The write path already pools chunk buffers (`chunkPool`, commit 187933e), but the read/reassembly path in `readLeg` still did `make([]byte, length)` per inbound chunk — one 16KB allocation per chunk on every striped download. The two sides are symmetric; whenever one gets a buffer-pool optimization, check the other.
**Action:** Pooling the read side is safe *only* because `readBuf` is replaced solely when empty (every assignment site is guarded by `len(readBuf)==0`), so a buffer can be returned to the pool exactly once, right after the final `copy` drains it. Track the backing `*[]byte` on the `chunk` and on a `readBufBase` field; never return a buffer still referenced by `pending` or an in-flight `readBuf`.

## 2026-09-14 - Benchmarking the striping Read path: never feed it with net.Pipe
**Learning:** A `BenchmarkConnRead` fed via `net.Pipe` stalls — the synchronous, unbuffered pipe couples every peer `Write` to the reader's `ReadFull`s and intermittently deadlocks long enough to trip the 20s `stallTimeout`, giving a flaky ~20s/op result.
**Action:** Feed the read benchmark from a non-blocking in-memory `net.Conn` whose `Read` serves an endless framed stream from one reused buffer (see `read_alloc_test.go`'s `seqFrameConn`). Deterministic, fast (~7 GB/s), and measures the Conn's own reassembly/allocation cost, not the pipe's.

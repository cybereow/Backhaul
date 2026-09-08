package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// Speedtest measures raw tunnel throughput over one mux stream. It is a simple
// framed protocol so the *receiver* can time the transfer: at low speeds the
// smux stream's flow-control buffer (megabytes) dwarfs a few seconds of data, so
// a sender-side byte count would be wildly optimistic. The receiver instead
// times from the first data frame to the end marker and reports the goodput.
//
// Wire format on the stream (after the FlowSpeedtest kind byte and the
// mode+seconds header):
//   - data:      4-byte big-endian length (> 0) followed by that many bytes
//   - end marker: 4-byte length of 0, no payload
//   - report (download only): the sink writes back 8 bytes total + 8 bytes
//     elapsed-nanoseconds so the source side can surface the receiver's number.
const (
	// SpeedtestDownload: the server sources, the client sinks (measures how fast
	// the tunnel can push data toward the client).
	SpeedtestDownload byte = 0x00
	// SpeedtestUpload: the client sources, the server sinks.
	SpeedtestUpload byte = 0x01

	speedtestChunk = 64 * 1024
)

// SendFlowSpeedtest writes the speedtest header: the flow-kind byte, the
// direction mode, and how many seconds the source should push for.
func SendFlowSpeedtest(conn net.Conn, mode byte, seconds uint32) error {
	var buf [1 + 1 + 4]byte
	buf[0] = FlowSpeedtest
	buf[1] = mode
	binary.BigEndian.PutUint32(buf[2:6], seconds)
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to send speedtest header: %w", err)
	}
	return nil
}

// ReceiveFlowSpeedtest reads the mode+seconds header. The flow-kind byte is
// consumed separately by ReadFlowKind first.
func ReceiveFlowSpeedtest(conn net.Conn) (mode byte, seconds uint32, err error) {
	var buf [1 + 4]byte
	if _, err = io.ReadFull(conn, buf[:]); err != nil {
		return 0, 0, fmt.Errorf("failed to read speedtest header: %w", err)
	}
	return buf[0], binary.BigEndian.Uint32(buf[1:5]), nil
}

// SpeedtestSource pushes zero-filled frames as fast as the stream drains for the
// given duration, then writes an end marker. Pacing is left to the stream's own
// flow control: Write blocks when the receiver is not keeping up.
func SpeedtestSource(conn net.Conn, dur time.Duration) error {
	buf := make([]byte, 4+speedtestChunk)
	binary.BigEndian.PutUint32(buf[0:4], speedtestChunk) // payload stays zero
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		if _, err := conn.Write(buf); err != nil {
			return fmt.Errorf("speedtest source write: %w", err)
		}
	}
	var end [4]byte // length 0 = end marker
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write(end[:]); err != nil {
		return fmt.Errorf("speedtest source end marker: %w", err)
	}
	return nil
}

// SpeedtestSink reads framed data until the end marker, returning the total
// payload bytes and the elapsed time from the first data frame to the end
// marker - the receiver-measured transfer time used for goodput.
func SpeedtestSink(conn net.Conn) (int64, time.Duration, error) {
	header := make([]byte, 4)
	discard := make([]byte, speedtestChunk)
	var total int64
	var start time.Time
	started := false
	for {
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			return total, 0, err
		}
		if _, err := io.ReadFull(conn, header); err != nil {
			return total, elapsed(start, started), fmt.Errorf("speedtest sink header: %w", err)
		}
		n := binary.BigEndian.Uint32(header)
		if n == 0 { // end marker
			return total, elapsed(start, started), nil
		}
		if n > speedtestChunk {
			return total, elapsed(start, started), fmt.Errorf("speedtest frame length %d exceeds max %d", n, speedtestChunk)
		}
		if !started {
			start = time.Now()
			started = true
		}
		if _, err := io.ReadFull(conn, discard[:n]); err != nil {
			return total, elapsed(start, started), fmt.Errorf("speedtest sink payload: %w", err)
		}
		total += int64(n)
	}
}

func elapsed(start time.Time, started bool) time.Duration {
	if !started {
		return 0
	}
	return time.Since(start)
}

// WriteSpeedtestReport sends the sink's measurement back to the source (used on
// the download path, where the client sinks but the server reports the result).
func WriteSpeedtestReport(conn net.Conn, bytes int64, d time.Duration) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(bytes))
	binary.BigEndian.PutUint64(buf[8:16], uint64(d.Nanoseconds()))
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to write speedtest report: %w", err)
	}
	return nil
}

// ReadSpeedtestReport reads the 16-byte measurement written by
// WriteSpeedtestReport.
func ReadSpeedtestReport(conn net.Conn) (int64, time.Duration, error) {
	var buf [16]byte
	if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return 0, 0, err
	}
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return 0, 0, fmt.Errorf("failed to read speedtest report: %w", err)
	}
	return int64(binary.BigEndian.Uint64(buf[0:8])), time.Duration(binary.BigEndian.Uint64(buf[8:16])), nil
}

// SpeedtestMbps converts a byte count and duration to megabits per second.
func SpeedtestMbps(bytes int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(bytes) * 8 / d.Seconds() / 1e6
}

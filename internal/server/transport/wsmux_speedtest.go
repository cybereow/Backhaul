package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/xtaci/smux"
)

// speedtestResult is the JSON returned by the speedtest endpoint. In "best"
// scope the top-level CDN/*Mbps/*Bytes fields carry the single-session result;
// in "all" scope Total*Mbps carry the aggregate across every CDN run in
// parallel and PerCDN carries the per-connection breakdown.
type speedtestResult struct {
	Direction string  `json:"direction"`
	Seconds   int     `json:"seconds"`
	Scope     string  `json:"scope"`
	CDN       string  `json:"cdn,omitempty"`    // best scope: the pool connection the test ran over
	RTTms     float64 `json:"rtt_ms,omitempty"` // best scope: last measured RTT of that connection
	DownMbps  float64 `json:"down_mbps,omitempty"`
	UpMbps    float64 `json:"up_mbps,omitempty"`
	DownBytes int64   `json:"down_bytes,omitempty"`
	UpBytes   int64   `json:"up_bytes,omitempty"`

	TotalDownMbps float64       `json:"total_down_mbps,omitempty"` // all scope: aggregate across CDNs
	TotalUpMbps   float64       `json:"total_up_mbps,omitempty"`
	PerCDN        []perCDNSpeed `json:"per_cdn,omitempty"`

	Error string `json:"error,omitempty"`
}

// perCDNSpeed is one connection's contribution in an "all"-scope aggregate run.
type perCDNSpeed struct {
	CDN      string  `json:"cdn"`
	RTTms    float64 `json:"rtt_ms,omitempty"`
	DownMbps float64 `json:"down_mbps,omitempty"`
	UpMbps   float64 `json:"up_mbps,omitempty"`
}

// phaseReport is one path's measurement returned by runPhase.
type phaseReport struct {
	bytes   int64
	elapsed time.Duration // receiver-measured data-window elapsed
}

// handleSpeedtestRequest runs a live throughput test over the pool and returns
// the result as JSON. Query params:
//   - dir=down|up|both (default both)
//   - seconds=1..30 (default 10)
//   - scope=best|all (default best): "best" rides one stream on the best
//     (CDN-aware) session - single-flow throughput over the chosen CDN; "all"
//     runs every distinct CDN in parallel and reports the aggregate - the whole
//     connection's combined throughput.
func (s *WsMuxTransport) handleSpeedtestRequest(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	switch dir {
	case "down", "up", "both":
	case "":
		dir = "both"
	default:
		writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "dir must be down, up or both"})
		return
	}

	scope := r.URL.Query().Get("scope")
	switch scope {
	case "best", "all":
	case "":
		scope = "best"
	default:
		writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "scope must be best or all"})
		return
	}

	seconds := 10
	if v := r.URL.Query().Get("seconds"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 30 {
			writeJSON(w, http.StatusBadRequest, speedtestResult{Error: "seconds must be an integer between 1 and 30"})
			return
		}
		seconds = n
	}

	res := s.runSpeedtest(dir, scope, seconds)
	status := http.StatusOK
	if res.Error != "" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, res)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// runSpeedtest measures tunnel throughput. In "best" scope it runs one stream
// on the best (least load, least latency) session; in "all" scope it runs every
// distinct CDN in parallel and aggregates.
func (s *WsMuxTransport) runSpeedtest(dir, scope string, seconds int) speedtestResult {
	res := speedtestResult{Direction: dir, Seconds: seconds, Scope: scope}

	if s.config.MuxVersion < 2 {
		res.Error = "speedtest requires mux_version >= 2 on both ends"
		return res
	}

	s.sessionsMu.Lock()
	avail := make([]*pooledSession, len(s.sessions))
	copy(avail, s.sessions)
	s.sessionsMu.Unlock()
	if len(avail) == 0 {
		res.Error = "no active pool sessions (is the client connected?)"
		return res
	}

	dur := time.Duration(seconds) * time.Second
	if scope == "all" {
		return s.runSpeedtestAll(avail, dir, seconds, dur)
	}

	ps := selectLegs(avail, 1, legScore)[0]
	res.CDN = ps.cdn
	if rtt := ps.rtt.Load(); rtt > 0 {
		res.RTTms = float64(rtt) / float64(time.Millisecond)
	}

	if dir == "down" || dir == "both" {
		bytes, el, err := s.speedtestOnce(ps.session, utils.SpeedtestDownload, seconds, dur)
		if err != nil {
			res.Error = "download: " + err.Error()
			return res
		}
		res.DownBytes = bytes
		res.DownMbps = utils.SpeedtestMbps(bytes, el)
	}
	if dir == "up" || dir == "both" {
		bytes, el, err := s.speedtestOnce(ps.session, utils.SpeedtestUpload, seconds, dur)
		if err != nil {
			res.Error = "upload: " + err.Error()
			return res
		}
		res.UpBytes = bytes
		res.UpMbps = utils.SpeedtestMbps(bytes, el)
	}
	return res
}

// speedtestOnce runs one direction of the test on a fresh stream and returns the
// receiver-measured bytes and elapsed time. On download the server sources the
// data and the client sinks and reports back; on upload the client sources and
// the server sinks and measures directly.
func (s *WsMuxTransport) speedtestOnce(session *smux.Session, mode byte, seconds int, dur time.Duration) (int64, time.Duration, error) {
	stream, err := session.OpenStream()
	if err != nil {
		return 0, 0, err
	}
	defer stream.Close()

	if err := utils.SendFlowSpeedtest(stream, mode, uint32(seconds)); err != nil {
		return 0, 0, err
	}
	if mode == utils.SpeedtestDownload {
		if err := utils.SpeedtestSource(stream, dur); err != nil {
			return 0, 0, err
		}
		return utils.ReadSpeedtestReport(stream)
	}
	return utils.SpeedtestSink(stream)
}

// validatePhaseReports checks receiver-reported measurements for sanity.
// Returns an error for negative bytes, invalid elapsed (positive bytes but
// non-positive elapsed, which would produce Inf/NaN Mbps), or byte-sum overflow.
func validatePhaseReports(reports []phaseReport) error {
	var sum int64
	for i, r := range reports {
		if r.bytes < 0 {
			return fmt.Errorf("path %d reported negative bytes %d", i, r.bytes)
		}
		if r.bytes > 0 && r.elapsed <= 0 {
			return fmt.Errorf("path %d reported %d bytes but non-positive elapsed %v", i, r.bytes, r.elapsed)
		}
		// Overflow-safe sum: check before adding.
		if r.bytes > 0 && sum > math.MaxInt64-r.bytes {
			return errors.New("byte sum overflow across paths")
		}
		sum += r.bytes
	}
	return nil
}

// calcPhaseRates fills per-path Mbps and returns the aggregate Mbps using the
// provided phase wall duration for the denominator.
// Per-path rate = bytes / receiver-elapsed (data-window goodput).
// Aggregate rate = sum(bytes) / phaseWall (effective whole-phase throughput,
// which includes setup and drain overhead and is intentionally different from
// per-path data-window rate).
//
// ponytail: pure arithmetic; exported for testability without network calls.
func calcPhaseRates(reports []phaseReport, phaseWall time.Duration, perMbps []float64) float64 {
	var sum int64
	for i, r := range reports {
		perMbps[i] = utils.SpeedtestMbps(r.bytes, r.elapsed)
		sum += r.bytes
	}
	return utils.SpeedtestMbps(sum, phaseWall)
}

// runSpeedtestAll measures the whole connection's throughput: it runs the test
// on the best session of every distinct CDN in parallel and sums the
// receiver-measured bytes over the test window. Because the runs are concurrent
// they contend for any shared bottleneck (e.g. one origin uplink behind several
// CDNs), so the aggregate honestly reflects what the pool can move at once
// rather than an inflated sum of isolated runs.
//
// Per-path rates are computed from receiver-measured elapsed (data-window
// goodput). Aggregate rates are computed from a local monotonic phase wall
// clock that spans from just before launching concurrent operations to after
// all return, including setup and report/drain overhead. These are intentionally
// different measurements and should be expected to differ.
func (s *WsMuxTransport) runSpeedtestAll(avail []*pooledSession, dir string, seconds int, dur time.Duration) speedtestResult {
	res := speedtestResult{Direction: dir, Seconds: seconds, Scope: "all"}
	targets := bestPerCDN(avail, legScore)

	per := make([]perCDNSpeed, len(targets))
	for i, ps := range targets {
		per[i].CDN = ps.cdn
		if rtt := ps.rtt.Load(); rtt > 0 {
			per[i].RTTms = float64(rtt) / float64(time.Millisecond)
		}
	}

	// runPhase runs one direction on every target concurrently and returns each
	// target's receiver-measured byte count and elapsed duration (index-aligned
	// with targets). The phase wall time is also returned.
	runPhase := func(mode byte) ([]phaseReport, time.Duration, error) {
		reports := make([]phaseReport, len(targets))
		errs := make([]error, len(targets))
		var wg sync.WaitGroup
		phaseStart := time.Now() // local monotonic clock: spans all concurrent ops
		for i, ps := range targets {
			wg.Add(1)
			go func(i int, ps *pooledSession) {
				defer wg.Done()
				b, el, err := s.speedtestOnce(ps.session, mode, seconds, dur)
				reports[i] = phaseReport{bytes: b, elapsed: el}
				errs[i] = err
			}(i, ps)
		}
		wg.Wait()
		phaseWall := time.Since(phaseStart) // includes launch overhead and drain time
		for i, err := range errs {
			if err != nil {
				return reports, phaseWall, fmt.Errorf("cdn %s: %w", targets[i].cdn, err)
			}
		}
		if err := validatePhaseReports(reports); err != nil {
			return reports, phaseWall, err
		}
		return reports, phaseWall, nil
	}

	// Upload and download have separate phase clocks. Requested seconds is the
	// workload duration, never the rate denominator.
	if dir == "down" || dir == "both" {
		reports, phaseWall, err := runPhase(utils.SpeedtestDownload)
		if err != nil {
			res.Error = "download: " + err.Error()
			return res
		}
		perMbps := make([]float64, len(targets))
		res.TotalDownMbps = calcPhaseRates(reports, phaseWall, perMbps)
		for i := range targets {
			per[i].DownMbps = perMbps[i]
		}
	}
	if dir == "up" || dir == "both" {
		reports, phaseWall, err := runPhase(utils.SpeedtestUpload)
		if err != nil {
			res.Error = "upload: " + err.Error()
			return res
		}
		perMbps := make([]float64, len(targets))
		res.TotalUpMbps = calcPhaseRates(reports, phaseWall, perMbps)
		for i := range targets {
			per[i].UpMbps = perMbps[i]
		}
	}

	res.PerCDN = per
	return res
}

package transport

import (
	"encoding/json"
	"fmt"
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

// runSpeedtestAll measures the whole connection's throughput: it runs the test
// on the best session of every distinct CDN in parallel and sums the
// receiver-measured bytes over the test window. Because the runs are concurrent
// they contend for any shared bottleneck (e.g. one origin uplink behind several
// CDNs), so the aggregate honestly reflects what the pool can move at once
// rather than an inflated sum of isolated runs.
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
	// target's receiver-measured byte count (index-aligned with targets).
	runPhase := func(mode byte) ([]int64, error) {
		bytes := make([]int64, len(targets))
		errs := make([]error, len(targets))
		var wg sync.WaitGroup
		for i, ps := range targets {
			wg.Add(1)
			go func(i int, ps *pooledSession) {
				defer wg.Done()
				b, _, err := s.speedtestOnce(ps.session, mode, seconds, dur)
				bytes[i] = b
				errs[i] = err
			}(i, ps)
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				return bytes, fmt.Errorf("cdn %s: %w", targets[i].cdn, err)
			}
		}
		return bytes, nil
	}

	// Aggregate throughput is the summed bytes over the common test window, so
	// concurrent runs that share a bottleneck don't add up beyond it.
	if dir == "down" || dir == "both" {
		b, err := runPhase(utils.SpeedtestDownload)
		if err != nil {
			res.Error = "download: " + err.Error()
			return res
		}
		var sum int64
		for i, x := range b {
			per[i].DownMbps = utils.SpeedtestMbps(x, dur)
			sum += x
		}
		res.TotalDownMbps = utils.SpeedtestMbps(sum, dur)
	}
	if dir == "up" || dir == "both" {
		b, err := runPhase(utils.SpeedtestUpload)
		if err != nil {
			res.Error = "upload: " + err.Error()
			return res
		}
		var sum int64
		for i, x := range b {
			per[i].UpMbps = utils.SpeedtestMbps(x, dur)
			sum += x
		}
		res.TotalUpMbps = utils.SpeedtestMbps(sum, dur)
	}

	res.PerCDN = per
	return res
}

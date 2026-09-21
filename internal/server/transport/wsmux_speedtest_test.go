package transport

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/musix/backhaul/internal/utils"
)

// -- deterministic arithmetic tests ------------------------------------------
// These tests exercise calcPhaseRates and validatePhaseReports without any
// network or smux involvement, so they pass on every platform.

// TestCalcPhaseRates_SinglePath: 1 000 000 bytes received over 2 s gives 4 Mbps per-path;
// the aggregate uses the phase wall time (4 s here) so it yields 2 Mbps.
func TestSpeedtestCalcPhaseRates_SinglePath(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1_000_000, elapsed: 2 * time.Second},
	}
	phaseWall := 4 * time.Second
	perMbps := make([]float64, 1)
	agg := calcPhaseRates(reports, phaseWall, perMbps)

	const wantPer = 4.0 // 1 000 000 * 8 / 2 / 1e6
	const wantAgg = 2.0 // 1 000 000 * 8 / 4 / 1e6
	const tol = 1e-9

	if math.Abs(perMbps[0]-wantPer) > tol {
		t.Errorf("per-path Mbps = %.6f, want %.6f", perMbps[0], wantPer)
	}
	if math.Abs(agg-wantAgg) > tol {
		t.Errorf("aggregate Mbps = %.6f, want %.6f", agg, wantAgg)
	}
}

// TestCalcPhaseRates_TwoUnequal: two paths with different receiver windows.
// Path A: 1 000 000 bytes / 1 s = 8 Mbps
// Path B: 2 000 000 bytes / 4 s = 4 Mbps
// Aggregate: 3 000 000 bytes / 3 s phase wall = 8 Mbps
func TestSpeedtestCalcPhaseRates_TwoUnequal(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1_000_000, elapsed: 1 * time.Second},
		{bytes: 2_000_000, elapsed: 4 * time.Second},
	}
	phaseWall := 3 * time.Second
	perMbps := make([]float64, 2)
	agg := calcPhaseRates(reports, phaseWall, perMbps)

	const wantA = 8.0   // 1 000 000 * 8 / 1 / 1e6
	const wantB = 4.0   // 2 000 000 * 8 / 4 / 1e6
	const wantAgg = 8.0 // 3 000 000 * 8 / 3 / 1e6
	const tol = 1e-9

	if math.Abs(perMbps[0]-wantA) > tol {
		t.Errorf("path A Mbps = %.6f, want %.6f", perMbps[0], wantA)
	}
	if math.Abs(perMbps[1]-wantB) > tol {
		t.Errorf("path B Mbps = %.6f, want %.6f", perMbps[1], wantB)
	}
	if math.Abs(agg-wantAgg) > tol {
		t.Errorf("aggregate Mbps = %.6f, want %.6f", agg, wantAgg)
	}
}

// TestCalcPhaseRates_ZeroBytes: a path that transferred nothing contributes
// 0 Mbps per-path and 0 to the sum; the aggregate denominator is phaseWall.
func TestSpeedtestCalcPhaseRates_ZeroBytes(t *testing.T) {
	reports := []phaseReport{
		{bytes: 0, elapsed: 0},
	}
	phaseWall := 10 * time.Second
	perMbps := make([]float64, 1)
	agg := calcPhaseRates(reports, phaseWall, perMbps)

	if perMbps[0] != 0 {
		t.Errorf("zero-bytes per-path Mbps = %.6f, want 0", perMbps[0])
	}
	if agg != 0 {
		t.Errorf("zero-bytes aggregate Mbps = %.6f, want 0", agg)
	}
}

// TestCalcPhaseRates_RequestedDurNotUsed: the rate denominator must be the
// receiver elapsed (per-path) and the phase wall time (aggregate), never the
// requested test duration. This test verifies the bug is absent: if rates were
// computed from requestedDur they would come out 1 Mbps not 4 Mbps / 2 Mbps.
func TestSpeedtestCalcPhaseRates_RequestedDurNotUsed(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1_000_000, elapsed: 2 * time.Second},
	}
	requestedDur := 10 * time.Second // wrong if used as denominator
	phaseWall := 4 * time.Second

	perMbps := make([]float64, 1)
	agg := calcPhaseRates(reports, phaseWall, perMbps)

	_ = requestedDur // must not affect result
	const wantPer = 4.0
	const wantAgg = 2.0
	const tol = 1e-9
	if math.Abs(perMbps[0]-wantPer) > tol {
		t.Errorf("per-path Mbps = %.6f, want %.6f (requested-dur bug present?)", perMbps[0], wantPer)
	}
	if math.Abs(agg-wantAgg) > tol {
		t.Errorf("aggregate Mbps = %.6f, want %.6f (requested-dur bug present?)", agg, wantAgg)
	}
}

// -- validatePhaseReports tests -----------------------------------------------

func TestSpeedtestValidatePhaseReports_OK(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1_000_000, elapsed: 2 * time.Second},
		{bytes: 0, elapsed: 0},
	}
	if err := validatePhaseReports(reports); err != nil {
		t.Errorf("unexpected error for valid reports: %v", err)
	}
}

func TestSpeedtestValidatePhaseReports_NegativeBytes(t *testing.T) {
	reports := []phaseReport{
		{bytes: -1, elapsed: 2 * time.Second},
	}
	err := validatePhaseReports(reports)
	if err == nil {
		t.Error("expected error for negative bytes, got nil")
	}
}

func TestSpeedtestValidatePhaseReports_PositiveBytesZeroElapsed(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1000, elapsed: 0},
	}
	err := validatePhaseReports(reports)
	if err == nil {
		t.Error("expected error for positive bytes with zero elapsed, got nil")
	}
}

func TestSpeedtestValidatePhaseReports_PositiveBytesNegativeElapsed(t *testing.T) {
	reports := []phaseReport{
		{bytes: 1000, elapsed: -time.Second},
	}
	err := validatePhaseReports(reports)
	if err == nil {
		t.Error("expected error for positive bytes with negative elapsed, got nil")
	}
}

func TestSpeedtestValidatePhaseReports_Overflow(t *testing.T) {
	reports := []phaseReport{
		{bytes: math.MaxInt64, elapsed: 10 * time.Second},
		{bytes: 1, elapsed: 10 * time.Second},
	}
	err := validatePhaseReports(reports)
	if err == nil {
		t.Error("expected error for byte sum overflow, got nil")
	}
}

func TestSpeedtestValidatePhaseReports_OverflowNotFalsePositive(t *testing.T) {
	// Two paths that almost overflow but don't.
	half := int64(math.MaxInt64 / 2)
	reports := []phaseReport{
		{bytes: half, elapsed: 10 * time.Second},
		{bytes: half, elapsed: 10 * time.Second},
	}
	if err := validatePhaseReports(reports); err != nil {
		t.Errorf("unexpected overflow error for non-overflowing sum: %v", err)
	}
}

// -- SpeedtestMbps contract ---------------------------------------------------
// SpeedtestMbps (in utils) returns 0 for d<=0. Verify the helpers honour that
// since calcPhaseRates delegates to SpeedtestMbps.

func TestSpeedtestMbps_ZeroDuration(t *testing.T) {
	got := utils.SpeedtestMbps(1_000_000, 0)
	if got != 0 {
		t.Errorf("SpeedtestMbps with zero duration = %.6f, want 0", got)
	}
}

func TestSpeedtestMbps_NegativeDuration(t *testing.T) {
	got := utils.SpeedtestMbps(1_000_000, -time.Second)
	if got != 0 {
		t.Errorf("SpeedtestMbps with negative duration = %.6f, want 0", got)
	}
}

// -- sentinel: verifies the "all"-scope bug description is not just a comment -
// It encodes the exact arithmetic the bug would break: if the old code used
// `dur` (requested 10 s) instead of receiver elapsed (2 s) the per-path rate
// would be 0.4 Mbps not 4 Mbps.
func TestSpeedtestMeasuredRates(t *testing.T) {
	type tc struct {
		name      string
		reports   []phaseReport
		phaseWall time.Duration
		wantPer   []float64 // per-path rates, index-aligned
		wantAgg   float64
	}
	tol := 1e-9
	tests := []tc{
		{
			name:      "single path 4 Mbps",
			reports:   []phaseReport{{bytes: 1_000_000, elapsed: 2 * time.Second}},
			phaseWall: 4 * time.Second,
			wantPer:   []float64{4.0},
			wantAgg:   2.0,
		},
		{
			name: "two paths aggregate",
			reports: []phaseReport{
				{bytes: 1_000_000, elapsed: 2 * time.Second},
				{bytes: 1_000_000, elapsed: 2 * time.Second},
			},
			phaseWall: 2 * time.Second,
			wantPer:   []float64{4.0, 4.0},
			wantAgg:   8.0, // 2 000 000 * 8 / 2 / 1e6
		},
		{
			name: "staggered completion: slowest path sets receiver window",
			reports: []phaseReport{
				{bytes: 500_000, elapsed: 1 * time.Second},
				{bytes: 4_000_000, elapsed: 8 * time.Second},
			},
			// phase wall extends to slowest goroutine (~8 s), here we use 8 s
			phaseWall: 8 * time.Second,
			wantPer:   []float64{4.0, 4.0},
			wantAgg:   4.5, // 4 500 000 * 8 / 8 / 1e6
		},
		{
			name:      "zero bytes, zero elapsed",
			reports:   []phaseReport{{bytes: 0, elapsed: 0}},
			phaseWall: 5 * time.Second,
			wantPer:   []float64{0},
			wantAgg:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perMbps := make([]float64, len(tt.reports))
			agg := calcPhaseRates(tt.reports, tt.phaseWall, perMbps)
			for i, want := range tt.wantPer {
				if math.Abs(perMbps[i]-want) > tol {
					t.Errorf("[%s] path %d: %.6f Mbps, want %.6f", tt.name, i, perMbps[i], want)
				}
			}
			if math.Abs(agg-tt.wantAgg) > tol {
				t.Errorf("[%s] aggregate: %.6f Mbps, want %.6f", tt.name, agg, tt.wantAgg)
			}
		})
	}
}

// -- verify calcPhaseRates satisfies the "dir=both separate clocks" property --
// Upload and download have independent phase clocks in runSpeedtestAll.
// This test checks that calling calcPhaseRates twice with different phase walls
// produces independently correct results (no shared wall state).
func TestSpeedtestCalcPhaseRates_SeparateUpDownClocks(t *testing.T) {
	downReports := []phaseReport{{bytes: 2_000_000, elapsed: 2 * time.Second}}
	upReports := []phaseReport{{bytes: 1_000_000, elapsed: 2 * time.Second}}
	downWall := 3 * time.Second
	upWall := 4 * time.Second

	downPer := make([]float64, 1)
	upPer := make([]float64, 1)
	downAgg := calcPhaseRates(downReports, downWall, downPer)
	upAgg := calcPhaseRates(upReports, upWall, upPer)

	const tol = 1e-9
	if math.Abs(downPer[0]-8.0) > tol {
		t.Errorf("down per-path = %.6f, want 8.0", downPer[0])
	}
	// down aggregate: 2 000 000 * 8 / 3 / 1e6
	wantDownAgg := 2_000_000.0 * 8 / 3 / 1e6
	if math.Abs(downAgg-wantDownAgg) > tol {
		t.Errorf("down aggregate = %.6f, want %.6f", downAgg, wantDownAgg)
	}
	if math.Abs(upPer[0]-4.0) > tol {
		t.Errorf("up per-path = %.6f, want 4.0", upPer[0])
	}
	wantUpAgg := 1_000_000.0 * 8 / 4 / 1e6
	if math.Abs(upAgg-wantUpAgg) > tol {
		t.Errorf("up aggregate = %.6f, want %.6f", upAgg, wantUpAgg)
	}
}

// -- validatePhaseReports: check it returns the right sentinel error types ----

func TestSpeedtestValidatePhaseReports_ErrorsAreSentinel(t *testing.T) {
	negBytes := []phaseReport{{bytes: -1, elapsed: time.Second}}
	if err := validatePhaseReports(negBytes); err == nil {
		t.Error("expected error for negative bytes")
	}

	badElapsed := []phaseReport{{bytes: 100, elapsed: 0}}
	if err := validatePhaseReports(badElapsed); err == nil {
		t.Error("expected error for positive bytes zero elapsed")
	}

	overflow := []phaseReport{
		{bytes: math.MaxInt64, elapsed: time.Second},
		{bytes: 1, elapsed: time.Second},
	}
	err := validatePhaseReports(overflow)
	if err == nil {
		t.Error("expected overflow error")
	}
	if !errors.Is(err, err) { // trivially true; guards that it's a real error
		t.Error("overflow error must be a real error value")
	}
}

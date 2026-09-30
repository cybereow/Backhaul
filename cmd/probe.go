package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"

	"github.com/musix/backhaul/config"
	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/utils"
)

// allRRTypes is the default candidate set when record_types is empty. Order is
// the probe order; NULL is high because it is often the only survivor under the
// harshest filtering (design §3).
var allRRTypes = []string{"TXT", "NULL", "A", "AAAA", "CNAME", "MX", "SRV", "PTR"}

var rrTypeByName = map[string]uint16{
	"TXT": dns.TypeTXT, "NULL": dns.TypeNULL, "A": dns.TypeA, "AAAA": dns.TypeAAAA,
	"CNAME": dns.TypeCNAME, "MX": dns.TypeMX, "SRV": dns.TypeSRV, "PTR": dns.TypePTR,
}

// RunProbe is the entry point for the -probe subcommand. It runs the DNS
// reachability/capacity prober (outside) or the authoritative responder
// (inside), depending on [dns_probe] role. It never touches the tunnel run path.
func RunProbe(configPath string, ctx context.Context) {
	var cfg config.Config
	if _, err := toml.DecodeFile(configPath, &cfg); err != nil {
		logger.Fatalf("failed to load probe configuration: %v", err)
	}
	p := &cfg.DNSProbe

	if strings.TrimSpace(p.Domain) == "" {
		logger.Fatalf("dns_probe 'domain' is required (the tunnel domain both sides own, e.g. \"t.example.com\")")
	}
	if strings.TrimSpace(p.Key) == "" {
		logger.Fatalf("dns_probe 'key' is required (shared secret used only to derive a per-query MAC; must match on both sides)")
	}

	log := utils.NewLogger("info")

	switch strings.ToLower(strings.TrimSpace(p.Role)) {
	case "", "prober":
		runProber(ctx, p, log)
	case "soak":
		runSoak(ctx, p, log)
	case "responder":
		if strings.TrimSpace(p.ResponderListen) == "" {
			logger.Fatalf("dns_probe 'responder_listen' is required for the responder role (e.g. \"0.0.0.0:53\")")
		}
		resp := dnsx.NewResponder(p.Domain, p.Key, log)
		if err := resp.Serve(ctx, p.ResponderListen); err != nil {
			logger.Fatalf("responder failed: %v", err)
		}
	default:
		logger.Fatalf("dns_probe 'role' must be \"prober\" or \"responder\" (it is %q)", p.Role)
	}
}

func runProber(ctx context.Context, p *config.ProbeConfig, log *logrus.Logger) {
	resolvers := buildResolvers(p.Resolvers)
	if len(resolvers) == 0 {
		logger.Fatalf("dns_probe 'resolvers' is required for the prober role: at least one recursive resolver (\"ip\" or \"ip:port\")")
	}

	rrTypes, err := parseRRTypes(p.RecordTypes)
	if err != nil {
		logger.Fatalf("%v", err)
	}

	params := dnsx.ProberParams{
		Domain:      p.Domain,
		Key:         p.Key,
		Resolvers:   resolvers,
		RRTypes:     rrTypes,
		QueryBudget: orDefault(p.QueryBudget, 40),
		RespSizes:   respSizes(p.ResponseSizes, p.ResponseBudget),
		Repeat:      orDefault(p.Repeat, 1),
		EDNSModes:   ednsModes(p.EDNS),
		UseTCP:      p.UseTCP,
		Timeout:     time.Duration(orDefault(p.TimeoutMS, 4000)) * time.Millisecond,
		Pacing:      time.Duration(orDefault(p.PacingMS, 50)) * time.Millisecond,
		Concurrency: orDefault(p.Concurrency, 4),
	}

	log.Infof("probing domain %q via %d resolver(s), %d record type(s)", p.Domain, len(resolvers), len(rrTypes))
	results := dnsx.NewProber(params, log).Run(ctx)

	printReport(results)
	if p.ReportJSON != "" {
		if err := writeJSON(p.ReportJSON, results); err != nil {
			log.Errorf("failed to write report JSON to %s: %v", p.ReportJSON, err)
		} else {
			log.Infof("wrote report to %s", p.ReportJSON)
		}
	}
}

// runSoak runs the sustained bidirectional path test (role="soak"). Streams are
// configured resolvers × record_types × response_sizes, transport forced TCP on
// the outside hop (the reliable carrier per Phase-1.5). Resolver-only: it dials
// only the configured recursive resolvers, never the authoritative server.
func runSoak(ctx context.Context, p *config.ProbeConfig, log *logrus.Logger) {
	var resolvers []string
	seen := map[string]bool{}
	for _, r := range p.Resolvers {
		if a := ensurePort(r); a != "" && !seen[a] {
			seen[a] = true
			resolvers = append(resolvers, a)
		}
	}
	if len(resolvers) == 0 {
		logger.Fatalf("dns_probe 'resolvers' is required for the soak role")
	}
	rrTypes, err := parseRRTypes(p.RecordTypes)
	if err != nil {
		logger.Fatalf("%v", err)
	}
	sizes := respSizes(p.ResponseSizes, p.ResponseBudget)

	var streams []dnsx.SoakStream
	for _, res := range resolvers {
		for _, t := range rrTypes {
			for _, sz := range sizes {
				streams = append(streams, dnsx.SoakStream{Resolver: res, RRType: t, Transport: "tcp", RespSize: sz})
			}
		}
	}

	dur := time.Duration(orDefault(p.SoakDurationMin, 20)) * time.Minute
	params := dnsx.SoakParams{
		Domain:      p.Domain,
		Key:         p.Key,
		QueryBudget: orDefault(p.QueryBudget, 40),
		EDNS:        strings.ToLower(strings.TrimSpace(p.EDNS)) != "off", // default on
		Timeout:     time.Duration(orDefault(p.TimeoutMS, 5000)) * time.Millisecond,
		Duration:    dur,
		Streams:     streams,
		MaxInflight: orDefault(p.SoakMaxInflight, 4),
		NoRamp:      p.SoakNoRamp,
		SampleEvery: 30 * time.Second,
	}

	log.Infof("soak: %d stream(s) for %s (max inflight %d/stream)", len(streams), dur, params.MaxInflight)
	results := dnsx.RunSoak(ctx, params, log)

	printSoak(results)
	if p.ReportJSON != "" {
		if err := writeJSON(p.ReportJSON, results); err != nil {
			log.Errorf("failed to write soak report to %s: %v", p.ReportJSON, err)
		} else {
			log.Infof("wrote soak report to %s", p.ReportJSON)
		}
	}
}

func printSoak(results []dnsx.SoakResult) {
	fmt.Println()
	fmt.Printf("%-14s %-5s %-4s %5s %7s %9s %9s %6s %6s %7s %6s %5s\n",
		"RESOLVER", "TYPE", "NET", "SZ", "OK/N", "UP B/s", "DOWN B/s", "p50", "p90", "STALLms", "CFAIL", "REC")
	for _, r := range results {
		rec := "y"
		if !r.Recovered {
			rec = "n"
		}
		fmt.Printf("%-14s %-5s %-4s %5d %d/%-5d %9.0f %9.0f %6d %6d %7d %6d %5s\n",
			stripHostPort(r.Resolver), r.RRType, r.Transport, r.RespSize,
			r.Successes, r.Exchanges, r.UpGoodputBps, r.DownGoodputBps,
			r.RTTp50Ms, r.RTTp90Ms, r.LongestStallMs, r.MaxConsecFails, rec)
	}
	fmt.Println()
}

func stripHostPort(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// buildResolvers normalizes the configured list (adding :53) and appends the
// system resolvers, deduped. The prober only ever sends here — never to the
// authoritative server directly (resolver-only path, §4).
func buildResolvers(configured []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(addr string) {
		addr = ensurePort(addr)
		if addr != "" && !seen[addr] {
			seen[addr] = true
			out = append(out, addr)
		}
	}
	for _, r := range configured {
		add(r)
	}
	if sys, err := dns.ClientConfigFromFile("/etc/resolv.conf"); err == nil {
		for _, s := range sys.Servers {
			add(net.JoinHostPort(s, sys.Port))
		}
	}
	return out
}

func ensurePort(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "53")
	}
	return addr
}

func parseRRTypes(names []string) ([]uint16, error) {
	if len(names) == 0 {
		names = allRRTypes
	}
	var out []uint16
	for _, n := range names {
		t, ok := rrTypeByName[strings.ToUpper(strings.TrimSpace(n))]
		if !ok {
			return nil, fmt.Errorf("dns_probe 'record_types': unsupported type %q (supported: %s)", n, strings.Join(allRRTypes, ", "))
		}
		out = append(out, t)
	}
	return out, nil
}

func ednsModes(mode string) []bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "off":
		return []bool{false}
	case "on":
		return []bool{true}
	default: // "both" or empty
		return []bool{false, true}
	}
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// respSizes picks the response-size sweep: explicit list wins, else a single
// size from response_budget (or the 120 B default).
func respSizes(sizes []int, budget int) []int {
	var out []int
	for _, s := range sizes {
		if s > 0 {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		out = []int{orDefault(budget, 120)}
	}
	return out
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// printReport writes a compact human-readable summary. The winning profiles
// (Stage ok, high success rate, full bytes both ways) are what later phases can
// build on. One row per profile, aggregated over its repeats.
func printReport(results []dnsx.ProfileResult) {
	sort.SliceStable(results, func(i, j int) bool {
		if si, sj := stageRank(results[i].Stage), stageRank(results[j].Stage); si != sj {
			return si < sj
		}
		return results[i].SuccessRate > results[j].SuccessRate
	})
	fmt.Println()
	fmt.Printf("%-18s %-5s %-4s %-5s %-6s %8s %8s %5s %6s %6s  %s\n",
		"RESOLVER", "TYPE", "NET", "EDNS", "OK/N", "Q-THRU", "R-REQ", "R-THRU", "IN", "RTTp50", "NOTE")
	for _, r := range results {
		fmt.Printf("%-18s %-5s %-4s %-5t %d/%-4d %8d %8d %5d %6s %6d  %s\n",
			r.Resolver, r.RRType, r.Transport, r.EDNS,
			r.Successes, r.Attempts,
			r.QBytesThrough, r.RespBudget, r.RespBytesThrough,
			orDash(r.InsideTransport), r.RTTp50Ms, r.Err)
	}
	fmt.Println()

	var ok int
	for _, r := range results {
		if r.Stage == dnsx.StageOK && r.Successes == r.Attempts && r.Err == "" &&
			r.QBytesThrough >= r.QueryBudget && r.RespBytesThrough >= r.RespBudget {
			ok++
		}
	}
	fmt.Printf("%d/%d profiles completed a clean round-trip on every attempt.\n", ok, len(results))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func stageRank(s dnsx.Stage) int {
	switch s {
	case dnsx.StageOK:
		return 0
	case dnsx.StageBadPeer:
		return 1
	case dnsx.StageResolver:
		return 2
	default:
		return 3
	}
}

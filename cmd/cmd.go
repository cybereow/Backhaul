package cmd

import (
	"context"
	"fmt"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"

	"github.com/musix/backhaul/internal/server"
	dnsx "github.com/musix/backhaul/internal/transport/dns"
	"github.com/musix/backhaul/internal/utils"

	"github.com/BurntSushi/toml"
)

var (
	logger = utils.NewLogger("info")
)

// detectConfigType decides whether a config is a server or a client (or
// neither). A client is recognized by either the single remote_addr or the
// multi-endpoint remote_addrs list, so a config that only sets remote_addrs is
// still valid.
func detectConfigType(cfg *config.Config) string {
	switch {
	case cfg.Server.BindAddr != "":
		return "server"
	case cfg.Client.RemoteAddr != "" || len(cfg.Client.RemoteAddrs) > 0:
		return "client"
	// the DNS transports have no bind_addr/remote_addr: the server is identified by the
	// domain it is authoritative for, the client by the domain it tunnels through
	// (dns_resolvers is optional: empty means auto-discover).
	case cfg.Server.Transport.IsDNS() && cfg.Server.DNSDomain != "":
		return "server"
	case cfg.Client.Transport.IsDNS() && cfg.Client.DNSDomain != "":
		return "client"
	default:
		return ""
	}
}

// maxStripeTotal is the largest number of legs one striped flow may have. The
// stripe header carries index, total and parity as single bytes (see
// utils.SendStripeHeader), so the wire ceiling is 255 - not the 256 total
// shards the Reed-Solomon library itself allows. A wider configuration cannot
// work: the sender's uint8 cast truncates the count (256 becomes 0) and the
// receiver rejects the mismatching header, so every flow fails. Reject it at
// startup, where the operator can see why.
const maxStripeTotal = 255

// validateStripeConfig checks mux_stripe and mux_stripe_parity for one role.
// Call it after applyDefaults, which has already normalized the legacy omitted
// and negative values; the checks below are the boundaries the wire imposes.
// Both roles share the rules because the two ends must be configured alike.
func validateStripeConfig(role string, factor, parity int) error {
	if factor < 1 || factor > maxStripeTotal {
		return fmt.Errorf("%s 'mux_stripe' must be between 1 and %d (it is %d): the stripe header carries the leg count in a single byte", role, maxStripeTotal, factor)
	}
	if parity < 0 {
		return fmt.Errorf("%s 'mux_stripe_parity' must not be negative (it is %d)", role, parity)
	}
	if parity == 0 {
		return nil
	}
	if factor < 2 {
		return fmt.Errorf("%s 'mux_stripe_parity' requires 'mux_stripe' >= 2 (it adds parity legs on top of striping)", role)
	}
	// Compare parity against the remaining headroom rather than summing it with
	// factor, so an enormous configured value cannot overflow the addition
	// before it is checked.
	if parity > maxStripeTotal-factor {
		return fmt.Errorf("%s 'mux_stripe' + 'mux_stripe_parity' must be <= %d (they are %d + %d): Reed-Solomon allows 256 total shards, but the stripe header carries the count in a single byte", role, maxStripeTotal, factor, parity)
	}
	return nil
}

// validateTransport rejects a transport this build does not carry. The tcp,
// tcpmux and udp transports were removed; naming them is a configuration error
// rather than a silent fallback.
func validateTransport(cfg *config.Config, configType string) error {
	t := cfg.Server.Transport
	if configType == "client" {
		t = cfg.Client.Transport
	}
	switch t {
	case config.WS, config.WSS, config.WSMUX, config.WSSMUX, config.DNS, config.DNSMUX:
		return nil
	}
	return fmt.Errorf("%s 'transport' %q is not supported: use one of ws, wss, wsmux, wssmux, dns, dnsmux", configType, t)
}

// validateDNSMux checks the keys the DNS transports cannot run without. It
// does nothing for any other transport.
func validateDNSMux(cfg *config.Config, configType string) error {
	switch {
	case configType == "server" && cfg.Server.Transport.IsDNS():
		if cfg.Server.DNSDomain == "" {
			return fmt.Errorf("server 'dns_domain' is required for the dns/dnsmux transports")
		}
	case configType == "client" && cfg.Client.Transport.IsDNS():
		if cfg.Client.DNSDomain == "" {
			return fmt.Errorf("client 'dns_domain' is required for the dns/dnsmux transports")
		}
		if err := dnsx.ValidateRecordTypes(cfg.Client.DNSDomain, cfg.Client.DNSRecordTypes); err != nil {
			return fmt.Errorf("client 'dns_record_types': %w", err)
		}
	}
	return nil
}

// validateHalfClose rejects mux_half_close on anything that cannot carry it: it
// is a server-only key, meaningful only on wsmux/wssmux, and its FlowPlainHC
// flow kind is only read on mux_version >= 2. Call after applyDefaults.
func validateHalfClose(cfg *config.Config, configType string) error {
	if configType != "server" || !cfg.Server.MuxHalfClose {
		return nil
	}
	if t := cfg.Server.Transport; t != config.WSMUX && t != config.WSSMUX {
		return fmt.Errorf("server 'mux_half_close' is only supported on the wsmux/wssmux transports (transport is %q)", t)
	}
	if cfg.Server.MuxVersion < 2 {
		return fmt.Errorf("server 'mux_half_close' requires 'mux_version' >= 2 (the half-close flow kind is only read on mux_version 2)")
	}
	return nil
}

// validate checks a defaulted configuration and returns which role it describes
// ("server" or "client"). Every problem is reported as an error naming the key to
// fix, so Run can refuse to start rather than run half-configured.
func validate(cfg *config.Config) (role string, err error) {
	role = detectConfigType(cfg)
	if role == "" {
		return "", fmt.Errorf("neither server nor client configuration is properly set")
	}

	// Require an explicit token on the active side. There is no built-in
	// default: a tokenless deployment would otherwise authenticate peers with a
	// well-known value and act as an open relay. Both tunnel ends must share the
	// same token.
	token, peer := cfg.Server.Token, "client"
	if role == "client" {
		token, peer = cfg.Client.Token, "server"
	}
	if token == "" {
		return "", fmt.Errorf("%s 'token' is required: set it in the [%s] config (it must match the %s's token)", role, role, peer)
	}

	// mux_stripe and mux_stripe_parity share one set of bounds for both roles
	// (see validateStripeConfig); parity only does anything once striping is on,
	// because the plain, non-striped session path never looks at it.
	factor, parity := cfg.Server.StripeFactor, cfg.Server.StripeParity
	if role == "client" {
		factor, parity = cfg.Client.StripeFactor, cfg.Client.StripeParity
	}
	for _, check := range []error{
		validateStripeConfig(role, factor, parity),
		validateTransport(cfg, role),
		validateHalfClose(cfg, role),
		validateDNSMux(cfg, role),
	} {
		if check != nil {
			return "", check
		}
	}
	return role, nil
}

// Run loads the configuration at configPath and runs the server or client it
// describes until ctx is cancelled.
func Run(configPath string, ctx context.Context) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		logger.Fatalf("failed to load configuration: %v", err)
	}
	applyDefaults(cfg)

	role, err := validate(cfg)
	if err != nil {
		logger.Fatalf("%v", err)
	}

	switch role {
	case "server":
		if !cfg.Server.SkipOptz {
			ApplyTCPTuning()
		}
		srv := server.NewServer(&cfg.Server, ctx)
		go srv.Start()

		<-ctx.Done()
		srv.Stop()
		logger.Println("shutting down server...")
	case "client":
		if !cfg.Client.SkipOptz {
			ApplyTCPTuning()
		}
		clnt := client.NewClient(&cfg.Client, ctx)
		go clnt.Start()

		<-ctx.Done()
		clnt.Stop()
		logger.Println("shutting down client...")
	}
}

// loadConfig loads and parses the TOML configuration file.
func loadConfig(configPath string) (*config.Config, error) {
	var cfg config.Config
	meta, err := toml.DecodeFile(configPath, &cfg)
	if err != nil {
		return &cfg, err
	}

	// max_conn_age/max_drain were replaced by cdn_max_age. Ignoring them would
	// silently turn rotation off for whoever set them, so refuse to start.
	for _, key := range []string{"max_conn_age", "max_drain"} {
		if meta.IsDefined("server", key) {
			return &cfg, fmt.Errorf("server.%s was removed; set server.cdn_max_age (seconds, the shortest max connection age of your CDNs) instead", key)
		}
	}

	// tls_engine selected between Go's TLS and OpenSSL; OpenSSL is now the only
	// engine. An old config that still sets it keeps working (the value is
	// ignored), but say so rather than let it look meaningful.
	if meta.IsDefined("server", "tls_engine") {
		logger.Warn("server.tls_engine is ignored: TLS is always terminated by OpenSSL")
	}

	// SEC: Secure by default. If the user omitted tls_verify from the
	// configuration, default it to true to prevent unintentional MITM.
	if !meta.IsDefined("client", "tls_verify") {
		cfg.Client.TLSVerify = true
	}

	// Standards-framed wsmux/wssmux legs are on unless the operator turned them
	// off: an omitted key is true, an explicit false is honored (legacy raw).
	if !meta.IsDefined("server", "mux_ws_framing") {
		cfg.Server.MuxWSFraming = true
	}
	if !meta.IsDefined("client", "mux_ws_framing") {
		cfg.Client.MuxWSFraming = true
	}

	// Token-derived handshake names are on unless turned off, like framing.
	if !meta.IsDefined("client", "mux_stealth_handshake") {
		cfg.Client.MuxStealthHandshake = true
	}

	return &cfg, nil
}

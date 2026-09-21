package cmd

import (
	"context"
	"fmt"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/client"

	"github.com/musix/backhaul/internal/server"
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
	default:
		return ""
	}
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

func Run(configPath string, ctx context.Context) {
	// Load and parse the configuration file
	cfg, err := loadConfig(configPath)
	if err != nil {
		logger.Fatalf("failed to load configuration: %v", err)
	}

	// Apply default values to the configuration
	applyDefaults(cfg)

	configType := detectConfigType(cfg)
	if configType == "" {
		logger.Fatalf("neither server nor client configuration is properly set.")
	}

	// Require an explicit token on the active side. There is no built-in
	// default: a tokenless deployment would otherwise authenticate peers with a
	// well-known value and act as an open relay. Both tunnel ends must share the
	// same token.
	if configType == "server" && cfg.Server.Token == "" {
		logger.Fatalf("server 'token' is required: set it in the [server] config (it must match the client's token)")
	}
	if configType == "client" && cfg.Client.Token == "" {
		logger.Fatalf("client 'token' is required: set it in the [client] config (it must match the server's token)")
	}

	// mux_stripe_parity only does anything once striping is on (mux_stripe > 1)
	// - the plain, non-striped session path never looks at it - and the
	// Reed-Solomon library caps total shards (data+parity) at 256.
	if configType == "server" && cfg.Server.StripeParity > 0 {
		if cfg.Server.StripeFactor < 2 {
			logger.Fatalf("server 'mux_stripe_parity' requires 'mux_stripe' >= 2 (it adds parity legs on top of striping)")
		}
		if cfg.Server.StripeFactor+cfg.Server.StripeParity > 256 {
			logger.Fatalf("server 'mux_stripe' + 'mux_stripe_parity' must be <= 256")
		}
	}
	if configType == "client" && cfg.Client.StripeParity > 0 {
		if cfg.Client.StripeFactor < 2 {
			logger.Fatalf("client 'mux_stripe_parity' requires 'mux_stripe' >= 2 (it adds parity legs on top of striping)")
		}
		if cfg.Client.StripeFactor+cfg.Client.StripeParity > 256 {
			logger.Fatalf("client 'mux_stripe' + 'mux_stripe_parity' must be <= 256")
		}
	}

	if err := validateHalfClose(cfg, configType); err != nil {
		logger.Fatalf("%v", err)
	}

	// Determine whether to run as a server or client
	switch configType {
	case "server":
		// Apply temporary TCP optimizations at startup
		if !cfg.Server.SkipOptz {
			ApplyTCPTuning()
		}

		srv := server.NewServer(&cfg.Server, ctx) // server
		go srv.Start()

		// Wait for shutdown signal
		<-ctx.Done()
		srv.Stop()
		logger.Println("shutting down server...")
	case "client":
		// Apply temporary TCP optimizations at startup
		if !cfg.Client.SkipOptz {
			ApplyTCPTuning()
		}

		clnt := client.NewClient(&cfg.Client, ctx) // client
		go clnt.Start()

		// Wait for shutdown signal
		<-ctx.Done()
		clnt.Stop()
		logger.Println("shutting down client...")

	default:
		logger.Fatalf("neither server nor client configuration is properly set.")

	}
}

// loadConfig loads and parses the TOML configuration file.
func loadConfig(configPath string) (*config.Config, error) {
	var cfg config.Config
	meta, err := toml.DecodeFile(configPath, &cfg)
	if err != nil {
		return &cfg, err
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

	return &cfg, nil
}

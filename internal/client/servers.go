package client

import (
	"fmt"
	"strings"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils/network"
)

// ServerTunnel is one resolved [[client.servers]] entry: the full client
// configuration of one independent tunnel, with the entry's overrides applied
// on top of [client].
type ServerTunnel struct {
	Name   string
	Config config.ClientConfig
}

// ResolveServers turns cfg.Servers into one complete configuration per server
// and checks that the tunnels can run side by side in one process. It returns
// nil, nil when no servers are configured (the single-server path). Call it
// after the defaults are applied, so inherited values are already final.
//
// Everything that could make one tunnel disturb another is refused here, at
// startup, rather than left to misbehave later: two entries reaching the same
// server would each open a control channel to it and keep displacing the
// other, and two web panels on one port (or one usage file) would collide.
func ResolveServers(cfg *config.ClientConfig) ([]ServerTunnel, error) {
	if len(cfg.Servers) == 0 {
		return nil, nil
	}
	if cfg.Transport != config.WSMUX && cfg.Transport != config.WSSMUX {
		return nil, fmt.Errorf("client 'servers' is only supported on the wsmux/wssmux transports (transport is %q)", cfg.Transport)
	}
	if cfg.RemoteAddr != "" || len(cfg.RemoteAddrs) > 0 || cfg.EdgeIP != "" || len(cfg.EdgeIPs) > 0 {
		return nil, fmt.Errorf("client 'servers' is set: move remote_addr/remote_addrs/edge_ip/edge_ips into the [[client.servers]] entries instead of [client]")
	}

	tunnels := make([]ServerTunnel, 0, len(cfg.Servers))
	names := make(map[string]int)
	endpoints := make(map[string]int)
	webPorts := make(map[int]int)
	snifferLogs := make(map[string]int)

	for i, s := range cfg.Servers {
		label := fmt.Sprintf("client 'servers' entry %d", i+1)
		if s.RemoteAddr == "" && len(s.RemoteAddrs) == 0 {
			return nil, fmt.Errorf("%s: remote_addr or remote_addrs is required", label)
		}

		c := *cfg
		c.Servers = nil
		c.RemoteAddr = s.RemoteAddr
		c.RemoteAddrs = append([]string(nil), s.RemoteAddrs...)
		c.EdgeIP = s.EdgeIP
		c.EdgeIPs = append([]string(nil), s.EdgeIPs...)
		if s.Token != "" {
			c.Token = s.Token
		}
		if s.Path != "" {
			c.Path = s.Path
		}
		if s.TLSVerify != nil {
			c.TLSVerify = *s.TLSVerify
		}
		if s.ConnectionPool > 0 {
			c.ConnectionPool = s.ConnectionPool
		}
		// [client].web_port belongs to the first tunnel only: the others would
		// otherwise all try to bind the same port.
		if i > 0 {
			c.WebPort = 0
		}
		if s.WebPort > 0 {
			c.WebPort = s.WebPort
		}
		if s.SnifferLog != "" {
			c.SnifferLog = s.SnifferLog
		}

		name := s.Name
		if name == "" {
			name = c.RemoteAddr
			if name == "" {
				name = c.RemoteAddrs[0]
			}
		}
		if prev, ok := names[name]; ok {
			return nil, fmt.Errorf("%s: name %q is already used by entry %d; give each server a distinct name", label, name, prev)
		}
		names[name] = i + 1
		label = fmt.Sprintf("%s (%s)", label, name)

		if c.Token == "" {
			return nil, fmt.Errorf("%s: token is required (set it on the entry or in [client]; it must match that server's token)", label)
		}

		// One entry may list the same address more than once (that only weights
		// its round-robin), but two entries must never reach the same server.
		path := network.NormalizeBasePath(c.Path)
		seen := make(map[string]bool)
		for _, addr := range tunnelAddrs(c) {
			key := strings.ToLower(strings.TrimSpace(addr)) + path
			if seen[key] {
				continue
			}
			seen[key] = true
			if prev, ok := endpoints[key]; ok {
				return nil, fmt.Errorf("%s: %s%s is already used by entry %d; two tunnels to one server would keep displacing each other's control channel", label, addr, path, prev)
			}
			endpoints[key] = i + 1
		}

		if c.WebPort > 0 {
			if prev, ok := webPorts[c.WebPort]; ok {
				return nil, fmt.Errorf("%s: web_port %d is already used by entry %d; give each tunnel its own web_port", label, c.WebPort, prev)
			}
			webPorts[c.WebPort] = i + 1
			if c.Sniffer {
				if prev, ok := snifferLogs[c.SnifferLog]; ok {
					return nil, fmt.Errorf("%s: sniffer_log %q is already used by entry %d; give each tunnel with a web_port its own sniffer_log", label, c.SnifferLog, prev)
				}
				snifferLogs[c.SnifferLog] = i + 1
			}
		}

		tunnels = append(tunnels, ServerTunnel{Name: name, Config: c})
	}
	return tunnels, nil
}

// tunnelAddrs is every address the tunnel dials, as buildEndpoints reads them.
func tunnelAddrs(c config.ClientConfig) []string {
	if len(c.RemoteAddrs) > 0 {
		return c.RemoteAddrs
	}
	return []string{c.RemoteAddr}
}

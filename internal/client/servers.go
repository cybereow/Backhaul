package client

import (
	"fmt"
	"strings"

	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils/network"
)

// ServerTunnel is one [[client.servers]] entry ready to run: the full client
// configuration of one independent tunnel and the name its logs carry.
type ServerTunnel struct {
	Name   string
	Config config.ClientConfig
}

// ResolveServers checks that the tunnels of a multi-server client
// (cfg.Tunnels, which the loader decoded from [[client.servers]] over [client])
// can run side by side in one process, and names them. It returns nil, nil when
// no servers are configured (the single-server path). Call it after the
// defaults are applied, so every value is final.
//
// Everything that could make one tunnel disturb another is refused here, at
// startup, rather than left to misbehave later: two entries reaching the same
// server would each open a control channel to it and keep displacing the
// other, and two web panels on one port (or one usage file) would collide.
func ResolveServers(cfg *config.ClientConfig) ([]ServerTunnel, error) {
	if len(cfg.Tunnels) == 0 {
		return nil, nil
	}
	if cfg.RemoteAddr != "" || len(cfg.RemoteAddrs) > 0 || cfg.EdgeIP != "" || len(cfg.EdgeIPs) > 0 {
		return nil, fmt.Errorf("client 'servers' is set: move remote_addr/remote_addrs/edge_ip/edge_ips into the [[client.servers]] entries instead of [client]")
	}

	tunnels := make([]ServerTunnel, 0, len(cfg.Tunnels))
	names := make(map[string]int)
	endpoints := make(map[string]int)
	webPorts := make(map[int]int)
	snifferLogs := make(map[string]int)

	for i, c := range cfg.Tunnels {
		label := fmt.Sprintf("client 'servers' entry %d", i+1)
		if c.RemoteAddr == "" && len(c.RemoteAddrs) == 0 {
			return nil, fmt.Errorf("%s: remote_addr or remote_addrs is required", label)
		}

		name := c.Name
		if name == "" {
			name = tunnelAddrs(c)[0]
		}
		if prev, ok := names[name]; ok {
			return nil, fmt.Errorf("%s: name %q is already used by entry %d; give each server a distinct name", label, name, prev)
		}
		names[name] = i + 1
		label = fmt.Sprintf("%s (%s)", label, name)

		if c.Transport != config.WSMUX && c.Transport != config.WSSMUX {
			return nil, fmt.Errorf("%s: client 'servers' is only supported on the wsmux/wssmux transports (transport is %q)", label, c.Transport)
		}
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

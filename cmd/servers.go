package cmd

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/musix/backhaul/config"
)

// processWideClientKeys are [client] keys that configure the whole process, not
// one tunnel, so a [[client.servers]] entry may not set them.
var processWideClientKeys = map[string]bool{
	"servers":   true,
	"pprof":     true,
	"skip_optz": true,
}

// expandClientServers decodes every [[client.servers]] entry over a copy of
// [client] into cfg.Client.Tunnels: a key an entry sets replaces the [client]
// value, every other key is inherited. It runs before applyDefaults, so the
// defaults are then worked out per tunnel from what that tunnel ends up with
// (e.g. a derived mux_streambuffer follows the entry's own mux_recievebuffer).
//
// Unknown keys in an entry are refused: inside a server entry a typo would
// otherwise silently fall back to the [client] value.
func expandClientServers(cfg *config.Config, md toml.MetaData) error {
	if len(cfg.Client.Servers) == 0 {
		return nil
	}

	base := cfg.Client
	base.Servers = nil
	base.Tunnels = nil
	// A name labels one tunnel; inherited, every tunnel would share it.
	base.Name = ""

	tunnels := make([]config.ClientConfig, len(cfg.Client.Servers))
	for i, prim := range cfg.Client.Servers {
		c := cloneClientConfig(base)
		// [client].web_port belongs to the first tunnel only: the others would
		// otherwise all try to bind the same port. An entry can still set one.
		if i > 0 {
			c.WebPort = 0
		}
		if err := md.PrimitiveDecode(prim, &c); err != nil {
			return fmt.Errorf("client 'servers' entry %d: %w", i+1, err)
		}
		tunnels[i] = c
	}

	// Every entry has been decoded into the struct, so whatever is still
	// undecoded under client.servers is a key ClientConfig does not have.
	var unknown []string
	for _, k := range md.Undecoded() {
		if len(k) > 2 && k[0] == "client" && k[1] == "servers" {
			unknown = append(unknown, strings.Join(k[2:], "."))
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("client 'servers': unknown key(s) %s", strings.Join(unknown, ", "))
	}

	// Decoding into a map marks keys as decoded, so it only runs after the
	// unknown-key check above.
	for i, prim := range cfg.Client.Servers {
		var keys map[string]interface{}
		if err := md.PrimitiveDecode(prim, &keys); err != nil {
			return fmt.Errorf("client 'servers' entry %d: %w", i+1, err)
		}
		for k := range keys {
			if processWideClientKeys[k] {
				return fmt.Errorf("client 'servers' entry %d: %q applies to the whole process; set it in [client] instead", i+1, k)
			}
		}
	}

	cfg.Client.Tunnels = tunnels
	return nil
}

// cloneClientConfig copies c with fresh backing arrays for every slice field.
// The TOML decoder writes a decoded array into the existing backing array, so
// decoding an entry over a shallow copy would overwrite [client]'s own list
// (and with it every other tunnel's inherited one).
func cloneClientConfig(c config.ClientConfig) config.ClientConfig {
	v := reflect.ValueOf(&c).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() != reflect.Slice || f.IsNil() || !f.CanSet() {
			continue
		}
		cp := reflect.MakeSlice(f.Type(), f.Len(), f.Len())
		reflect.Copy(cp, f)
		f.Set(cp)
	}
	return c
}

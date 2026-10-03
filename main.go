// Command backhaul is a reverse tunnel: a server and a client that carry
// forwarded TCP (and, over wsmux/wssmux, UDP) connections across ws, wss, wsmux,
// wssmux, dns or dnsmux.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/musix/backhaul/cmd"
	"github.com/musix/backhaul/internal/utils"
)

const version = "v0.7.3"

var logger = utils.NewLogger("info")

const (
	// reloadPollInterval is how often the config file's mtime is checked.
	reloadPollInterval = 2 * time.Second
	// reloadSettle gives the old instance time to release its ports before the
	// new one binds them.
	reloadSettle = 2 * time.Second
	// shutdownGrace gives running instances time to close before the process exits.
	shutdownGrace = 1 * time.Second
)

func main() {
	configPath := flag.String("c", "", "path to the configuration file (TOML format)")
	probePath := flag.String("probe", "", "run the standalone DNS reachability/capacity prober with this [dns_probe] config and exit (does not start a tunnel)")
	showVersion := flag.Bool("v", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// The DNS prober/responder is a standalone diagnostic, kept off the normal
	// run path and its config hot-reload. It runs until it finishes (prober) or
	// is interrupted (responder).
	if *probePath != "" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-sigChan
			cancel()
		}()
		cmd.RunProbe(*probePath, ctx)
		return
	}

	if *configPath == "" {
		logger.Fatalf("Usage: %s -c /path/to/config.toml", flag.CommandLine.Name())
	}

	runner := &reloadingRunner{path: *configPath}
	runner.start()
	go runner.watch()

	<-sigChan
	runner.stop()
	time.Sleep(shutdownGrace)
}

// reloadingRunner runs the tunnel described by a config file and restarts it
// whenever the file changes.
type reloadingRunner struct {
	path string

	mu     sync.Mutex
	cancel context.CancelFunc
	done   bool
}

// start launches a fresh instance from the config file, unless stop has been
// called: a permanent stop must never be undone by a reload that was mid-delay.
func (r *reloadingRunner) start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go cmd.Run(r.path, ctx)
}

// stop ends the running instance for good (no further reloads).
func (r *reloadingRunner) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
	if r.cancel != nil {
		r.cancel()
	}
}

// restart replaces the running instance with one built from the current config.
func (r *reloadingRunner) restart() {
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.cancel()
	r.mu.Unlock()

	time.Sleep(reloadSettle)
	r.start()
}

// watch polls the config file's modification time and restarts on change.
func (r *reloadingRunner) watch() {
	lastMod, err := modTime(r.path)
	if err != nil {
		logger.Fatalf("Error getting modification time: %v", err)
	}

	ticker := time.NewTicker(reloadPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		done := r.done
		r.mu.Unlock()
		if done {
			return
		}

		mod, err := modTime(r.path)
		if err != nil {
			logger.Errorf("Error checking file modification time: %v", err)
			continue
		}
		if mod.After(lastMod) {
			logger.Info("Config file changed, reloading application")
			lastMod = mod
			r.restart()
		}
	}
}

func modTime(file string) (time.Time, error) {
	abs, err := filepath.Abs(file)
	if err != nil {
		return time.Time{}, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

package server

import (
	"context"
	"testing"
	"time"

	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
)

func TestNewServer(t *testing.T) {
	cfg := &config.ServerConfig{
		BindAddr: "127.0.0.1:8080",
		LogLevel: "debug",
	}

	parentCtx := context.Background()

	srv := NewServer(cfg, parentCtx)

	assert.NotNil(t, srv)
	assert.Equal(t, cfg, srv.config)
	assert.NotNil(t, srv.ctx)
	assert.NotNil(t, srv.cancel)
	assert.NotNil(t, srv.logger)
}

func TestServerStop(t *testing.T) {
	cfg := &config.ServerConfig{
		BindAddr: "127.0.0.1:8080",
		LogLevel: "info",
	}

	parentCtx := context.Background()

	srv := NewServer(cfg, parentCtx)

	assert.NotNil(t, srv)
	assert.NotNil(t, srv.cancel)

	srv.Stop()

	// Ensure the context was cancelled
	select {
	case <-srv.ctx.Done():
		// Success
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled within 1 second")
	}
}

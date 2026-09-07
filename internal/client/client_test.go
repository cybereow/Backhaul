package client

import (
	"context"
	"testing"

	"github.com/musix/backhaul/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cfg := &config.ClientConfig{
		LogLevel: "debug",
	}
	parentCtx := context.Background()

	client := NewClient(cfg, parentCtx)

	require.NotNil(t, client, "Expected NewClient to return a non-nil client")
	assert.Equal(t, cfg, client.config, "Expected config to be assigned correctly")
	assert.NotNil(t, client.ctx, "Expected context to be initialized")
	assert.NotNil(t, client.cancel, "Expected cancel function to be initialized")
	assert.NotNil(t, client.logger, "Expected logger to be initialized")

	// Ensure context cancellation works and context is not exactly parentCtx
	assert.NotEqual(t, parentCtx, client.ctx, "Expected a new child context to be created")

	client.Stop()
	// The context should now be canceled
	select {
	case <-client.ctx.Done():
		// Success
	default:
		t.Error("Expected context to be canceled after calling Stop()")
	}
}

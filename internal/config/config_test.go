package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadUsesLoopbackAndNoCrossOriginAccessByDefault(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("GRPC_ADDR", "")
	t.Setenv("CORS_ALLOWED_ORIGINS", "")

	cfg := Load()

	require.Equal(t, "127.0.0.1:8080", cfg.HTTPAddr)
	require.Equal(t, "127.0.0.1:9090", cfg.GRPCAddr)
	require.Empty(t, cfg.AllowedOrigins)
}

func TestLoadParsesAllowedOrigins(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://one.example, https://two.example")

	cfg := Load()

	require.Equal(t, []string{"https://one.example", "https://two.example"}, cfg.AllowedOrigins)
}

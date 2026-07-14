package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCORSAllowsConfiguredOrigin(t *testing.T) {
	server := &Server{allowedOrigins: map[string]struct{}{"http://localhost:5173": {}}}
	handler := server.cors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodOptions, "/v1/jobs", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, "http://localhost:5173", response.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, response.Header().Values("Vary"), "Origin")
}

func TestCORSRejectsUnconfiguredOrigin(t *testing.T) {
	server := &Server{allowedOrigins: map[string]struct{}{"http://localhost:5173": {}}}
	called := false
	handler := server.cors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Origin", "https://example.invalid")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	require.Equal(t, http.StatusForbidden, response.Code)
	require.False(t, called)
	require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSLeavesNonBrowserClientsUnaffected(t *testing.T) {
	server := &Server{}
	handler := server.cors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/jobs", nil))

	require.Equal(t, http.StatusAccepted, response.Code)
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", response.Header().Get("X-Frame-Options"))
}

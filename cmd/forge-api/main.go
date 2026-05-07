package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/connor/forge/internal/api"
	"github.com/connor/forge/internal/config"
	"github.com/connor/forge/internal/grpcsvc"
	"github.com/connor/forge/internal/metrics"
	"github.com/connor/forge/internal/queue"
	"github.com/connor/forge/internal/ratelimit"
	"github.com/connor/forge/internal/store"
	"github.com/connor/forge/internal/tracing"
	"github.com/connor/forge/proto/forgepb"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, "forge-api", cfg.OTLPEndpoint)
	if err != nil {
		logger.Warn("tracing disabled", "error", err)
	} else {
		defer shutdownTracing(context.Background())
	}
	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database open failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	q := queue.New(db)
	stats := metrics.NewRecorder(redisClient)
	hub := api.NewHub()
	events := api.NewRedisEvents(redisClient, hub)
	go events.Run(ctx)
	go metrics.StartDepthSampler(ctx, db, cfg.Queues)

	server := api.NewServer(q, hub, events, stats, ratelimit.New(redisClient, cfg.RateLimitPerSec, cfg.RateLimitBurst), logger)
	go server.StartStatsTicks(ctx)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcServer := grpc.NewServer()
	forgepb.RegisterForgeServer(grpcServer, grpcsvc.New(q, stats, events))
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("grpc listen failed", "error", err)
		os.Exit(1)
	}
	go func() {
		logger.Info("grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc stopped", "error", err)
		}
	}()
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http stopped", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/connor/forge/internal/api"
	"github.com/connor/forge/internal/config"
	"github.com/connor/forge/internal/metrics"
	"github.com/connor/forge/internal/queue"
	"github.com/connor/forge/internal/store"
	"github.com/connor/forge/internal/tracing"
	"github.com/connor/forge/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := tracing.Init(ctx, "forge-worker", cfg.OTLPEndpoint)
	if err == nil {
		defer func() { _ = shutdownTracing(context.Background()) }()
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
	defer func() { _ = redisClient.Close() }()
	q := queue.New(db)
	pool := &worker.Pool{
		Queue:         q,
		Queues:        cfg.Queues,
		Registry:      worker.DefaultRegistry(db),
		Concurrency:   cfg.Concurrency,
		WorkerID:      cfg.WorkerID,
		Lease:         cfg.LeaseDuration,
		JobTimeout:    cfg.JobTimeout,
		PollInterval:  100 * time.Millisecond,
		ShutdownGrace: cfg.ShutdownGrace,
		Events:        api.NewRedisEvents(redisClient, nil),
		Stats:         metrics.NewRecorder(redisClient),
		Logger:        logger,
	}
	if err := pool.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

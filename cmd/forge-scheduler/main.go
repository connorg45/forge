package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/connor/forge/internal/config"
	"github.com/connor/forge/internal/queue"
	"github.com/connor/forge/internal/scheduler"
	"github.com/connor/forge/internal/store"
	"github.com/connor/forge/internal/tracing"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := tracing.Init(ctx, "forge-scheduler", cfg.OTLPEndpoint)
	if err == nil {
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
	q := queue.New(db)
	err = scheduler.New(db, q, logger).Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}
}

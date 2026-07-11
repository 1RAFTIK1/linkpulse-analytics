// Analytics service — консьюмер топика link-clicks: идемпотентная запись
// кликов и часовая агрегация. gRPC API (GetLinkStats, StreamLiveClicks)
// добавляется в фазе 4.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1RAFTIK1/linkpulse-analytics/internal/config"
	"github.com/1RAFTIK1/linkpulse-analytics/internal/consumer"
	"github.com/1RAFTIK1/linkpulse-analytics/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("сервис завершился с ошибкой", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	store, err := storage.NewPostgres(initCtx, cfg.PostgresDSN, log)
	if err != nil {
		return err
	}
	defer store.Close()

	cons, err := consumer.New(initCtx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, store, log)
	if err != nil {
		return err
	}
	defer cons.Close()

	log.Info("analytics запущен", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	// Run блокируется до SIGTERM/SIGINT (ctx) или фатальной ошибки БД.
	// Graceful shutdown по спеке §11: цикл сам не коммитит недообработанное.
	return cons.Run(ctx)
}

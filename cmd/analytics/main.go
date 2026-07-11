// Analytics service — консьюмер топика link-clicks (идемпотентная запись,
// часовая агрегация) + gRPC API: GetLinkStats и StreamLiveClicks (live-события
// для дашборда через in-memory fan-out).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	analyticsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/analytics/v1"

	"github.com/1RAFTIK1/linkpulse-analytics/internal/config"
	"github.com/1RAFTIK1/linkpulse-analytics/internal/consumer"
	"github.com/1RAFTIK1/linkpulse-analytics/internal/fanout"
	"github.com/1RAFTIK1/linkpulse-analytics/internal/grpcapi"
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

	hub := fanout.New(log)

	cons, err := consumer.New(initCtx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, store, hub, log)
	if err != nil {
		return err
	}
	defer cons.Close()

	// gRPC-сервер — в отдельной горутине рядом с консьюмером.
	grpcServer := grpc.NewServer()
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, grpcapi.NewServer(store, hub, log))

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("grpc сервер запущен", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()
	go func() {
		log.Info("analytics запущен", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)
		errCh <- cons.Run(ctx) // блокируется до отмены ctx или ошибки БД
	}()

	// Ждём сигнал (ctx) или фатальную ошибку любой из горутин.
	select {
	case err := <-errCh:
		grpcServer.Stop()
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: GracefulStop дожидается активных RPC (стримы получат
	// отмену ctx), с таймаутом на случай зависших клиентов.
	log.Info("получен сигнал, останавливаемся", "timeout", cfg.ShutdownTimeout)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("grpc GracefulStop не уложился в таймаут, останавливаем принудительно")
		grpcServer.Stop()
	}

	// Консьюмер выходит сам по отмене ctx (без коммита недообработанного).
	if err := <-errCh; err != nil {
		return err
	}
	log.Info("сервис остановлен корректно")
	return nil
}

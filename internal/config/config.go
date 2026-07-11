// Package config — конфигурация Analytics service из переменных окружения.
// Тот же подход, что в linkpulse-link: stdlib, fail-fast, агрегированные ошибки.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	PostgresDSN string // БД linkpulse_analytics (обязательна)

	KafkaBrokers []string // обязательны: консьюмер без Kafka бессмыслен
	KafkaTopic   string
	KafkaGroup   string // consumer group — офсеты хранятся на группу

	GRPCAddr string // адрес gRPC API (GetLinkStats, StreamLiveClicks)

	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	var errs []error

	cfg := Config{
		PostgresDSN:     os.Getenv("POSTGRES_DSN"),
		KafkaBrokers:    splitNonEmpty(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "link-clicks"),
		KafkaGroup:      getEnv("KAFKA_GROUP", "analytics-service"),
		GRPCAddr:        getEnv("GRPC_ADDR", ":50051"),
		ShutdownTimeout: 10 * time.Second,
	}

	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN обязателен"))
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS обязателен"))
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("конфиг: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func splitNonEmpty(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Package consumer — Kafka-консьюмер событий клика.
//
// Семантика: at-least-once от Kafka + идемпотентная запись (storage) =
// effectively-once в БД. Достигается порядком операций:
//
//	poll → decode → SaveBatch (транзакция) → CommitRecords
//
// Офсеты коммитятся ТОЛЬКО после успешной транзакции. Упали между commit tx
// и commit офсетов — батч приедет снова, дедуп по event_id его отсеет.
package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/events/v1"
)

// Storage — приёмник батчей событий (Postgres в проде). Возвращает события,
// реально вставленные в этом вызове (без дублей).
type Storage interface {
	SaveBatch(ctx context.Context, events []*eventsv1.ClickEvent) ([]*eventsv1.ClickEvent, error)
}

// Publisher — приёмник live-событий (fan-out хаб gRPC-стримов). Получает
// только новые события: дубли переигранных батчей сюда не попадают.
type Publisher interface {
	Publish(events []*eventsv1.ClickEvent)
}

type Consumer struct {
	client *kgo.Client
	store  Storage
	pub    Publisher
	log    *slog.Logger
}

func New(ctx context.Context, brokers []string, topic, group string, store Storage, pub Publisher, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		// Автокоммит выключен: сами коммитим после записи в БД (спека §11 —
		// «коммитим офсеты только обработанных сообщений»).
		kgo.DisableAutoCommit(),
		// Первый запуск группы: читаем топик с начала, не теряем историю.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return &Consumer{client: client, store: store, pub: pub, log: log}, nil
}

// Run крутит poll-цикл до отмены ctx. Возвращает ошибку только при сбое
// записи в БД: сервис падает с некоммиченными офсетами (fail-fast), после
// рестарта батч переигрывается — данные не теряются.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("консьюмер запущен")
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			// Остановка: незакоммиченное переиграется после рестарта.
			c.log.Info("консьюмер останавливается")
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.log.Error("kafka fetch", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		events := c.decode(ctx, records)
		inserted, err := c.store.SaveBatch(ctx, events)
		if err != nil {
			return fmt.Errorf("сохранение батча (%d событий): %w", len(events), err)
		}

		// Live-лента получает только новые события — уже после фиксации в БД.
		if c.pub != nil {
			c.pub.Publish(inserted)
		}

		if err := c.client.CommitRecords(ctx, records...); err != nil {
			// БД уже записала батч; после рестарта дубли отсеет дедуп.
			c.log.Error("коммит офсетов", "error", err)
		}
	}
}

// decode разбирает protobuf. Битое сообщение (poison pill) пишем в лог и
// пропускаем: одно кривое событие не должно навечно заблокировать партицию.
// Офсет при этом коммитится вместе с батчом — сообщение осознанно теряется.
func (c *Consumer) decode(ctx context.Context, records []*kgo.Record) []*eventsv1.ClickEvent {
	events := make([]*eventsv1.ClickEvent, 0, len(records))
	for _, rec := range records {
		var ev eventsv1.ClickEvent
		if err := proto.Unmarshal(rec.Value, &ev); err != nil {
			c.log.ErrorContext(ctx, "битое сообщение — пропущено",
				"error", err, "partition", rec.Partition, "offset", rec.Offset)
			continue
		}
		events = append(events, &ev)
	}
	return events
}

// Close покидает consumer group (быстрый ребаланс для остальных участников).
func (c *Consumer) Close() { c.client.Close() }

// Package storage — запись кликов и часовых агрегатов в Postgres.
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	eventsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/events/v1"
)

type Postgres struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewPostgres(ctx context.Context, dsn string, log *slog.Logger) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("создание пула: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool, log: log}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

// bucketKey — ключ часового агрегата.
type bucketKey struct {
	linkID int64
	hour   time.Time
}

// SaveBatch идемпотентно пишет пачку событий и обновляет часовые агрегаты —
// всё в ОДНОЙ транзакции: либо факты и агрегаты записаны согласованно, либо
// ничего (и офсеты Kafka не коммитятся — батч приедет снова).
//
// Идемпотентность: PRIMARY KEY(id=event_id) + ON CONFLICT DO NOTHING. Дубль
// (повторная доставка после ребаланса) не вставляется, и — принципиально —
// НЕ инкрементирует агрегат: смотрим RowsAffected каждой вставки.
func (p *Postgres) SaveBatch(ctx context.Context, events []*eventsv1.ClickEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback после commit — no-op

	const insertClick = `
		INSERT INTO clicks (id, link_id, short_code, clicked_at, referrer, country)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING`

	buckets := make(map[bucketKey]int64)
	var dups int
	for _, ev := range events {
		tag, err := tx.Exec(ctx, insertClick,
			ev.GetEventId(), ev.GetLinkId(), ev.GetShortCode(),
			ev.GetClickedAt().AsTime(), ev.GetReferrer(), ev.GetCountry())
		if err != nil {
			return fmt.Errorf("insert click %d: %w", ev.GetEventId(), err)
		}
		if tag.RowsAffected() == 0 {
			dups++ // дубль — агрегат не трогаем
			continue
		}
		key := bucketKey{
			linkID: ev.GetLinkId(),
			hour:   ev.GetClickedAt().AsTime().UTC().Truncate(time.Hour),
		}
		buckets[key]++
	}

	const upsertBucket = `
		INSERT INTO link_stats_hourly (link_id, hour_bucket, click_count)
		VALUES ($1, $2, $3)
		ON CONFLICT (link_id, hour_bucket)
		DO UPDATE SET click_count = link_stats_hourly.click_count + EXCLUDED.click_count`

	for key, n := range buckets {
		if _, err := tx.Exec(ctx, upsertBucket, key.linkID, key.hour, n); err != nil {
			return fmt.Errorf("upsert bucket link=%d hour=%s: %w", key.linkID, key.hour, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	if dups > 0 {
		p.log.InfoContext(ctx, "батч сохранён с дублями", "events", len(events), "duplicates", dups)
	}
	return nil
}

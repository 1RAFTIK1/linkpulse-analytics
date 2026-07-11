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
//
// Возвращает события, вставленные ИМЕННО в этом вызове: их (и только их)
// консьюмер раздаёт в live-стримы — переигранный батч не порождает
// «призрачных» кликов на дашборде.
func (p *Postgres) SaveBatch(ctx context.Context, events []*eventsv1.ClickEvent) ([]*eventsv1.ClickEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback после commit — no-op

	const insertClick = `
		INSERT INTO clicks (id, link_id, short_code, clicked_at, referrer, country)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING`

	buckets := make(map[bucketKey]int64)
	inserted := make([]*eventsv1.ClickEvent, 0, len(events))
	for _, ev := range events {
		tag, err := tx.Exec(ctx, insertClick,
			ev.GetEventId(), ev.GetLinkId(), ev.GetShortCode(),
			ev.GetClickedAt().AsTime(), ev.GetReferrer(), ev.GetCountry())
		if err != nil {
			return nil, fmt.Errorf("insert click %d: %w", ev.GetEventId(), err)
		}
		if tag.RowsAffected() == 0 {
			continue // дубль — ни агрегат, ни live-лента его не видят
		}
		inserted = append(inserted, ev)
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
			return nil, fmt.Errorf("upsert bucket link=%d hour=%s: %w", key.linkID, key.hour, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	if dups := len(events) - len(inserted); dups > 0 {
		p.log.InfoContext(ctx, "батч сохранён с дублями", "events", len(events), "duplicates", dups)
	}
	return inserted, nil
}

// HourlyBucket — часовой интервал для GetLinkStats.
type HourlyBucket struct {
	Hour  time.Time
	Count int64
}

// GetLinkStats возвращает сумму и часовую разбивку кликов ссылки за период
// (границы включительно). Читает денормализованный агрегат, а не COUNT(*)
// по фактам — дёшево при любом объёме кликов.
func (p *Postgres) GetLinkStats(ctx context.Context, linkID int64, from, to time.Time) (int64, []HourlyBucket, error) {
	const q = `
		SELECT hour_bucket, click_count
		FROM link_stats_hourly
		WHERE link_id = $1 AND hour_bucket BETWEEN $2 AND $3
		ORDER BY hour_bucket`

	rows, err := p.pool.Query(ctx, q, linkID, from.UTC(), to.UTC())
	if err != nil {
		return 0, nil, fmt.Errorf("select stats: %w", err)
	}
	defer rows.Close()

	var (
		total   int64
		buckets []HourlyBucket
	)
	for rows.Next() {
		var b HourlyBucket
		if err := rows.Scan(&b.Hour, &b.Count); err != nil {
			return 0, nil, fmt.Errorf("scan bucket: %w", err)
		}
		total += b.Count
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("iterate buckets: %w", err)
	}
	return total, buckets, nil
}

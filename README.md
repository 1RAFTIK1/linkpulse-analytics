# linkpulse-analytics

Analytics service проекта **LinkPulse**: консьюмер Kafka-топика `link-clicks`,
идемпотентная запись кликов и часовая агрегация. В фазе 4 добавляется gRPC API
(`GetLinkStats`, `StreamLiveClicks`) для дашборда.

## Как устроена доставка (главное в этом сервисе)

Kafka даёт **at-least-once**: после ребаланса или рестарта событие может
прийти повторно. Сервис превращает это в **effectively-once в БД**:

```
poll → protobuf decode → транзакция Postgres → commit офсетов
                          │
                          ├─ INSERT clicks ... ON CONFLICT (id) DO NOTHING
                          │    id = event_id (Snowflake из Link service)
                          └─ агрегат инкрементируется ТОЛЬКО по реально
                             вставленным строкам (RowsAffected)
```

- Офсеты коммитятся **только после** commit'а транзакции. Сбой между ними →
  батч переигрывается → дубли гасятся PRIMARY KEY.
- Агрегат `link_stats_hourly` не расходится с фактами: дубль не инкрементирует
  счётчик часа.
- Ошибка БД → сервис падает с некоммиченными офсетами (fail-fast), после
  рестарта данные не теряются.
- Битое сообщение (poison pill) логируется и пропускается — не блокирует
  партицию навечно.

Проверено вживую: сброс офсетов группы на `--to-earliest` и переигрывание
всех событий не меняет ни `clicks`, ни `link_stats_hourly`.

## Данные (Postgres, БД linkpulse_analytics)

```sql
clicks(id PK, link_id, short_code, clicked_at, referrer, country)
link_stats_hourly(link_id, hour_bucket, click_count, PK(link_id, hour_bucket))
```

## Конфигурация (env)

| Переменная | Дефолт | Описание |
|---|---|---|
| `POSTGRES_DSN` | — (обязательна) | БД linkpulse_analytics |
| `KAFKA_BROKERS` | `localhost:9092` | список брокеров через запятую |
| `KAFKA_TOPIC` | `link-clicks` | топик событий |
| `KAFKA_GROUP` | `analytics-service` | consumer group |

## Запуск локально

```bash
cd ../linkpulse-infra && make up          # Postgres + Redis + Kafka
make tools && make migrate-up             # миграции
make run
```

## Зависимости и версии

| Библиотека | Версия | Роль |
|---|---|---|
| twmb/franz-go | 1.21.5 | Kafka-клиент (чистый Go, консьюмер-группы, ручной коммит офсетов) |
| jackc/pgx/v5 | 5.10.0 | Postgres |
| linkpulse-contracts | local replace | protobuf-схема ClickEvent |

Go 1.26. До публикации contracts на GitHub используется `replace` на соседнюю
папку (см. комментарий в Dockerfile и CI).

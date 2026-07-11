-- Факты кликов. id = event_id из ClickEvent (Snowflake, генерируется Link service).
-- PRIMARY KEY и есть механизм идемпотентности консьюмера: повторная доставка
-- события упирается в ON CONFLICT (id) DO NOTHING.
CREATE TABLE clicks (
    id         BIGINT PRIMARY KEY,
    link_id    BIGINT NOT NULL,
    short_code TEXT NOT NULL,
    clicked_at TIMESTAMPTZ NOT NULL,
    referrer   TEXT NOT NULL DEFAULT '',
    country    TEXT NOT NULL DEFAULT ''
);

-- GetLinkStats(link_id, from, to) ходит по этому индексу.
CREATE INDEX idx_clicks_link_time ON clicks (link_id, clicked_at);

-- Часовые агрегаты: денормализация под быстрые графики дашборда,
-- чтобы не считать COUNT(*) по clicks на каждый запрос статистики.
CREATE TABLE link_stats_hourly (
    link_id     BIGINT NOT NULL,
    hour_bucket TIMESTAMPTZ NOT NULL,
    click_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (link_id, hour_bucket)
);

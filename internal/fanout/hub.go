// Package fanout — in-memory раздача live-событий подписчикам gRPC-стримов.
//
// Консьюмер Kafka после успешной записи батча публикует НОВЫЕ события в Hub,
// Hub раскладывает их по каналам подписчиков конкретного link_id
// (map link_id → подписчики). Дашборд не читает Kafka напрямую: подписки
// и их жизненный цикл живут здесь, рядом с единственным читателем топика.
package fanout

import (
	"log/slog"
	"sync"

	eventsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/events/v1"
)

// subscriberBuffer — ёмкость канала подписчика. Буфер сглаживает всплески;
// политика при переполнении — drop (см. Publish).
const subscriberBuffer = 64

type subscriber struct {
	ch chan *eventsv1.ClickEvent
}

// Hub потокобезопасен; создаётся New().
type Hub struct {
	mu   sync.RWMutex
	subs map[int64]map[*subscriber]struct{} // link_id → подписчики
	log  *slog.Logger
}

func New(log *slog.Logger) *Hub {
	return &Hub{subs: make(map[int64]map[*subscriber]struct{}), log: log}
}

// Subscribe регистрирует подписку на события ссылки. Возвращает канал событий
// и функцию отписки; отписка закрывает канал (читатель узнаёт о конце) и
// подчищает пустые map-записи.
func (h *Hub) Subscribe(linkID int64) (<-chan *eventsv1.ClickEvent, func()) {
	sub := &subscriber{ch: make(chan *eventsv1.ClickEvent, subscriberBuffer)}

	h.mu.Lock()
	if h.subs[linkID] == nil {
		h.subs[linkID] = make(map[*subscriber]struct{})
	}
	h.subs[linkID][sub] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[linkID], sub)
			if len(h.subs[linkID]) == 0 {
				delete(h.subs, linkID)
			}
			h.mu.Unlock()
			close(sub.ch)
		})
	}
	return sub.ch, cancel
}

// Publish раздаёт события подписчикам их link_id. Отправка неблокирующая:
// если буфер подписчика полон (медленный клиент), событие для него
// отбрасывается — live-лента не обязана быть полной, полные данные всегда
// в Postgres. Блокироваться здесь нельзя: Publish зовётся из цикла консьюмера.
func (h *Hub) Publish(events []*eventsv1.ClickEvent) {
	if len(events) == 0 {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ev := range events {
		for sub := range h.subs[ev.GetLinkId()] {
			select {
			case sub.ch <- ev:
			default:
				h.log.Warn("fanout: буфер подписчика полон, событие пропущено",
					"link_id", ev.GetLinkId(), "event_id", ev.GetEventId())
			}
		}
	}
}

// Subscribers — число активных подписок (пригодится метрикам в фазе 6).
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, m := range h.subs {
		n += len(m)
	}
	return n
}

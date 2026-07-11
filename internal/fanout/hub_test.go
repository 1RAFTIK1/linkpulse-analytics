package fanout

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	eventsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/events/v1"
)

func newHub() *Hub {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func ev(linkID, eventID int64) *eventsv1.ClickEvent {
	return &eventsv1.ClickEvent{LinkId: linkID, EventId: eventID}
}

func TestSubscribePublish_RoutesByLinkID(t *testing.T) {
	h := newHub()
	ch1, cancel1 := h.Subscribe(1)
	ch2, cancel2 := h.Subscribe(2)
	defer cancel1()
	defer cancel2()

	h.Publish([]*eventsv1.ClickEvent{ev(1, 100), ev(2, 200), ev(1, 101)})

	if got := (<-ch1).GetEventId(); got != 100 {
		t.Errorf("ch1 первое событие: %d", got)
	}
	if got := (<-ch1).GetEventId(); got != 101 {
		t.Errorf("ch1 второе событие: %d", got)
	}
	if got := (<-ch2).GetEventId(); got != 200 {
		t.Errorf("ch2 событие: %d", got)
	}
	select {
	case e := <-ch2:
		t.Errorf("ch2 не должен получать чужие события, получил %d", e.GetEventId())
	default:
	}
}

func TestCancel_ClosesChannelAndCleansUp(t *testing.T) {
	h := newHub()
	ch, cancel := h.Subscribe(7)

	cancel()
	if _, ok := <-ch; ok {
		t.Error("канал должен быть закрыт после отписки")
	}
	if n := h.Subscribers(); n != 0 {
		t.Errorf("после отписки подписчиков %d, ожидали 0", n)
	}
	cancel() // повторная отписка безопасна (sync.Once)

	// Публикация после отписки не должна паниковать (send on closed channel).
	h.Publish([]*eventsv1.ClickEvent{ev(7, 1)})
}

func TestPublish_SlowSubscriberDropsNotBlocks(t *testing.T) {
	h := newHub()
	_, cancel := h.Subscribe(1) // канал никто не читает
	defer cancel()

	// Публикуем больше ёмкости буфера: Publish обязан вернуться, не зависнув.
	events := make([]*eventsv1.ClickEvent, subscriberBuffer+10)
	for i := range events {
		events[i] = ev(1, int64(i))
	}
	done := make(chan struct{})
	go func() { h.Publish(events); close(done) }()
	<-done // если Publish блокируется — тест упадёт по таймауту пакета
}

func TestConcurrentSubscribeUnsubscribePublish(t *testing.T) {
	h := newHub()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func(linkID int64) {
			defer wg.Done()
			for range 100 {
				ch, cancel := h.Subscribe(linkID)
				_ = ch
				cancel()
			}
		}(int64(i % 3))
		go func() {
			defer wg.Done()
			for j := range 100 {
				h.Publish([]*eventsv1.ClickEvent{ev(int64(j%3), int64(j))})
			}
		}()
	}
	wg.Wait()
	if n := h.Subscribers(); n != 0 {
		t.Errorf("после всех отписок подписчиков %d, ожидали 0", n)
	}
}

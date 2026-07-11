// Package grpcapi — реализация analytics.v1.AnalyticsService.
//
// GetLinkStats читает готовые часовые агрегаты; StreamLiveClicks вешает
// подписку на fan-out хаб и пушит события, пока клиент держит стрим.
package grpcapi

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	analyticsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/analytics/v1"
	eventsv1 "github.com/1RAFTIK1/linkpulse-contracts/gen/go/events/v1"

	"github.com/1RAFTIK1/linkpulse-analytics/internal/storage"
)

// Stats — источник агрегатов (Postgres).
type Stats interface {
	GetLinkStats(ctx context.Context, linkID int64, from, to time.Time) (int64, []storage.HourlyBucket, error)
}

// Subscriber — источник live-подписок (fan-out хаб).
type Subscriber interface {
	Subscribe(linkID int64) (<-chan *eventsv1.ClickEvent, func())
}

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	stats Stats
	hub   Subscriber
	log   *slog.Logger
}

func NewServer(stats Stats, hub Subscriber, log *slog.Logger) *Server {
	return &Server{stats: stats, hub: hub, log: log}
}

// GetLinkStats — агрегированная статистика за период.
func (s *Server) GetLinkStats(ctx context.Context, req *analyticsv1.GetLinkStatsRequest) (*analyticsv1.GetLinkStatsResponse, error) {
	if req.GetLinkId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "link_id обязателен")
	}

	// Дефолт периода: последние 24 часа, если границы не заданы.
	to := time.Now().UTC()
	if req.GetTo() != nil {
		to = req.GetTo().AsTime()
	}
	from := to.Add(-24 * time.Hour)
	if req.GetFrom() != nil {
		from = req.GetFrom().AsTime()
	}
	if from.After(to) {
		return nil, status.Error(codes.InvalidArgument, "from позже to")
	}

	total, buckets, err := s.stats.GetLinkStats(ctx, req.GetLinkId(), from, to)
	if err != nil {
		s.log.ErrorContext(ctx, "get link stats", "link_id", req.GetLinkId(), "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	resp := &analyticsv1.GetLinkStatsResponse{
		LinkId:      req.GetLinkId(),
		TotalClicks: total,
	}
	for _, b := range buckets {
		resp.HourlyBreakdown = append(resp.HourlyBreakdown, &analyticsv1.HourlyBucket{
			Hour:       timestamppb.New(b.Hour),
			ClickCount: b.Count,
		})
	}
	return resp, nil
}

// StreamLiveClicks — server-streaming живых кликов по ссылке.
// Стрим живёт, пока клиент не отменит его (или сервер не остановится):
// ctx стрима отменяется в обоих случаях, отписка от хаба гарантирована.
func (s *Server) StreamLiveClicks(req *analyticsv1.StreamLiveClicksRequest, stream analyticsv1.AnalyticsService_StreamLiveClicksServer) error {
	if req.GetLinkId() == 0 {
		return status.Error(codes.InvalidArgument, "link_id обязателен")
	}

	events, cancel := s.hub.Subscribe(req.GetLinkId())
	defer cancel()

	s.log.Info("live-стрим открыт", "link_id", req.GetLinkId())
	defer s.log.Info("live-стрим закрыт", "link_id", req.GetLinkId())

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return nil // клиент отключился или сервер останавливается
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err // обрыв соединения — выходим, defer отпишет от хаба
			}
		}
	}
}

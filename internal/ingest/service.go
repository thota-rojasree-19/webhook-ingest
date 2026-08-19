// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// LoadAccountStats populates the in-memory cache from Postgres.
func (s *Service) LoadAccountStats(ctx context.Context) error {
	allStats, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	for accountID, st := range allStats {
		s.cache.Set(accountID, stats.AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		})
	}
	return nil
}

// Stats returns the cached totals for an account. If absent in cache, it loads from Postgres.
func (s *Service) Stats(accountID string) stats.AccountStats {
	st := s.cache.Get(accountID)
	if st.CallCount == 0 && st.TotalDurationSec == 0 {
		dbStats, err := s.store.AccountStats(context.Background(), accountID)
		if err == nil && (dbStats.CallCount > 0 || dbStats.TotalDurationSec > 0) {
			cacheSt := stats.AccountStats{
				CallCount:        dbStats.CallCount,
				TotalDurationSec: dbStats.TotalDurationSec,
			}
			s.cache.Set(accountID, cacheSt)
			return cacheSt
		}
	}
	return st
}


// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEventTx(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx := context.WithoutCancel(ctx)
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Shutdown waits for active background recording tasks to complete or until ctx is done.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RecoverPendingRecordings processes any calls with unprocessed recordings in the background.
func (s *Service) RecoverPendingRecordings(ctx context.Context) error {
	callIDs, err := s.store.PendingRecordings(ctx)
	if err != nil {
		return err
	}
	for _, callID := range callIDs {
		callID := callID
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(context.Background(), store.Event{CallID: callID}); err != nil {
				s.log.Error("recovery process recording failed", "call_id", callID, "err", err)
			}
		}()
	}
	return nil
}

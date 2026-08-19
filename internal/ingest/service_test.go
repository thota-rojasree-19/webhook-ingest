package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	const concurrentCount = 50
	errCh := make(chan error, concurrentCount)
	startSig := make(chan struct{})

	for i := 0; i < concurrentCount; i++ {
		go func() {
			<-startSig
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				errCh <- err
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("status code %d", resp.StatusCode)
				return
			}
			errCh <- nil
		}()
	}

	close(startSig)

	for i := 0; i < concurrentCount; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent request failed: %v", err)
		}
	}

	var eventCount int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("query event count: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("stored %d copies of %s, want 1", eventCount, eventID)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("query account stats: %v", err)
	}
	if stats.CallCount != 1 {
		t.Fatalf("got CallCount=%d, want 1", stats.CallCount)
	}
	if stats.TotalDurationSec != 143 {
		t.Fatalf("got TotalDurationSec=%d, want 143", stats.TotalDurationSec)
	}
}

func TestConcurrentIngestDirect(t *testing.T) {
	st := testutil.NewStore(t)
	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := stats.NewCache()
	svc := ingest.New(st, cache, rdb, log)

	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  100,
		RecordingURL: "https://example.com/rec.wav",
		OccurredAt:   time.Now(),
	}

	const concurrentCount = 30
	var wg sync.WaitGroup
	startSig := make(chan struct{})

	for i := 0; i < concurrentCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startSig
			_ = svc.Ingest(ctx, evt)
		}()
	}

	close(startSig)
	wg.Wait()

	var eventCount int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("query event count: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("stored %d copies of %s, want 1", eventCount, eventID)
	}

	dbStats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("query account stats: %v", err)
	}
	if dbStats.CallCount != 1 {
		t.Fatalf("DB CallCount=%d, want 1", dbStats.CallCount)
	}
	if dbStats.TotalDurationSec != 100 {
		t.Fatalf("DB TotalDurationSec=%d, want 100", dbStats.TotalDurationSec)
	}

	memStats := cache.Get(accountID)
	if memStats.CallCount != 1 {
		t.Fatalf("Cache CallCount=%d, want 1", memStats.CallCount)
	}
	if memStats.TotalDurationSec != 100 {
		t.Fatalf("Cache TotalDurationSec=%d, want 100", memStats.TotalDurationSec)
	}
}



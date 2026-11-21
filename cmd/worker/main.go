package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cladder/ke-events/internal/broker"
	"github.com/cladder/ke-events/internal/cache"
	"github.com/cladder/ke-events/internal/config"
	"github.com/cladder/ke-events/internal/crawler"
	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/domain"
	"github.com/cladder/ke-events/internal/resilience"
	"github.com/cladder/ke-events/internal/storage"
	"github.com/cladder/ke-events/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := config.Load()
	log := telemetry.NewLogger("worker-" + cfg.InstanceID)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	shutdownTrace := telemetry.InitTracer(ctx, "ke-events-worker", cfg.OTELEndpoint)
	defer func() { _ = shutdownTrace(context.Background()) }()

	store, err := db.New(ctx, cfg.DatabaseURL, cfg.DatabaseRead)
	if err != nil {
		log.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	rdb := cache.New(cfg.RedisAddr)
	defer rdb.Close()

	brk, err := broker.New(cfg.KafkaBrokers)
	if err != nil {
		log.Error("kafka connect failed", "err", err)
		os.Exit(1)
	}
	defer brk.Close()
	_ = brk.EnsureTopics(ctx)

	media, err := storage.NewMediaStore(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOPublicURL, store)
	if err != nil {
		log.Warn("minio init failed, continuing without image mirroring", "err", err)
	}

	// Metrics & Health endpoint for Prometheus
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		_ = http.ListenAndServe(cfg.MetricsAddr, mux)
	}()

	// Seed the 10 verified Kenyan ticketer sources
	for _, s := range crawler.SeededSources() {
		src := s
		if err := store.UpsertSource(ctx, &src); err != nil {
			log.Warn("seed source error", "slug", src.Slug, "err", err)
		}
	}

	ring := resilience.NewConsistentHashRing(cfg.WorkerShards, 32)
	breakers := &sync.Map{}

	// 1. Outbox Relay -> Kafka + Redis Pub/Sub
	go runOutboxRelay(ctx, store, brk, rdb, log)

	// 2. Kafka Consumer for dynamic crawl.jobs
	go runCrawlConsumer(ctx, brk, store, rdb, media, breakers, log)

	// 3. Kafka Consumer for events.discovered -> Webhook Dispatcher + DLQ
	go runWebhookConsumer(ctx, brk, store, log)

	// 4. Periodic Sharded Crawler Loop
	crawlAll := func() {
		sources, err := store.ListSources(ctx)
		if err != nil {
			log.Error("list sources failed", "err", err)
			return
		}
		var wg sync.WaitGroup
		sem := make(chan struct{}, 4) // Bulkhead: max 4 concurrent crawls per worker
		for _, src := range sources {
			if !src.IsActive {
				continue
			}
			// Consistent Hashing shard ownership check
			owner := ring.GetNode(src.Domain)
			if owner != "" && owner != cfg.InstanceID {
				continue
			}
			wg.Add(1)
			go func(s domain.Source) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				processSource(ctx, s, store, rdb, media, breakers, log)
			}(src)
		}
		wg.Wait()
	}

	log.Info("worker started", "instance", cfg.InstanceID, "shards", cfg.WorkerShards)
	crawlAll()

	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			crawlAll()
		}
	}
}

func processSource(ctx context.Context, src domain.Source, store *db.Store, rdb *cache.Client, media *storage.MediaStore, breakers *sync.Map, log *slog.Logger) {
	token, ok, err := rdb.AcquireLock(ctx, "crawl:"+src.Slug, 2*time.Minute)
	if err != nil || !ok {
		return
	}
	defer rdb.ReleaseLock(ctx, "crawl:"+src.Slug, token)

	cbAny, _ := breakers.LoadOrStore(src.Domain, resilience.NewCircuitBreaker(3, 5*time.Minute))
	cb := cbAny.(*resilience.CircuitBreaker)
	if err := cb.Allow(); err != nil {
		telemetry.CircuitBreakerState.WithLabelValues(src.Domain).Set(2)
		return
	}

	var res crawler.CrawlResult
	err = resilience.RetryWithBackoff(ctx, 2, 1*time.Second, func() error {
		var cErr error
		res, cErr = crawler.GetAdapter(src.AdapterType).Crawl(ctx, src)
		return cErr
	})
	if err != nil {
		st := cb.RecordFailure()
		telemetry.CircuitBreakerState.WithLabelValues(src.Domain).Set(float64(st))
		_ = store.UpdateSourceHealth(ctx, src.ID, st.String(), src.ConsecutiveFailures+1, err.Error())
		log.Warn("source crawl failed", "source", src.Slug, "err", err)
		return
	}

	cb.RecordSuccess()
	telemetry.CircuitBreakerState.WithLabelValues(src.Domain).Set(0)

	// Save site metadata (title, description, logo) + mirror logo to MinIO
	if res.SiteMeta.Title != "" || res.SiteMeta.LogoURL != "" {
		src.SiteTitle = res.SiteMeta.Title
		src.SiteDescription = res.SiteMeta.Description
		src.LogoURL = res.SiteMeta.LogoURL
		if media != nil && src.LogoURL != "" {
			if sURL, mErr := media.MirrorImage(ctx, src.LogoURL); mErr == nil && sURL != "" {
				src.LogoStorageURL = sURL
			}
		}
		_ = store.UpsertSource(ctx, &src)
	}

	saved := 0
	for i := range res.Events {
		ev := &res.Events[i]
		ev.SourceID = src.ID
		if media != nil && ev.MainPhotoURL != "" {
			if sURL, mErr := media.MirrorImage(ctx, ev.MainPhotoURL); mErr == nil && sURL != "" {
				ev.MainPhotoStorageURL = sURL
			}
		}
		if err := store.UpsertEventTx(ctx, ev); err != nil {
			telemetry.EventsIngestedTotal.WithLabelValues(src.Slug, "invalid").Inc()
			continue
		}
		saved++
		telemetry.EventsIngestedTotal.WithLabelValues(src.Slug, "ok").Inc()
	}

	_ = store.UpdateSourceHealth(ctx, src.ID, "closed", 0, "")
	if saved > 0 {
		rdb.InvalidatePrefix(ctx, "events:")
		rdb.InvalidatePrefix(ctx, "sources:")
	}
	log.Info("source crawled", "source", src.Slug, "discovered", len(res.Events), "saved", saved)
}

func runOutboxRelay(ctx context.Context, store *db.Store, brk *broker.Client, rdb *cache.Client, log *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, err := store.FetchUnpublishedOutbox(ctx, 50)
			if err != nil || len(rows) == 0 {
				continue
			}
			for _, r := range rows {
				if err := brk.Publish(ctx, broker.TopicEventsDiscovered, r.ID.String(), r.Payload); err != nil {
					log.Warn("outbox kafka publish failed", "err", err)
					break
				}
				_ = rdb.PublishEvent(ctx, r.Payload)
				_ = store.MarkOutboxPublished(ctx, r.ID)
				telemetry.OutboxPublishedTotal.Inc()
			}
		}
	}
}

func runCrawlConsumer(ctx context.Context, brk *broker.Client, store *db.Store, rdb *cache.Client, media *storage.MediaStore, breakers *sync.Map, log *slog.Logger) {
	cl, err := brk.NewConsumer("ke-crawler-group", broker.TopicCrawlJobs)
	if err != nil {
		return
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var src domain.Source
			if json.Unmarshal(rec.Value, &src) == nil {
				processSource(ctx, src, store, rdb, media, breakers, log)
			}
		})
		_ = cl.CommitUncommittedOffsets(ctx)
	}
}

func runWebhookConsumer(ctx context.Context, brk *broker.Client, store *db.Store, log *slog.Logger) {
	cl, err := brk.NewConsumer("ke-webhook-group", broker.TopicEventsDiscovered)
	if err != nil {
		return
	}
	defer cl.Close()
	hc := &http.Client{Timeout: 8 * time.Second}

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		webhooks, _ := store.ListActiveWebhooks(ctx)
		if len(webhooks) == 0 {
			_ = cl.CommitUncommittedOffsets(ctx)
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			for _, wh := range webhooks {
				err := resilience.RetryWithBackoff(ctx, 3, 500*time.Millisecond, func() error {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, wh.TargetURL, bytes.NewReader(rec.Value))
					req.Header.Set("Content-Type", "application/json")
					mac := hmac.New(sha256.New, []byte(wh.Secret))
					mac.Write(rec.Value)
					req.Header.Set("X-KE-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
					resp, err := hc.Do(req)
					if err != nil {
						return err
					}
					resp.Body.Close()
					if resp.StatusCode >= 300 {
						return http.ErrHandlerTimeout
					}
					return nil
				})
				if err != nil {
					_ = brk.Publish(ctx, broker.TopicEventsDLQ, wh.ID.String(), rec.Value)
					telemetry.DLQMessagesTotal.Inc()
					log.Warn("webhook failed, routed to DLQ", "webhook", wh.TargetURL)
				}
			}
		})
		_ = cl.CommitUncommittedOffsets(ctx)
	}
}

package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cladder/ke-events/internal/broker"
	"github.com/cladder/ke-events/internal/cache"
	"github.com/cladder/ke-events/internal/crawler"
	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/domain"
	"github.com/cladder/ke-events/internal/resilience"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServer struct {
	Store      *db.Store
	Cache      *cache.Client
	Broker     *broker.Client
	JWTSecret  string
	InstanceID string
}

func (s *HTTPServer) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(MetricsMiddleware)
	r.Use(RateLimitMiddleware(s.Cache, 120, time.Minute))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "instance": s.InstanceID})
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "db down"})
			return
		}
		if err := s.Cache.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "redis down"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "instance": s.InstanceID})
	})
	r.Handle("/metrics", promhttp.Handler())

	// URL Shortener Redirect (/e/{shortCode})
	r.Get("/e/{code}", s.handleShortRedirect)

	// OpenAPI spec
	r.Get("/openapi.json", s.handleOpenAPI)

	// GraphQL
	if gql, err := NewGraphQLHandler(s.Store); err == nil {
		r.Post("/graphql", gql)
	}

	r.Route("/api/v1", func(v1 chi.Router) {
		v1.Post("/auth/token", s.handleIssueToken)
		v1.Get("/counties", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"counties": domain.KenyanCounties, "count": len(domain.KenyanCounties)})
		})
		v1.Get("/sources", s.handleListSources)
		v1.Get("/events", s.handleListEvents)
		v1.Get("/events/{id}", s.handleGetEvent)
		v1.Get("/stream", s.handleSSEStream)
		v1.Get("/ws", s.handleWebSocket)

		// Protected write endpoints (RBAC: contributor or admin)
		v1.Group(func(priv chi.Router) {
			priv.Use(RequireRole(s.JWTSecret, "contributor", "admin"))
			priv.Post("/sources", s.handleRegisterSource)
			priv.Post("/ingest", s.handleIngestEndpoint)
			priv.Post("/webhooks", s.handleCreateWebhook)
		})
	})

	return r
}

func (s *HTTPServer) handleIssueToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Subject string `json:"subject"`
		Role    string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Subject == "" {
		req.Subject = "guest"
	}
	if req.Role != "admin" && req.Role != "contributor" {
		req.Role = "contributor"
	}
	tok, err := IssueJWT(s.JWTSecret, req.Subject, req.Role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": tok, "role": req.Role})
}

func (s *HTTPServer) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := cache.GetOrLoad(r.Context(), s.Cache, "sources:all", 30*time.Second, func(ctx context.Context) ([]domain.Source, error) {
		return s.Store.ListSources(ctx)
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": sources, "count": len(sources)})
}

func (s *HTTPServer) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	county := q.Get("county")
	search := q.Get("q")
	source := q.Get("source")
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	cacheKey := fmt.Sprintf("events:c=%s:q=%s:s=%s:l=%d:o=%d", county, search, source, limit, offset)
	type listResp struct {
		Events []domain.CanonicalEvent `json:"events"`
		Total  int64                   `json:"total"`
	}
	resp, err := cache.GetOrLoad(r.Context(), s.Cache, cacheKey, 20*time.Second, func(ctx context.Context) (listResp, error) {
		evs, total, err := s.Store.ListEvents(ctx, db.EventFilter{
			County: county, Query: search, Source: source, Limit: limit, Offset: offset,
		})
		return listResp{Events: evs, Total: total}, err
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("X-Served-By", s.InstanceID)
	w.Header().Set("X-Powered-By", "Events Aggregator by Maina")
	writeJSON(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ev, err := cache.GetOrLoad(r.Context(), s.Cache, "event:"+id, 60*time.Second, func(ctx context.Context) (*domain.CanonicalEvent, error) {
		return s.Store.GetEvent(ctx, id)
	})
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "event not found"})
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *HTTPServer) handleShortRedirect(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	ev, err := s.Store.GetEvent(r.Context(), code)
	if err != nil || ev.EventURL == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "short link not found"})
		return
	}
	http.Redirect(w, r, ev.EventURL, http.StatusFound)
}

func (s *HTTPServer) handleRegisterSource(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		EndpointURL string `json:"endpoint_url"`
		AdapterType string `json:"adapter_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EndpointURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "endpoint_url is required"})
		return
	}
	if err := resilience.ValidateExternalURL(req.EndpointURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	u, _ := url.Parse(req.EndpointURL)
	if req.Name == "" {
		req.Name = u.Host
	}
	if req.AdapterType == "" {
		req.AdapterType = "generic_endpoint"
	}
	src := &domain.Source{
		Slug:        strings.ReplaceAll(strings.ToLower(u.Host), ".", "-"),
		Name:        req.Name,
		Domain:      u.Host,
		EndpointURL: req.EndpointURL,
		AdapterType: req.AdapterType,
		IsActive:    true,
	}
	if body, _, err := crawler.FetchURL(r.Context(), "https://"+u.Host); err == nil {
		meta := crawler.ExtractSiteMetadata(body, "https://"+u.Host)
		src.SiteTitle = meta.Title
		src.SiteDescription = meta.Description
		src.LogoURL = meta.LogoURL
	}
	if err := s.Store.UpsertSource(r.Context(), src); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.Broker != nil {
		b, _ := json.Marshal(src)
		_ = s.Broker.Publish(r.Context(), broker.TopicCrawlJobs, src.Slug, b)
	}
	claims := GetClaims(r.Context())
	s.Store.RecordAuditLog(r.Context(), claims.Sub, claims.Role, "register_source", src.EndpointURL, r.RemoteAddr, nil)
	s.Cache.InvalidatePrefix(r.Context(), "sources:")
	writeJSON(w, http.StatusAccepted, src)
}

func (s *HTTPServer) handleIngestEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EndpointURL string `json:"endpoint_url"`
		AdapterType string `json:"adapter_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EndpointURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "endpoint_url is required"})
		return
	}
	if err := resilience.ValidateExternalURL(req.EndpointURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	u, _ := url.Parse(req.EndpointURL)
	src := &domain.Source{
		Slug:        "custom-" + strings.ReplaceAll(strings.ToLower(u.Host), ".", "-"),
		Name:        u.Host,
		Domain:      u.Host,
		EndpointURL: req.EndpointURL,
		AdapterType: req.AdapterType,
		IsActive:    true,
	}
	if src.AdapterType == "" {
		src.AdapterType = "generic_endpoint"
	}
	_ = s.Store.UpsertSource(r.Context(), src)

	res, err := crawler.GetAdapter(src.AdapterType).Crawl(r.Context(), *src)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	var saved []domain.CanonicalEvent
	for i := range res.Events {
		res.Events[i].SourceID = src.ID
		if s.Store.UpsertEventTx(r.Context(), &res.Events[i]) == nil {
			saved = append(saved, res.Events[i])
		}
	}
	s.Cache.InvalidatePrefix(r.Context(), "events:")
	claims := GetClaims(r.Context())
	s.Store.RecordAuditLog(r.Context(), claims.Sub, claims.Role, "ingest_endpoint", req.EndpointURL, r.RemoteAddr, map[string]any{"count": len(saved)})
	writeJSON(w, http.StatusOK, map[string]any{"ingested": len(saved), "events": saved})
}

func (s *HTTPServer) handleCreateWebhook(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TargetURL    string `json:"target_url"`
		CountyFilter string `json:"county_filter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TargetURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "target_url is required"})
		return
	}
	if err := resilience.ValidateExternalURL(req.TargetURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	sec := make([]byte, 16)
	_, _ = rand.Read(sec)
	sub, err := s.Store.CreateWebhook(r.Context(), req.TargetURL, hex.EncodeToString(sec), req.CountyFilter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

func (s *HTTPServer) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	sub := s.Cache.SubscribeEvents(r.Context())
	defer sub.Close()
	ch := sub.Channel()

	_, _ = fmt.Fprintf(w, "event: connected\ndata: {\"instance\":\"%s\"}\n\n", s.InstanceID)
	flusher.Flush()

	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case msg, ok := <-ch:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "event: event_discovered\ndata: %s\n\n", msg.Payload)
			flusher.Flush()
		}
	}
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *HTTPServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	sub := s.Cache.SubscribeEvents(r.Context())
	defer sub.Close()
	ch := sub.Channel()

	_ = conn.WriteJSON(map[string]string{"type": "connected", "instance": s.InstanceID})

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload)); err != nil {
				return
			}
		}
	}
}

func (s *HTTPServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	spec := map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   "Events Aggregator by Maina — Kenyan Events API",
			"version": "1.0.0",
			"contact": map[string]string{
				"name": "Maina",
				"url":  "https://github.com/tensorGT/kenya-events-aggregator",
			},
		},
		"paths": map[string]any{
			"/api/v1/events":     map[string]any{"get": map[string]string{"summary": "List & search Kenyan events"}},
			"/api/v1/sources":    map[string]any{"get": map[string]string{"summary": "List ticketer sources"}, "post": map[string]string{"summary": "Register a new ticketer endpoint"}},
			"/api/v1/ingest":     map[string]any{"post": map[string]string{"summary": "Immediately ingest an endpoint"}},
			"/api/v1/stream":     map[string]any{"get": map[string]string{"summary": "SSE real-time event stream"}},
			"/api/v1/webhooks":   map[string]any{"post": map[string]string{"summary": "Subscribe to event webhooks"}},
			"/api/v1/counties":   map[string]any{"get": map[string]string{"summary": "List all 47 Kenyan counties"}},
			"/graphql":           map[string]any{"post": map[string]string{"summary": "GraphQL query endpoint"}},
		},
	}
	writeJSON(w, http.StatusOK, spec)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

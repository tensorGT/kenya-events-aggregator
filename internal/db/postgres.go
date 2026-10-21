package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cladder/ke-events/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Primary *pgxpool.Pool
	Replica *pgxpool.Pool
}

func New(ctx context.Context, primaryURL, replicaURL string) (*Store, error) {
	pCfg, err := pgxpool.ParseConfig(primaryURL)
	if err != nil {
		return nil, fmt.Errorf("parse primary db: %w", err)
	}
	pCfg.MaxConns = 25
	pPool, err := pgxpool.NewWithConfig(ctx, pCfg)
	if err != nil {
		return nil, fmt.Errorf("connect primary db: %w", err)
	}

	rPool := pPool
	if replicaURL != "" && replicaURL != primaryURL {
		rCfg, err := pgxpool.ParseConfig(replicaURL)
		if err == nil {
			rCfg.MaxConns = 25
			if rp, err := pgxpool.NewWithConfig(ctx, rCfg); err == nil {
				rPool = rp
			}
		}
	}

	return &Store{Primary: pPool, Replica: rPool}, nil
}

func (s *Store) Close() {
	s.Primary.Close()
	if s.Replica != s.Primary {
		s.Replica.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.Primary.Ping(ctx)
}

// UpsertSource creates or updates a ticketer source and its site metadata (title, description, logo).
func (s *Store) UpsertSource(ctx context.Context, src *domain.Source) error {
	if src.ID == uuid.Nil {
		src.ID = uuid.New()
	}
	err := s.Primary.QueryRow(ctx, `
		INSERT INTO sources (id, slug, name, domain, endpoint_url, adapter_type, site_title, site_description, logo_url, logo_storage_url, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (slug) DO UPDATE SET
			endpoint_url = EXCLUDED.endpoint_url,
			adapter_type = EXCLUDED.adapter_type,
			site_title = CASE WHEN EXCLUDED.site_title <> '' THEN EXCLUDED.site_title ELSE sources.site_title END,
			site_description = CASE WHEN EXCLUDED.site_description <> '' THEN EXCLUDED.site_description ELSE sources.site_description END,
			logo_url = CASE WHEN EXCLUDED.logo_url <> '' THEN EXCLUDED.logo_url ELSE sources.logo_url END,
			logo_storage_url = CASE WHEN EXCLUDED.logo_storage_url <> '' THEN EXCLUDED.logo_storage_url ELSE sources.logo_storage_url END,
			updated_at = NOW()
		RETURNING id, created_at
	`, src.ID, src.Slug, src.Name, src.Domain, src.EndpointURL, src.AdapterType, src.SiteTitle, src.SiteDescription, src.LogoURL, src.LogoStorageURL, src.IsActive).
		Scan(&src.ID, &src.CreatedAt)
	return err
}

func (s *Store) ListSources(ctx context.Context) ([]domain.Source, error) {
	rows, err := s.Replica.Query(ctx, `
		SELECT id, slug, name, domain, endpoint_url, adapter_type, site_title, site_description, logo_url, logo_storage_url,
		       is_active, circuit_state, consecutive_failures, last_crawled_at, last_error, created_at
		FROM sources ORDER BY name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Source
	for rows.Next() {
		var src domain.Source
		if err := rows.Scan(
			&src.ID, &src.Slug, &src.Name, &src.Domain, &src.EndpointURL, &src.AdapterType,
			&src.SiteTitle, &src.SiteDescription, &src.LogoURL, &src.LogoStorageURL,
			&src.IsActive, &src.CircuitState, &src.ConsecutiveFailures, &src.LastCrawledAt, &src.LastError, &src.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSourceHealth(ctx context.Context, sourceID uuid.UUID, circuitState string, failures int, lastErr string) error {
	_, err := s.Primary.Exec(ctx, `
		UPDATE sources SET circuit_state=$2, consecutive_failures=$3, last_error=$4, last_crawled_at=NOW(), updated_at=NOW()
		WHERE id=$1
	`, sourceID, circuitState, failures, lastErr)
	return err
}

// UpsertEventTx atomically saves an event, its schedules, ticket tiers, and an Outbox message.
func (s *Store) UpsertEventTx(ctx context.Context, ev *domain.CanonicalEvent) error {
	if err := ev.Validate(); err != nil {
		return err
	}
	if ev.ID == uuid.Nil {
		ev.ID = uuid.New()
	}

	photosJSON, _ := json.Marshal(ev.Photos)
	itineraryJSON, _ := json.Marshal(ev.Itinerary)
	expectJSON, _ := json.Marshal(ev.WhatToExpect)
	rawJSON, _ := json.Marshal(ev.RawPayload)

	tx, err := s.Primary.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Cross-ticketer deduplication: match exact content_hash OR high trigram similarity in same county & date
	var existingID uuid.UUID
	var firstDate string
	if len(ev.Schedules) > 0 {
		firstDate = ev.Schedules[0].Date
	}
	err = tx.QueryRow(ctx, `
		SELECT e.id FROM events e
		LEFT JOIN event_schedules s ON s.event_id = e.id
		WHERE e.content_hash = $1
		   OR (LOWER(e.county) = LOWER($2) AND s.event_date = NULLIF($3, '')::date AND similarity(e.title, $4) > 0.78)
		LIMIT 1
	`, ev.ContentHash, ev.County, firstDate, ev.Title).Scan(&existingID)
	if err == nil && existingID != uuid.Nil {
		// Merge into existing canonical event from another ticketer
		err = tx.QueryRow(ctx, `
			UPDATE events SET
				description = CASE WHEN length(EXCLUDED_DESC.v) > length(events.description) THEN EXCLUDED_DESC.v ELSE events.description END,
				google_maps_link = CASE WHEN events.google_maps_link = '' THEN $2 ELSE events.google_maps_link END,
				main_photo_storage_url = CASE WHEN events.main_photo_storage_url = '' THEN $3 ELSE events.main_photo_storage_url END,
				itinerary = CASE WHEN jsonb_array_length(events.itinerary) = 0 THEN $4 ELSE events.itinerary END,
				what_to_expect = CASE WHEN jsonb_array_length(events.what_to_expect) = 0 THEN $5 ELSE events.what_to_expect END,
				updated_at = NOW()
			FROM (SELECT $6::text AS v) AS EXCLUDED_DESC
			WHERE id = $1
			RETURNING id, short_code, created_at, updated_at
		`, existingID, ev.GoogleMapsLink, ev.MainPhotoStorageURL, itineraryJSON, expectJSON, ev.Description).
			Scan(&ev.ID, &ev.ShortCode, &ev.CreatedAt, &ev.UpdatedAt)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO events (id, source_id, external_id, content_hash, short_code, title, description, venue, county,
		                    event_url, google_maps_link, main_photo_url, main_photo_storage_url, photos, itinerary, what_to_expect, raw_payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT (source_id, external_id) DO UPDATE SET
			content_hash = EXCLUDED.content_hash,
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			venue = EXCLUDED.venue,
			county = EXCLUDED.county,
			event_url = EXCLUDED.event_url,
			google_maps_link = CASE WHEN EXCLUDED.google_maps_link <> '' THEN EXCLUDED.google_maps_link ELSE events.google_maps_link END,
			main_photo_url = EXCLUDED.main_photo_url,
			main_photo_storage_url = CASE WHEN EXCLUDED.main_photo_storage_url <> '' THEN EXCLUDED.main_photo_storage_url ELSE events.main_photo_storage_url END,
			photos = EXCLUDED.photos,
			itinerary = EXCLUDED.itinerary,
			what_to_expect = EXCLUDED.what_to_expect,
			raw_payload = EXCLUDED.raw_payload,
			updated_at = NOW()
		RETURNING id, short_code, created_at, updated_at
	`, ev.ID, ev.SourceID, ev.ExternalID, ev.ContentHash, ev.ShortCode, ev.Title, ev.Description, ev.Venue, ev.County,
		ev.EventURL, ev.GoogleMapsLink, ev.MainPhotoURL, ev.MainPhotoStorageURL, photosJSON, itineraryJSON, expectJSON, rawJSON).
		Scan(&ev.ID, &ev.ShortCode, &ev.CreatedAt, &ev.UpdatedAt)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM event_schedules WHERE event_id = $1`, ev.ID); err != nil {
		return err
	}
	for _, sc := range ev.Schedules {
		tz := sc.Timezone
		if tz == "" {
			tz = "Africa/Nairobi"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO event_schedules (event_id, event_date, start_time, end_time, timezone)
			VALUES ($1, $2::date, $3, $4, $5)
		`, ev.ID, sc.Date, sc.StartTime, sc.EndTime, tz); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM ticket_tiers WHERE event_id = $1`, ev.ID); err != nil {
		return err
	}
	for _, t := range ev.TicketTiers {
		cur := t.Currency
		if cur == "" {
			cur = "KES"
		}
		benJSON, _ := json.Marshal(t.Benefits)
		if _, err := tx.Exec(ctx, `
			INSERT INTO ticket_tiers (event_id, name, price, currency, conditions, benefits, availability)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, ev.ID, t.Name, t.Price, cur, t.Conditions, benJSON, t.Availability); err != nil {
			return err
		}
	}

	// Transactional Outbox entry
	outboxPayload, _ := json.Marshal(ev)
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ('event', $1, 'events.discovered', $2)
	`, ev.ID, outboxPayload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

type EventFilter struct {
	County string
	Query  string
	Source string
	Limit  int
	Offset int
}

func (s *Store) ListEvents(ctx context.Context, f EventFilter) ([]domain.CanonicalEvent, int64, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	var args []any
	var where []string
	idx := 1

	if f.County != "" {
		where = append(where, fmt.Sprintf("LOWER(e.county) = LOWER($%d)", idx))
		args = append(args, f.County)
		idx++
	}
	if f.Source != "" {
		where = append(where, fmt.Sprintf("s.slug = $%d", idx))
		args = append(args, f.Source)
		idx++
	}
	if f.Query != "" {
		where = append(where, fmt.Sprintf("(e.search_tsv @@ websearch_to_tsquery('english', $%d) OR e.title ILIKE '%%' || $%d || '%%')", idx, idx))
		args = append(args, f.Query)
		idx++
	}

	whereSQL := ""
	if len(where) > 0 {
		whereSQL = "WHERE " + strings.Join(where, " AND ")
	}

	var total int64
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM events e JOIN sources s ON s.id = e.source_id %s`, whereSQL)
	if err := s.Replica.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, f.Limit, f.Offset)
	querySQL := fmt.Sprintf(`
		SELECT e.id, e.source_id, s.name, e.external_id, e.content_hash, e.short_code,
		       e.title, e.description, e.venue, e.county, e.event_url, e.google_maps_link,
		       e.main_photo_url, e.main_photo_storage_url, e.photos, e.itinerary, e.what_to_expect,
		       e.created_at, e.updated_at
		FROM events e
		JOIN sources s ON s.id = e.source_id
		%s
		ORDER BY e.created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, idx, idx+1)

	rows, err := s.Replica.Query(ctx, querySQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var events []domain.CanonicalEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, 0, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	for i := range events {
		s.hydrateChildren(ctx, &events[i])
	}
	return events, total, nil
}

func (s *Store) GetEvent(ctx context.Context, idOrShort string) (*domain.CanonicalEvent, error) {
	col := "e.short_code"
	if _, err := uuid.Parse(idOrShort); err == nil {
		col = "e.id::text"
	}
	row := s.Replica.QueryRow(ctx, fmt.Sprintf(`
		SELECT e.id, e.source_id, s.name, e.external_id, e.content_hash, e.short_code,
		       e.title, e.description, e.venue, e.county, e.event_url, e.google_maps_link,
		       e.main_photo_url, e.main_photo_storage_url, e.photos, e.itinerary, e.what_to_expect,
		       e.created_at, e.updated_at
		FROM events e
		JOIN sources s ON s.id = e.source_id
		WHERE %s = $1
	`, col), idOrShort)

	ev, err := scanEventRow(row)
	if err != nil {
		return nil, err
	}
	s.hydrateChildren(ctx, &ev)
	return &ev, nil
}

func scanEventRow(row pgx.Row) (domain.CanonicalEvent, error) {
	var ev domain.CanonicalEvent
	var photosB, itinB, expectB []byte
	err := row.Scan(
		&ev.ID, &ev.SourceID, &ev.SourceName, &ev.ExternalID, &ev.ContentHash, &ev.ShortCode,
		&ev.Title, &ev.Description, &ev.Venue, &ev.County, &ev.EventURL, &ev.GoogleMapsLink,
		&ev.MainPhotoURL, &ev.MainPhotoStorageURL, &photosB, &itinB, &expectB,
		&ev.CreatedAt, &ev.UpdatedAt,
	)
	if err != nil {
		return ev, err
	}
	_ = json.Unmarshal(photosB, &ev.Photos)
	_ = json.Unmarshal(itinB, &ev.Itinerary)
	_ = json.Unmarshal(expectB, &ev.WhatToExpect)
	return ev, nil
}

func (s *Store) hydrateChildren(ctx context.Context, ev *domain.CanonicalEvent) {
	sRows, err := s.Replica.Query(ctx, `
		SELECT to_char(event_date, 'YYYY-MM-DD'), start_time, end_time, timezone
		FROM event_schedules WHERE event_id = $1 ORDER BY event_date ASC
	`, ev.ID)
	if err == nil {
		defer sRows.Close()
		for sRows.Next() {
			var sc domain.EventSchedule
			if sRows.Scan(&sc.Date, &sc.StartTime, &sc.EndTime, &sc.Timezone) == nil {
				ev.Schedules = append(ev.Schedules, sc)
			}
		}
	}

	tRows, err := s.Replica.Query(ctx, `
		SELECT name, price::float8, currency, conditions, benefits, availability
		FROM ticket_tiers WHERE event_id = $1 ORDER BY price ASC
	`, ev.ID)
	if err == nil {
		defer tRows.Close()
		for tRows.Next() {
			var t domain.TicketTier
			var benB []byte
			if tRows.Scan(&t.Name, &t.Price, &t.Currency, &t.Conditions, &benB, &t.Availability) == nil {
				_ = json.Unmarshal(benB, &t.Benefits)
				ev.TicketTiers = append(ev.TicketTiers, t)
			}
		}
	}
}

func (s *Store) GetMediaBySHA256(ctx context.Context, sha string) (string, bool) {
	var url string
	err := s.Replica.QueryRow(ctx, `SELECT storage_url FROM media_blobs WHERE sha256 = $1`, sha).Scan(&url)
	return url, err == nil
}

func (s *Store) SaveMediaBlob(ctx context.Context, sha, srcURL, bucket, key, storageURL, ct string, size int64) error {
	_, err := s.Primary.Exec(ctx, `
		INSERT INTO media_blobs (sha256, source_url, bucket, object_key, storage_url, content_type, size_bytes)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (sha256) DO NOTHING
	`, sha, srcURL, bucket, key, storageURL, ct, size)
	return err
}

type OutboxRow struct {
	ID        uuid.UUID
	EventType string
	Payload   []byte
}

func (s *Store) FetchUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := s.Primary.Query(ctx, `
		SELECT id, event_type, payload FROM outbox_events
		WHERE published_at IS NULL ORDER BY created_at ASC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if rows.Scan(&r.ID, &r.EventType, &r.Payload) == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id uuid.UUID) error {
	_, err := s.Primary.Exec(ctx, `UPDATE outbox_events SET published_at = NOW() WHERE id = $1`, id)
	return err
}

type WebhookSub struct {
	ID           uuid.UUID `json:"id"`
	TargetURL    string    `json:"target_url"`
	Secret       string    `json:"secret,omitempty"`
	CountyFilter string    `json:"county_filter,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

func (s *Store) CreateWebhook(ctx context.Context, targetURL, secret, countyFilter string) (*WebhookSub, error) {
	w := &WebhookSub{ID: uuid.New(), TargetURL: targetURL, Secret: secret, CountyFilter: countyFilter}
	err := s.Primary.QueryRow(ctx, `
		INSERT INTO webhooks (id, target_url, secret, county_filter) VALUES ($1,$2,$3,$4) RETURNING created_at
	`, w.ID, w.TargetURL, w.Secret, w.CountyFilter).Scan(&w.CreatedAt)
	return w, err
}

func (s *Store) ListActiveWebhooks(ctx context.Context) ([]WebhookSub, error) {
	rows, err := s.Replica.Query(ctx, `SELECT id, target_url, secret, county_filter, created_at FROM webhooks WHERE is_active = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WebhookSub
	for rows.Next() {
		var w WebhookSub
		if rows.Scan(&w.ID, &w.TargetURL, &w.Secret, &w.CountyFilter, &w.CreatedAt) == nil {
			out = append(out, w)
		}
	}
	return out, rows.Err()
}

func (s *Store) RecordAuditLog(ctx context.Context, actor, role, action, resource, ip string, meta map[string]any) {
	b, _ := json.Marshal(meta)
	_, _ = s.Primary.Exec(ctx, `
		INSERT INTO audit_logs (actor, role, action, resource, ip_address, metadata) VALUES ($1,$2,$3,$4,$5,$6)
	`, actor, role, action, resource, ip, b)
}

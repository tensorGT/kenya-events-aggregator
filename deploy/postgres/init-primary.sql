-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create replication role for Read Replica (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator_pass';
    END IF;
END
$$;

-- 1. Ticketer Sources (with site metadata: title, description, logo_url)
CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    domain TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    adapter_type TEXT NOT NULL DEFAULT 'auto',
    site_title TEXT NOT NULL DEFAULT '',
    site_description TEXT NOT NULL DEFAULT '',
    logo_url TEXT NOT NULL DEFAULT '',
    logo_storage_url TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    circuit_state TEXT NOT NULL DEFAULT 'closed',
    consecutive_failures INT NOT NULL DEFAULT 0,
    last_crawled_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Canonical Events
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    short_code TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    venue TEXT NOT NULL,
    county TEXT NOT NULL,
    event_url TEXT NOT NULL DEFAULT '',
    google_maps_link TEXT NOT NULL DEFAULT '',
    main_photo_url TEXT NOT NULL,
    main_photo_storage_url TEXT NOT NULL DEFAULT '',
    photos JSONB NOT NULL DEFAULT '[]'::jsonb,
    itinerary JSONB NOT NULL DEFAULT '[]'::jsonb,
    what_to_expect JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description, '') || ' ' || coalesce(venue, '') || ' ' || coalesce(county, ''))
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_events_county ON events(county);
CREATE INDEX IF NOT EXISTS idx_events_content_hash ON events(content_hash);
CREATE INDEX IF NOT EXISTS idx_events_short_code ON events(short_code);
CREATE INDEX IF NOT EXISTS idx_events_search_tsv ON events USING GIN(search_tsv);
CREATE INDEX IF NOT EXISTS idx_events_title_trgm ON events USING GIN(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);

-- 3. Event Schedules (Date/Dates + Time)
CREATE TABLE IF NOT EXISTS event_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    event_date DATE NOT NULL,
    start_time TEXT NOT NULL DEFAULT '00:00',
    end_time TEXT NOT NULL DEFAULT '',
    timezone TEXT NOT NULL DEFAULT 'Africa/Nairobi'
);
CREATE INDEX IF NOT EXISTS idx_schedules_event_id ON event_schedules(event_id);
CREATE INDEX IF NOT EXISTS idx_schedules_date ON event_schedules(event_date);

-- 4. Ticket Tiers (Name, Price, Conditions, Benefits)
CREATE TABLE IF NOT EXISTS ticket_tiers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'KES',
    conditions TEXT NOT NULL DEFAULT '',
    benefits JSONB NOT NULL DEFAULT '[]'::jsonb,
    availability TEXT NOT NULL DEFAULT 'InStock'
);
CREATE INDEX IF NOT EXISTS idx_tiers_event_id ON ticket_tiers(event_id);
CREATE INDEX IF NOT EXISTS idx_tiers_price ON ticket_tiers(price);

-- 5. Media Blobs (SHA-256 Content-Addressable Deduplication for MinIO)
CREATE TABLE IF NOT EXISTS media_blobs (
    sha256 TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    storage_url TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. Transactional Outbox Pattern (Guaranteed At-Least-Once Kafka Emission)
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events(created_at) WHERE published_at IS NULL;

-- 7. Webhook Subscriptions (Event Emission to External Services)
CREATE TABLE IF NOT EXISTS webhooks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    target_url TEXT NOT NULL,
    secret TEXT NOT NULL,
    county_filter TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    webhook_id UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status_code INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 8. Security Audit Logs
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    ip_address TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_logs(created_at DESC);

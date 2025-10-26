-- Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- PostgreSQL Schema for NVSentinel DataStore
-- This schema supports both MaintenanceEvent and HealthEvent storage
-- with JSON document storage for flexibility and PostgreSQL benefits

-- Extension for JSON operations and UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ====================================================================
-- MAINTENANCE EVENTS (CSP Health Monitor)
-- ====================================================================

CREATE TABLE maintenance_events (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Core event identification (extracted for indexing)
    event_id VARCHAR(255) UNIQUE NOT NULL,
    csp VARCHAR(50) NOT NULL,
    cluster_name VARCHAR(255) NOT NULL,
    node_name VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    csp_status VARCHAR(50),

    -- Time fields (extracted for efficient querying)
    scheduled_start_time TIMESTAMPTZ,
    actual_end_time TIMESTAMPTZ,
    event_received_timestamp TIMESTAMPTZ NOT NULL,
    last_updated_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Full document storage (JSON)
    document JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for maintenance_events
CREATE INDEX idx_maintenance_events_event_id ON maintenance_events(event_id);
CREATE INDEX idx_maintenance_events_csp_cluster ON maintenance_events(csp, cluster_name);
CREATE INDEX idx_maintenance_events_node_status ON maintenance_events(node_name, status);
CREATE INDEX idx_maintenance_events_status_scheduled ON maintenance_events(status, scheduled_start_time) WHERE scheduled_start_time IS NOT NULL;
CREATE INDEX idx_maintenance_events_status_actual_end ON maintenance_events(status, actual_end_time) WHERE actual_end_time IS NOT NULL;
CREATE INDEX idx_maintenance_events_received_desc ON maintenance_events(csp, cluster_name, event_received_timestamp DESC);
CREATE INDEX idx_maintenance_events_last_updated ON maintenance_events(last_updated_timestamp DESC);
CREATE INDEX idx_maintenance_events_csp_status ON maintenance_events(csp_status) WHERE csp_status IS NOT NULL;

-- GIN index for flexible JSON querying
CREATE INDEX idx_maintenance_events_document_gin ON maintenance_events USING GIN (document);

-- ====================================================================
-- HEALTH EVENTS (Platform Connectors)
-- ====================================================================

CREATE TABLE health_events (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Core event identification (extracted for indexing)
    node_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(100),
    severity VARCHAR(50),
    recommended_action VARCHAR(100),

    -- Status tracking
    node_quarantined VARCHAR(50),
    user_pods_eviction_status VARCHAR(50) DEFAULT 'NotStarted',
    user_pods_eviction_message TEXT,
    fault_remediated BOOLEAN,
    last_remediation_timestamp TIMESTAMPTZ,

    -- Full document storage (JSON)
    document JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for health_events
CREATE INDEX idx_health_events_node_name ON health_events(node_name);
CREATE INDEX idx_health_events_node_type ON health_events(node_name, event_type);
CREATE INDEX idx_health_events_quarantined ON health_events(node_quarantined) WHERE node_quarantined IS NOT NULL;
CREATE INDEX idx_health_events_eviction_status ON health_events(user_pods_eviction_status);
CREATE INDEX idx_health_events_created_desc ON health_events(created_at DESC);
CREATE INDEX idx_health_events_updated_desc ON health_events(updated_at DESC);

-- GIN index for flexible JSON querying
CREATE INDEX idx_health_events_document_gin ON health_events USING GIN (document);

-- ====================================================================
-- CHANGE TRACKING (For polling-based change streams)
-- ====================================================================

CREATE TABLE datastore_changelog (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    operation VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

-- Index for change tracking
CREATE INDEX idx_changelog_unprocessed ON datastore_changelog(changed_at) WHERE processed = FALSE;
CREATE INDEX idx_changelog_table_record ON datastore_changelog(table_name, record_id);

-- ====================================================================
-- RESUME TOKENS (For change stream compatibility)
-- ====================================================================

CREATE TABLE resume_tokens (
    client_name VARCHAR(255) PRIMARY KEY,
    resume_token JSONB NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- ====================================================================
-- TRIGGERS FOR CHANGE TRACKING
-- ====================================================================

-- Function to log changes
CREATE OR REPLACE FUNCTION log_table_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO datastore_changelog (table_name, record_id, operation, old_values)
        VALUES (TG_TABLE_NAME, OLD.id, TG_OP, to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO datastore_changelog (table_name, record_id, operation, old_values, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, TG_OP, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO datastore_changelog (table_name, record_id, operation, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, TG_OP, to_jsonb(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers
CREATE TRIGGER maintenance_events_changes
    AFTER INSERT OR UPDATE OR DELETE ON maintenance_events
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();

CREATE TRIGGER health_events_changes
    AFTER INSERT OR UPDATE OR DELETE ON health_events
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();

-- ====================================================================
-- HELPER FUNCTIONS
-- ====================================================================

-- Function to clean old changelog entries (call periodically)
CREATE OR REPLACE FUNCTION cleanup_changelog(retention_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM datastore_changelog
    WHERE changed_at < NOW() - INTERVAL '1 day' * retention_days
    AND processed = TRUE;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to mark changelog entries as processed
CREATE OR REPLACE FUNCTION mark_changelog_processed(max_id BIGINT)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE datastore_changelog
    SET processed = TRUE
    WHERE id <= max_id AND processed = FALSE;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;
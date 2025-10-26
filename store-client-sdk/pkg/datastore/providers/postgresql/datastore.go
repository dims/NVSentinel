// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	_ "github.com/lib/pq" // PostgreSQL driver
	"k8s.io/klog/v2"
)

// PostgreSQLDataStore implements the DataStore interface for PostgreSQL
type PostgreSQLDataStore struct {
	db                    *sql.DB
	maintenanceEventStore datastore.MaintenanceEventStore
	healthEventStore      datastore.HealthEventStore
}

// NewPostgreSQLDataStore creates a new PostgreSQL datastore
func NewPostgreSQLDataStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	connectionString := buildConnectionString(config.Connection)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// Create tables if they don't exist
	if err := createTables(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	store := &PostgreSQLDataStore{db: db}
	store.maintenanceEventStore = NewPostgreSQLMaintenanceEventStore(db)
	store.healthEventStore = NewPostgreSQLHealthEventStore(db)

	klog.Infof("Successfully connected to PostgreSQL database: %s", config.Connection.Host)

	return store, nil
}

// MaintenanceEventStore returns the maintenance event store
func (p *PostgreSQLDataStore) MaintenanceEventStore() datastore.MaintenanceEventStore {
	return p.maintenanceEventStore
}

// HealthEventStore returns the health event store
func (p *PostgreSQLDataStore) HealthEventStore() datastore.HealthEventStore {
	return p.healthEventStore
}

// Connect initializes the database connection and creates necessary tables
func (p *PostgreSQLDataStore) Connect(ctx context.Context) error {
	// Test connection
	if err := p.db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Create tables if they don't exist
	if err := createTables(ctx, p.db); err != nil {
		return fmt.Errorf("failed to create database schema: %w", err)
	}

	return nil
}

// Ping tests the database connection
func (p *PostgreSQLDataStore) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

// Close closes the database connection
func (p *PostgreSQLDataStore) Close(ctx context.Context) error {
	return p.db.Close()
}

// GetDB returns the underlying database connection for change stream watchers
func (p *PostgreSQLDataStore) GetDB() *sql.DB {
	return p.db
}

// InsertMany inserts multiple documents into the database
func (p *PostgreSQLDataStore) InsertMany(ctx context.Context, documents []interface{}) error {
	if len(documents) == 0 {
		return nil
	}

	// For PostgreSQL, we'll insert into the health_events table
	// This is a simplified implementation - in practice you might want more sophisticated logic
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			klog.Warningf("Failed to rollback transaction: %v", rollbackErr)
		}
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO health_events (document) VALUES ($1)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, doc := range documents {
		jsonDoc, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		_, err = stmt.ExecContext(ctx, jsonDoc)
		if err != nil {
			return fmt.Errorf("failed to insert document: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	klog.V(2).Infof("Successfully inserted %d documents", len(documents))

	return nil
}

// Verify that PostgreSQLDataStore implements the DataStore interface
var _ datastore.DataStore = (*PostgreSQLDataStore)(nil)

// buildConnectionString creates a PostgreSQL connection string
func buildConnectionString(conn datastore.ConnectionConfig) string {
	params := make([]string, 0)

	params = append(params, fmt.Sprintf("host=%s", conn.Host))

	if conn.Port > 0 {
		params = append(params, fmt.Sprintf("port=%d", conn.Port))
	}

	if conn.Database != "" {
		params = append(params, fmt.Sprintf("dbname=%s", conn.Database))
	}

	if conn.Username != "" {
		params = append(params, fmt.Sprintf("user=%s", conn.Username))
	}

	if conn.Password != "" {
		params = append(params, fmt.Sprintf("password=%s", conn.Password))
	}

	if conn.SSLMode != "" {
		params = append(params, fmt.Sprintf("sslmode=%s", conn.SSLMode))
	} else {
		params = append(params, "sslmode=prefer")
	}

	// Add SSL certificate parameters
	if conn.SSLCert != "" {
		params = append(params, fmt.Sprintf("sslcert=%s", conn.SSLCert))
	}

	if conn.SSLKey != "" {
		params = append(params, fmt.Sprintf("sslkey=%s", conn.SSLKey))
	}

	if conn.SSLRootCert != "" {
		params = append(params, fmt.Sprintf("sslrootcert=%s", conn.SSLRootCert))
	}

	// Add extra parameters
	for key, value := range conn.ExtraParams {
		params = append(params, fmt.Sprintf("%s=%s", key, value))
	}

	return strings.Join(params, " ")
}

// createTables creates the necessary tables if they don't exist
func createTables(ctx context.Context, db *sql.DB) error {
	schemas := []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,

		// Maintenance Events Table
		`CREATE TABLE IF NOT EXISTS maintenance_events (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			event_id VARCHAR(255) UNIQUE NOT NULL,
			csp VARCHAR(50) NOT NULL,
			cluster_name VARCHAR(255) NOT NULL,
			node_name VARCHAR(255),
			status VARCHAR(50) NOT NULL,
			csp_status VARCHAR(50),
			scheduled_start_time TIMESTAMPTZ,
			actual_end_time TIMESTAMPTZ,
			event_received_timestamp TIMESTAMPTZ NOT NULL,
			last_updated_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			document JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Health Events Table
		`CREATE TABLE IF NOT EXISTS health_events (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			node_name VARCHAR(255) NOT NULL,
			event_type VARCHAR(100),
			severity VARCHAR(50),
			recommended_action VARCHAR(100),
			node_quarantined VARCHAR(50),
			user_pods_eviction_status VARCHAR(50) DEFAULT 'NotStarted',
			user_pods_eviction_message TEXT,
			fault_remediated BOOLEAN,
			last_remediation_timestamp TIMESTAMPTZ,
			document JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Change tracking table for polling-based change streams
		`CREATE TABLE IF NOT EXISTS datastore_changelog (
			id BIGSERIAL PRIMARY KEY,
			table_name VARCHAR(100) NOT NULL,
			record_id UUID NOT NULL,
			operation VARCHAR(20) NOT NULL,
			old_values JSONB,
			new_values JSONB,
			changed_at TIMESTAMPTZ DEFAULT NOW(),
			processed BOOLEAN DEFAULT FALSE
		)`,

		// Resume tokens table
		`CREATE TABLE IF NOT EXISTS resume_tokens (
			client_name VARCHAR(255) PRIMARY KEY,
			resume_token JSONB NOT NULL,
			last_updated TIMESTAMPTZ DEFAULT NOW()
		)`,
	}

	indexes := []string{
		// Maintenance Events Indexes
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_event_id ` +
			`ON maintenance_events(event_id)`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_csp_cluster ` +
			`ON maintenance_events(csp, cluster_name)`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_node_status ` +
			`ON maintenance_events(node_name, status)`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_status_scheduled ` +
			`ON maintenance_events(status, scheduled_start_time) ` +
			`WHERE scheduled_start_time IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_status_actual_end ` +
			`ON maintenance_events(status, actual_end_time) ` +
			`WHERE actual_end_time IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_received_desc ` +
			`ON maintenance_events(csp, cluster_name, event_received_timestamp DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_events_document_gin ` +
			`ON maintenance_events USING GIN (document)`,

		// Health Events Indexes
		`CREATE INDEX IF NOT EXISTS idx_health_events_node_name ON health_events(node_name)`,
		`CREATE INDEX IF NOT EXISTS idx_health_events_node_type ON health_events(node_name, event_type)`,
		`CREATE INDEX IF NOT EXISTS idx_health_events_quarantined ON health_events(node_quarantined) ` +
			`WHERE node_quarantined IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_health_events_created_desc ON health_events(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_health_events_document_gin ON health_events USING GIN (document)`,

		// Changelog Indexes
		`CREATE INDEX IF NOT EXISTS idx_changelog_unprocessed ON datastore_changelog(changed_at) WHERE processed = FALSE`,
		`CREATE INDEX IF NOT EXISTS idx_changelog_table_record ON datastore_changelog(table_name, record_id)`,
	}

	// Execute schema creation
	for _, schema := range schemas {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	// Execute index creation
	for _, index := range indexes {
		if _, err := db.ExecContext(ctx, index); err != nil {
			klog.Warningf("Failed to create index (may already exist): %v", err)
		}
	}

	// Create change tracking triggers
	if err := createChangeTriggers(ctx, db); err != nil {
		return fmt.Errorf("failed to create change triggers: %w", err)
	}

	klog.Info("Successfully created PostgreSQL tables and indexes")

	return nil
}

// createChangeTriggers creates triggers for change tracking
func createChangeTriggers(ctx context.Context, db *sql.DB) error {
	triggerFunction := `
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
		$$ LANGUAGE plpgsql;`

	triggers := []string{
		triggerFunction,
		`DROP TRIGGER IF EXISTS maintenance_events_changes ON maintenance_events`,
		`CREATE TRIGGER maintenance_events_changes
			AFTER INSERT OR UPDATE OR DELETE ON maintenance_events
			FOR EACH ROW EXECUTE FUNCTION log_table_changes()`,
		`DROP TRIGGER IF EXISTS health_events_changes ON health_events`,
		`CREATE TRIGGER health_events_changes
			AFTER INSERT OR UPDATE OR DELETE ON health_events
			FOR EACH ROW EXECUTE FUNCTION log_table_changes()`,
	}

	for _, trigger := range triggers {
		if _, err := db.ExecContext(ctx, trigger); err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	return nil
}

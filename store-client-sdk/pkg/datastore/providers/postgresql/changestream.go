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
	"strconv"
	"time"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"k8s.io/klog/v2"
)

// PostgreSQLChangeStreamWatcher implements ChangeStreamWatcher for PostgreSQL using polling
type PostgreSQLChangeStreamWatcher struct {
	db           *sql.DB
	clientName   string
	tableName    string
	events       chan datastore.EventWithToken
	stopCh       chan struct{}
	lastEventID  int64
	pollInterval time.Duration
}

// NewPostgreSQLChangeStreamWatcher creates a new PostgreSQL change stream watcher
func NewPostgreSQLChangeStreamWatcher(
	db *sql.DB, clientName string, tableName string,
) *PostgreSQLChangeStreamWatcher {
	return &PostgreSQLChangeStreamWatcher{
		db:           db,
		clientName:   clientName,
		tableName:    tableName,
		events:       make(chan datastore.EventWithToken, 100),
		stopCh:       make(chan struct{}),
		pollInterval: 5 * time.Second, // Default poll interval
	}
}

// Events returns the events channel
func (w *PostgreSQLChangeStreamWatcher) Events() <-chan datastore.EventWithToken {
	return w.events
}

// Start starts the change stream watcher
func (w *PostgreSQLChangeStreamWatcher) Start(ctx context.Context) {
	// Load last processed event ID
	if err := w.loadResumePosition(ctx); err != nil {
		klog.Errorf("Failed to load resume position for client %s: %v", w.clientName, err)
	}

	go w.pollForChanges(ctx)
}

// MarkProcessed marks events as processed up to the given token
func (w *PostgreSQLChangeStreamWatcher) MarkProcessed(ctx context.Context, token []byte) error {
	if len(token) == 0 {
		return nil
	}

	// Token is the event ID as string
	eventIDStr := string(token)

	eventID, err := strconv.ParseInt(eventIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid token format: %w", err)
	}

	// Mark events as processed in changelog
	query := `
		UPDATE datastore_changelog
		SET processed = TRUE
		WHERE id <= $1 AND table_name = $2 AND processed = FALSE
	`

	_, err = w.db.ExecContext(ctx, query, eventID, w.tableName)
	if err != nil {
		return fmt.Errorf("failed to mark events as processed: %w", err)
	}

	// Update resume position
	w.lastEventID = eventID
	if err := w.saveResumePosition(ctx, eventID); err != nil {
		klog.Errorf("Failed to save resume position: %v", err)
	}

	klog.V(3).Infof("Marked events processed up to ID %d for table %s", eventID, w.tableName)

	return nil
}

// Close closes the change stream watcher
func (w *PostgreSQLChangeStreamWatcher) Close(ctx context.Context) error {
	close(w.stopCh)
	close(w.events)

	return nil
}

// pollForChanges polls the changelog table for new changes
func (w *PostgreSQLChangeStreamWatcher) pollForChanges(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			if err := w.fetchNewChanges(ctx); err != nil {
				klog.Errorf("Error fetching changes for table %s: %v", w.tableName, err)
			}
		}
	}
}

// fetchNewChanges fetches new changes from the changelog table
func (w *PostgreSQLChangeStreamWatcher) fetchNewChanges(ctx context.Context) error {
	query := `
		SELECT id, operation, old_values, new_values, changed_at
		FROM datastore_changelog
		WHERE table_name = $1 AND id > $2 AND processed = FALSE
		ORDER BY id ASC
		LIMIT 100
	`

	rows, err := w.db.QueryContext(ctx, query, w.tableName, w.lastEventID)
	if err != nil {
		return fmt.Errorf("failed to query changelog: %w", err)
	}
	defer rows.Close()

	events, err := w.processChangelogRows(rows)
	if err != nil {
		return err
	}

	return w.sendEventsToChannel(ctx, events)
}

// processChangelogRows processes changelog rows and builds events
func (w *PostgreSQLChangeStreamWatcher) processChangelogRows(rows *sql.Rows) ([]datastore.EventWithToken, error) {
	var events []datastore.EventWithToken

	for rows.Next() {
		var (
			id                   int64
			operation            string
			oldValues, newValues sql.NullString
			changedAt            time.Time
		)

		if err := rows.Scan(&id, &operation, &oldValues, &newValues, &changedAt); err != nil {
			return nil, fmt.Errorf("failed to scan changelog row: %w", err)
		}

		event := w.buildEventDocument(id, operation, oldValues, newValues, changedAt)
		token := []byte(fmt.Sprintf("%d", id))

		eventWithToken := datastore.EventWithToken{
			Event:       event,
			ResumeToken: token,
		}

		events = append(events, eventWithToken)
		w.lastEventID = id
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating changelog rows: %w", err)
	}

	return events, nil
}

// buildEventDocument builds an event document from changelog row data
func (w *PostgreSQLChangeStreamWatcher) buildEventDocument(
	id int64,
	operation string,
	oldValues, newValues sql.NullString,
	changedAt time.Time,
) map[string]interface{} {
	event := map[string]interface{}{
		"_id": map[string]interface{}{
			"_data": fmt.Sprintf("%d", id),
		},
		"operationType": mapOperation(operation),
		"clusterTime":   changedAt,
		"fullDocument":  nil,
	}

	w.addDocumentDataToEvent(event, operation, oldValues, newValues)

	return event
}

// addDocumentDataToEvent adds document data to event based on operation type
func (w *PostgreSQLChangeStreamWatcher) addDocumentDataToEvent(
	event map[string]interface{},
	operation string,
	oldValues, newValues sql.NullString,
) {
	switch operation {
	case "INSERT", "UPDATE":
		if newValues.Valid {
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(newValues.String), &doc); err == nil {
				event["fullDocument"] = doc
			}
		}
	case "DELETE":
		if oldValues.Valid {
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(oldValues.String), &doc); err == nil {
				event["fullDocumentBeforeChange"] = doc
			}
		}
	}
}

// sendEventsToChannel sends events to the channel
func (w *PostgreSQLChangeStreamWatcher) sendEventsToChannel(
	ctx context.Context,
	events []datastore.EventWithToken,
) error {
	for _, event := range events {
		select {
		case w.events <- event:
		case <-ctx.Done():
			return ctx.Err()
		case <-w.stopCh:
			return nil
		}
	}

	if len(events) > 0 {
		klog.V(3).Infof("Sent %d change events for table %s", len(events), w.tableName)
	}

	return nil
}

// loadResumePosition loads the last processed event ID from resume tokens table
func (w *PostgreSQLChangeStreamWatcher) loadResumePosition(ctx context.Context) error {
	query := `
		SELECT resume_token FROM resume_tokens
		WHERE client_name = $1
	`

	var tokenJSON []byte

	err := w.db.QueryRowContext(ctx, query, w.clientName).Scan(&tokenJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			w.lastEventID = 0 // Start from beginning
			return nil
		}

		return fmt.Errorf("failed to load resume position: %w", err)
	}

	var token map[string]interface{}
	if err := json.Unmarshal(tokenJSON, &token); err != nil {
		return fmt.Errorf("failed to unmarshal resume token: %w", err)
	}

	if eventIDVal, exists := token["eventID"]; exists {
		if eventIDFloat, ok := eventIDVal.(float64); ok {
			w.lastEventID = int64(eventIDFloat)
		}
	}

	klog.V(2).Infof("Loaded resume position for client %s: %d", w.clientName, w.lastEventID)

	return nil
}

// saveResumePosition saves the resume position to the resume tokens table
func (w *PostgreSQLChangeStreamWatcher) saveResumePosition(ctx context.Context, eventID int64) error {
	token := map[string]interface{}{
		"eventID":   eventID,
		"timestamp": time.Now(),
	}

	tokenJSON, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("failed to marshal resume token: %w", err)
	}

	query := `
		INSERT INTO resume_tokens (client_name, resume_token, last_updated)
		VALUES ($1, $2, NOW())
		ON CONFLICT (client_name)
		DO UPDATE SET resume_token = EXCLUDED.resume_token, last_updated = NOW()
	`

	_, err = w.db.ExecContext(ctx, query, w.clientName, tokenJSON)
	if err != nil {
		return fmt.Errorf("failed to save resume position: %w", err)
	}

	return nil
}

// mapOperation maps PostgreSQL operations to MongoDB-style operation types
func mapOperation(pgOp string) string {
	switch pgOp {
	case "INSERT":
		return "insert"
	case "UPDATE":
		return "update"
	case "DELETE":
		return "delete"
	default:
		return "unknown"
	}
}

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
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

const (
	healthEventsTable = "health_events"
)

// PostgreSQLChangeStreamWatcher implements ChangeStreamWatcher for PostgreSQL using polling
type PostgreSQLChangeStreamWatcher struct {
	db             *sql.DB
	clientName     string
	tableName      string
	events         chan datastore.EventWithToken
	stopCh         chan struct{}
	lastEventID    int64
	mu             sync.RWMutex // Protects lastEventID
	pollInterval   time.Duration
	pipelineFilter *PipelineFilter // Optional filter based on MongoDB-style pipeline
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
		pollInterval: 500 * time.Millisecond, // Default poll interval (reduced for better latency)
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
		slog.Error("Failed to load resume position", "client", w.clientName, "error", err)
	}

	go w.pollForChanges(ctx)
}

// MarkProcessed marks events as processed up to the given token
func (w *PostgreSQLChangeStreamWatcher) MarkProcessed(ctx context.Context, token []byte) error {
	var eventID int64

	if len(token) == 0 {
		// When token is empty, use the current lastEventID (similar to MongoDB's behavior)
		// This handles the common case where event processors pass empty tokens
		// and expect the watcher to track its own position.
		//
		// In MongoDB, an empty token means "use the change stream's current resume token".
		// In PostgreSQL, we use lastEventID which tracks the last event we read from the stream.
		w.mu.RLock()
		eventID = w.lastEventID
		w.mu.RUnlock()

		if eventID == 0 {
			slog.Debug("No events processed yet, skipping MarkProcessed",
				"client", w.clientName)

			return nil
		}

		slog.Debug("Using current stream position for empty token",
			"client", w.clientName,
			"eventID", eventID)
	} else {
		// Token is the event ID as string
		eventIDStr := string(token)

		var err error

		eventID, err = strconv.ParseInt(eventIDStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid token format: %w", err)
		}
	}

	// Mark all events up to and including this eventID as processed
	// This is safe because:
	// 1. The application only calls MarkProcessed after successfully processing an event
	// 2. Events are delivered in order (by ID)
	// 3. If an event is filtered by the pipeline, we never send it, so MarkProcessed is never called for it
	query := `
		UPDATE datastore_changelog
		SET processed = TRUE
		WHERE id <= $1 AND table_name = $2 AND processed = FALSE
	`

	_, err := w.db.ExecContext(ctx, query, eventID, w.tableName)
	if err != nil {
		return fmt.Errorf("failed to mark event as processed: %w", err)
	}

	// Update resume position
	w.mu.Lock()
	w.lastEventID = eventID
	w.mu.Unlock()

	if err := w.saveResumePosition(ctx, eventID); err != nil {
		slog.Error("Failed to save resume position", "error", err)
	}

	slog.Info("Marked events processed",
		"client", w.clientName,
		"eventID", eventID,
		"table", w.tableName)

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

	slog.Info("[CHANGESTREAM-DEBUG] Started polling for changes",
		"client", w.clientName,
		"table", w.tableName,
		"pollInterval", w.pollInterval,
		"lastEventID", w.lastEventID)

	for {
		select {
		case <-ctx.Done():
			slog.Info("[CHANGESTREAM-DEBUG] Context cancelled, stopping polling",
				"client", w.clientName)

			return
		case <-w.stopCh:
			slog.Info("[CHANGESTREAM-DEBUG] Stop channel triggered, stopping polling",
				"client", w.clientName)

			return
		case <-ticker.C:
			slog.Debug("[CHANGESTREAM-DEBUG] Poll tick - fetching new changes",
				"client", w.clientName,
				"lastEventID", w.lastEventID)

			if err := w.fetchNewChanges(ctx); err != nil {
				slog.Error("Error fetching changes", "table", w.tableName, "error", err)
			}
		}
	}
}

// fetchNewChanges fetches new changes from the changelog table
func (w *PostgreSQLChangeStreamWatcher) fetchNewChanges(ctx context.Context) error {
	slog.Info("[CHANGESTREAM-DEBUG] Querying changelog table",
		"client", w.clientName,
		"table", w.tableName,
		"lastEventID", w.lastEventID)

	query := `
		SELECT id, record_id, operation, old_values, new_values, changed_at
		FROM datastore_changelog
		WHERE table_name = $1 AND id > $2
		ORDER BY ID ASC
		LIMIT 100
	`

	rows, err := w.db.QueryContext(ctx, query, w.tableName, w.lastEventID)
	if err != nil {
		slog.Error("[CHANGESTREAM-DEBUG] Failed to query changelog",
			"client", w.clientName,
			"error", err)

		return fmt.Errorf("failed to query changelog: %w", err)
	}
	defer rows.Close()

	events, err := w.processChangelogRows(rows)
	if err != nil {
		slog.Error("[CHANGESTREAM-DEBUG] Failed to process changelog rows",
			"client", w.clientName,
			"error", err)

		return err
	}

	slog.Info("[CHANGESTREAM-DEBUG] Fetched events from changelog",
		"client", w.clientName,
		"eventCount", len(events))

	return w.sendEventsToChannel(ctx, events)
}

// processChangelogRows processes changelog rows and builds events
func (w *PostgreSQLChangeStreamWatcher) processChangelogRows(rows *sql.Rows) ([]datastore.EventWithToken, error) {
	var events []datastore.EventWithToken

	for rows.Next() {
		var (
			id                   int64
			recordID             string
			operation            string
			oldValues, newValues sql.NullString
			changedAt            time.Time
		)

		if err := rows.Scan(&id, &recordID, &operation, &oldValues, &newValues, &changedAt); err != nil {
			slog.Error("[CHANGESTREAM-DEBUG] Failed to scan changelog row",
				"client", w.clientName,
				"error", err)

			return nil, fmt.Errorf("failed to scan changelog row: %w", err)
		}

		// Calculate event delivery latency
		receivedAt := time.Now()
		latency := receivedAt.Sub(changedAt)

		slog.Info("[CHANGESTREAM-DEBUG] Processing changelog row",
			"client", w.clientName,
			"id", id,
			"recordID", recordID,
			"operation", operation,
			"changedAt", changedAt,
			"receivedAt", receivedAt,
			"latencyMs", latency.Milliseconds(),
			"hasOldValues", oldValues.Valid,
			"hasNewValues", newValues.Valid)

		event := w.buildEventDocument(id, recordID, operation, oldValues, newValues, changedAt)
		token := []byte(fmt.Sprintf("%d", id))

		eventWithToken := datastore.EventWithToken{
			Event:       event,
			ResumeToken: token,
		}

		events = append(events, eventWithToken)

		// NOTE: Do NOT update lastEventID here!
		// lastEventID should only be updated in sendEventsToChannel() after the event
		// is successfully sent to the channel. This ensures that:
		// 1. Filtered events don't advance the resume position
		// 2. Events are only marked as "seen" after they're actually delivered
		// 3. MarkProcessed() with empty token uses the correct position
		slog.Debug("[PROCESS-ROWS-DEBUG] Built event from changelog",
			"client", w.clientName,
			"changelogID", id,
			"operationType", operation)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating changelog rows: %w", err)
	}

	slog.Info("[PROCESS-ROWS-DEBUG] Finished processing changelog rows",
		"client", w.clientName,
		"totalEvents", len(events))

	return events, nil
}

// buildEventDocument builds an event document from changelog row data
func (w *PostgreSQLChangeStreamWatcher) buildEventDocument(
	id int64,
	recordID string,
	operation string,
	oldValues, newValues sql.NullString,
	changedAt time.Time,
) map[string]interface{} {
	slog.Info("[BUILD-EVENT-DEBUG] Building event document",
		"client", w.clientName,
		"changelogID", id,
		"recordID", recordID,
		"operation", operation)

	event := map[string]interface{}{
		"_id": map[string]interface{}{
			// Use changelog ID (not recordID) for _data field to maintain consistency
			// with resume tokens and to ensure GetDocumentID() returns an int-parseable value
			// that can be used for metrics and resume position tracking.
			"_data": fmt.Sprintf("%d", id), // Use changelog sequence ID
		},
		"operationType": mapOperation(operation),
		"clusterTime":   changedAt,
		"fullDocument":  nil,
	}

	w.addDocumentDataToEvent(event, recordID, operation, oldValues, newValues)

	// DEBUG: Log the final event structure
	eventJSON, _ := json.Marshal(event)
	slog.Info("[BUILD-EVENT-DEBUG] Completed event document",
		"client", w.clientName,
		"changelogID", id,
		"hasUpdateDescription", event["updateDescription"] != nil,
		"event", string(eventJSON))

	return event
}

// addDocumentDataToEvent adds document data to event based on operation type
//
//nolint:cyclop,gocognit,nestif // Event processing requires operation-specific handling
func (w *PostgreSQLChangeStreamWatcher) addDocumentDataToEvent(
	event map[string]interface{},
	recordID string,
	operation string,
	oldValues, newValues sql.NullString,
) {
	switch operation {
	case "INSERT":
		if newValues.Valid {
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(newValues.String), &doc); err == nil {
				slog.Info("[CONSTRUCT-DEBUG] Parsed new_values for INSERT",
					"tableName", w.tableName,
					"docKeys", getMapKeys(doc),
					"hasId", doc["id"] != nil,
					"hasDocument", doc["document"] != nil)

				// For health_events table, extract the inner document field
				if w.tableName == healthEventsTable {
					if innerDoc, ok := doc["document"].(map[string]interface{}); ok {
						slog.Info("[CONSTRUCT-DEBUG] Extracted innerDoc",
							"innerDocKeys", getMapKeys(innerDoc))

						// Add the record ID from the database column (not from JSONB document)
						innerDoc["id"] = recordID
						slog.Info("[CONSTRUCT-DEBUG] Added id to innerDoc", "id", recordID)

						event["fullDocument"] = innerDoc
					} else {
						event["fullDocument"] = doc
					}
				} else {
					event["fullDocument"] = doc
				}
			}
		}
	case "UPDATE":
		// For UPDATE operations, add both fullDocument and updateDescription
		if newValues.Valid {
			var newDoc map[string]interface{}
			if err := json.Unmarshal([]byte(newValues.String), &newDoc); err == nil {
				slog.Info("[CONSTRUCT-DEBUG] Parsed new_values for UPDATE",
					"tableName", w.tableName,
					"docKeys", getMapKeys(newDoc),
					"hasId", newDoc["id"] != nil,
					"hasDocument", newDoc["document"] != nil)

				// For health_events table, extract the inner document field for comparison
				var newDocForEvent, newDocForComparison map[string]interface{}

				if w.tableName == healthEventsTable {
					if innerDoc, ok := newDoc["document"].(map[string]interface{}); ok {
						slog.Info("[CONSTRUCT-DEBUG] Extracted innerDoc for UPDATE",
							"innerDocKeys", getMapKeys(innerDoc))

						// Use innerDoc AS-IS for comparison (without top-level id)
						newDocForComparison = innerDoc

						// Create a copy with id for fullDocument
						newDocForEvent = make(map[string]interface{})
						for k, v := range innerDoc {
							newDocForEvent[k] = v
						}

						// Add the record ID from the database column (not from JSONB document)
						newDocForEvent["id"] = recordID
						slog.Info("[CONSTRUCT-DEBUG] Added id to innerDoc for UPDATE", "id", recordID)
					} else {
						newDocForEvent = newDoc
						newDocForComparison = newDoc
					}
				} else {
					newDocForEvent = newDoc
					newDocForComparison = newDoc
				}

				event["fullDocument"] = newDocForEvent

				// Add updateDescription to match MongoDB changestream format
				if oldValues.Valid {
					var oldDoc map[string]interface{}
					if err := json.Unmarshal([]byte(oldValues.String), &oldDoc); err == nil {
						// DEBUG: Log the old and new values for comparison
						oldValuesJSON, _ := json.Marshal(oldDoc)
						newValuesJSON, _ := json.Marshal(newDoc)

						slog.Info("[CHANGESTREAM-DEBUG] UPDATE event - old_values",
							"client", w.clientName,
							"recordID", recordID,
							"old_values", string(oldValuesJSON))
						slog.Info("[CHANGESTREAM-DEBUG] UPDATE event - new_values",
							"client", w.clientName,
							"recordID", recordID,
							"new_values", string(newValuesJSON))

						// For health_events table, extract the inner document field for comparison
						var oldDocForComparison map[string]interface{}

						if w.tableName == healthEventsTable {
							if innerDoc, ok := oldDoc["document"].(map[string]interface{}); ok {
								oldDocForComparison = innerDoc
							} else {
								oldDocForComparison = oldDoc
							}
						} else {
							oldDocForComparison = oldDoc
						}

						// DEBUG: Log what we're comparing
						oldCmpJSON, _ := json.Marshal(oldDocForComparison)

						newCmpJSON, _ := json.Marshal(newDocForComparison)

						slog.Info("[CHANGESTREAM-DEBUG] Comparing oldDoc",
							"client", w.clientName,
							"recordID", recordID,
							"oldDoc", string(oldCmpJSON))
						slog.Info("[CHANGESTREAM-DEBUG] Comparing newDoc",
							"client", w.clientName,
							"recordID", recordID,
							"newDoc", string(newCmpJSON))

						updatedFields := w.findUpdatedFields(oldDocForComparison, newDocForComparison)

						// DEBUG: Log what findUpdatedFields returned
						updatedFieldsJSON, _ := json.Marshal(updatedFields)
						slog.Info("[CHANGESTREAM-DEBUG] findUpdatedFields returned",
							"client", w.clientName,
							"recordID", recordID,
							"updatedFields", string(updatedFieldsJSON),
							"fieldCount", len(updatedFields))

						if len(updatedFields) > 0 {
							event["updateDescription"] = map[string]interface{}{
								"updatedFields": updatedFields,
							}

							// DEBUG: Log the complete updateDescription
							updateDescJSON, _ := json.Marshal(event["updateDescription"])
							slog.Info("[CHANGESTREAM-DEBUG] updateDescription added to event",
								"client", w.clientName,
								"recordID", recordID,
								"updateDescription", string(updateDescJSON))
						} else {
							slog.Warn("[CHANGESTREAM-DEBUG] No updatedFields found, updateDescription not added",
								"client", w.clientName,
								"recordID", recordID)
						}
					}
				}
			}
		}
	case "DELETE":
		if oldValues.Valid {
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(oldValues.String), &doc); err == nil {
				// For health_events table, extract the inner document field
				if w.tableName == healthEventsTable {
					if innerDoc, ok := doc["document"].(map[string]interface{}); ok {
						event["fullDocumentBeforeChange"] = innerDoc
					} else {
						event["fullDocumentBeforeChange"] = doc
					}
				} else {
					event["fullDocumentBeforeChange"] = doc
				}
			}
		}
	}
}

// findUpdatedFields compares old and new documents to find changed fields
// Returns a flattened map with dot-notation keys to match MongoDB changestream format
func (w *PostgreSQLChangeStreamWatcher) findUpdatedFields(
	oldDoc, newDoc map[string]interface{},
) map[string]interface{} {
	updatedFields := make(map[string]interface{})

	slog.Info("[FIND-UPDATED-DEBUG] Starting field comparison",
		"client", w.clientName,
		"oldDocKeys", getMapKeys(oldDoc),
		"newDocKeys", getMapKeys(newDoc))

	// Compare all fields in newDoc with oldDoc
	for key, newValue := range newDoc {
		oldValue, exists := oldDoc[key]

		// Field is updated if it didn't exist before or if the value changed
		if !exists || !w.valuesEqual(oldValue, newValue) {
			slog.Info("[FIND-UPDATED-DEBUG] Field changed",
				"client", w.clientName,
				"key", key,
				"existed", exists,
				"valuesEqual", w.valuesEqual(oldValue, newValue))

			// For nested objects, flatten them with dot notation
			// e.g., healtheventstatus: {nodequarantined: "Quarantined"} becomes
			// "healtheventstatus.nodequarantined": "Quarantined"
			if newValueMap, ok := newValue.(map[string]interface{}); ok {
				slog.Info("[FIND-UPDATED-DEBUG] Flattening nested map",
					"client", w.clientName,
					"key", key,
					"nestedKeys", getMapKeys(newValueMap))
				w.flattenMap("", key, newValueMap, oldValue, updatedFields)
			} else {
				slog.Info("[FIND-UPDATED-DEBUG] Adding simple field",
					"client", w.clientName,
					"key", key,
					"value", newValue)
				updatedFields[key] = newValue
			}
		} else {
			slog.Debug("[FIND-UPDATED-DEBUG] Field unchanged, skipping",
				"client", w.clientName,
				"key", key)
		}
	}

	slog.Info("[FIND-UPDATED-DEBUG] Field comparison complete",
		"client", w.clientName,
		"updatedFieldCount", len(updatedFields),
		"updatedKeys", getMapKeys(updatedFields))

	return updatedFields
}

// flattenMap recursively flattens nested maps using dot notation
func (w *PostgreSQLChangeStreamWatcher) flattenMap(
	parentPrefix, currentKey string,
	currentValue map[string]interface{},
	oldValue interface{},
	result map[string]interface{},
) {
	var oldMap map[string]interface{}
	if oldValue != nil {
		oldMap, _ = oldValue.(map[string]interface{})
	}

	prefix := currentKey
	if parentPrefix != "" {
		prefix = parentPrefix + "." + currentKey
	}

	for k, v := range currentValue {
		fullKey := prefix + "." + k

		var oldV interface{}
		if oldMap != nil {
			oldV = oldMap[k]
		}

		// Recursively flatten nested maps
		if vMap, ok := v.(map[string]interface{}); ok {
			slog.Info("[FLATTENMAP-DEBUG] Recursing into nested map at key", "key", fullKey)
			w.flattenMap(prefix, k, vMap, oldV, result)
		} else if !w.valuesEqual(oldV, v) {
			// Only include if the value actually changed
			slog.Info("[FLATTENMAP-DEBUG] Adding changed field", "key", fullKey, "newValue", v, "oldValue", oldV)
			result[fullKey] = v
		} else {
			slog.Info("[FLATTENMAP-DEBUG] Skipping unchanged field", "key", fullKey, "value", v)
		}
	}
}

// valuesEqual compares two values for equality
//
//nolint:cyclop,gocognit,nestif // Deep equality comparison requires type checking
func (w *PostgreSQLChangeStreamWatcher) valuesEqual(v1, v2 interface{}) bool {
	// Handle nil cases
	if v1 == nil && v2 == nil {
		return true
	}

	if v1 == nil || v2 == nil {
		return false
	}

	// For maps, do deep comparison
	if m1, ok1 := v1.(map[string]interface{}); ok1 {
		if m2, ok2 := v2.(map[string]interface{}); ok2 {
			if len(m1) != len(m2) {
				return false
			}

			for k, val1 := range m1 {
				val2, exists := m2[k]
				if !exists || !w.valuesEqual(val1, val2) {
					return false
				}
			}

			return true
		}

		return false
	}

	// For slices, do deep comparison
	if s1, ok1 := v1.([]interface{}); ok1 {
		if s2, ok2 := v2.([]interface{}); ok2 {
			if len(s1) != len(s2) {
				return false
			}

			for i, val1 := range s1 {
				if !w.valuesEqual(val1, s2[i]) {
					return false
				}
			}

			return true
		}

		return false
	}

	// For primitive types, use direct comparison
	return v1 == v2
}

const unknownNodeName = "unknown"

// extractEventInfo extracts node name and operation type from an event
func extractEventInfo(event datastore.EventWithToken) (string, interface{}) {
	nodeName := unknownNodeName
	operationType := event.Event["operationType"]

	if fullDoc, ok := event.Event["fullDocument"].(map[string]interface{}); ok {
		if healthevent, ok := fullDoc["healthevent"].(map[string]interface{}); ok {
			// Try exact match first (case-sensitive)
			if name, ok := healthevent["nodename"].(string); ok {
				nodeName = name
			} else if name, ok := healthevent["nodeName"].(string); ok {
				// Fall back to camelCase (PostgreSQL uses camelCase JSON tags)
				nodeName = name
			}
		}
	}

	return nodeName, operationType
}

// sendEventsToChannel sends events to the channel
// Sends each event to the events channel sequentially
// If a pipeline filter is configured, events are filtered before sending
//
//nolint:cyclop // Event processing requires sequential steps
func (w *PostgreSQLChangeStreamWatcher) sendEventsToChannel(
	ctx context.Context,
	events []datastore.EventWithToken,
) error {
	sentCount := 0
	filteredCount := 0

	for _, event := range events {
		nodeName, operationType := extractEventInfo(event)

		// Apply pipeline filter if configured
		if w.pipelineFilter != nil {
			// DEBUG: Log the event structure before filtering
			eventJSON, _ := json.Marshal(event.Event)
			slog.Info("[CHANGESTREAM-DEBUG] Before pipeline filter",
				"client", w.clientName,
				"token", string(event.ResumeToken),
				"operationType", operationType,
				"nodeName", nodeName,
				"event", string(eventJSON))

			if !w.pipelineFilter.MatchesEvent(event) {
				slog.Warn("[CHANGESTREAM-DEBUG] Event filtered out by pipeline",
					"client", w.clientName,
					"token", string(event.ResumeToken),
					"operationType", operationType,
					"nodeName", nodeName)

				filteredCount++

				continue // Skip events that don't match the pipeline
			}
		}

		slog.Info("[CHANGESTREAM-DEBUG] Sending event to channel",
			"client", w.clientName,
			"token", string(event.ResumeToken),
			"operationType", operationType,
			"nodeName", nodeName)

		// Extract event ID from resume token for tracking
		eventIDStr := string(event.ResumeToken)

		var (
			parseErr error
			eventID  int64
		)

		eventID, parseErr = strconv.ParseInt(eventIDStr, 10, 64)
		if parseErr != nil {
			slog.Error("[CHANGESTREAM-DEBUG] Failed to parse event ID from resume token",
				"client", w.clientName,
				"token", eventIDStr,
				"error", parseErr)
			// Continue anyway - don't fail the whole send operation
		}

		select {
		case w.events <- event:
			sentCount++

			// CRITICAL: Update lastEventID ONLY after event is successfully sent to channel
			// This ensures that:
			// 1. MarkProcessed() with empty token uses the position of the last SENT event
			// 2. Filtered events don't advance the resume position
			// 3. If we crash after sending but before processing, the event will be redelivered
			if parseErr == nil {
				w.mu.Lock()
				w.lastEventID = eventID
				w.mu.Unlock()

				slog.Info("[CHANGESTREAM-DEBUG] Event sent successfully, updated lastEventID",
					"client", w.clientName,
					"token", string(event.ResumeToken),
					"lastEventID", eventID)
			} else {
				slog.Info("[CHANGESTREAM-DEBUG] Event sent successfully (lastEventID not updated due to parse error)",
					"client", w.clientName,
					"token", string(event.ResumeToken))
			}
		case <-ctx.Done():
			slog.Warn("[CHANGESTREAM-DEBUG] Context cancelled while sending event",
				"client", w.clientName)

			return ctx.Err()
		case <-w.stopCh:
			slog.Warn("[CHANGESTREAM-DEBUG] Stop channel triggered while sending event",
				"client", w.clientName)

			return nil
		}
	}

	if len(events) > 0 {
		slog.Info("[CHANGESTREAM-DEBUG] Finished sending events",
			"client", w.clientName,
			"totalEvents", len(events),
			"sentCount", sentCount,
			"filteredCount", filteredCount)

		if w.pipelineFilter != nil {
			slog.Debug("Sent filtered change events",
				"sent", sentCount,
				"filtered_out", filteredCount,
				"total", len(events),
				"table", w.tableName)
		} else {
			slog.Debug("Sent change events", "count", sentCount, "table", w.tableName)
		}
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
			// No resume token found - start from current position (like MongoDB does)
			// This prevents replaying all historical events on first startup
			maxIDQuery := `
				SELECT COALESCE(MAX(id), 0)
				FROM datastore_changelog
				WHERE table_name = $1
			`

			var maxID int64

			err := w.db.QueryRowContext(ctx, maxIDQuery, w.tableName).Scan(&maxID)
			if err != nil {
				return fmt.Errorf("failed to get latest event ID: %w", err)
			}

			w.lastEventID = maxID

			slog.Info("No resume token found, starting from current position",
				"client", w.clientName,
				"eventID", maxID,
				"table", w.tableName)

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

	slog.Info("Loaded resume position", "client", w.clientName, "eventID", w.lastEventID)

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

// GetUnprocessedEventCount returns the count of unprocessed events in the changelog
// This implements the ChangeStreamMetrics interface for observability
func (w *PostgreSQLChangeStreamWatcher) GetUnprocessedEventCount(
	ctx context.Context,
	lastProcessedID string,
) (int64, error) {
	// Parse the last processed ID
	var eventID int64

	if lastProcessedID != "" {
		var err error

		eventID, err = strconv.ParseInt(lastProcessedID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid lastProcessedID format: %w", err)
		}
	} else {
		// If no lastProcessedID provided, use the watcher's current position
		eventID = w.lastEventID
	}

	// Count unprocessed events in the changelog for this table
	query := `
		SELECT COUNT(*)
		FROM datastore_changelog
		WHERE table_name = $1 AND id > $2 AND processed = FALSE
	`

	var count int64

	err := w.db.QueryRowContext(ctx, query, w.tableName, eventID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count unprocessed events: %w", err)
	}

	return count, nil
}

// Verify that PostgreSQLChangeStreamWatcher implements the ChangeStreamWatcher interface
var _ datastore.ChangeStreamWatcher = (*PostgreSQLChangeStreamWatcher)(nil)

// --- Backward Compatibility Adapter for client.ChangeStreamWatcher ---

// PostgreSQLEventAdapter wraps a datastore.EventWithToken and implements client.Event
// This provides backward compatibility with services using the old EventProcessor/EventWatcher
type PostgreSQLEventAdapter struct {
	eventData   map[string]interface{}
	resumeToken []byte
}

// GetDocumentID returns the changelog sequence ID for this event.
// This ID is used for:
// - Tracking the last processed position in the changestream
// - Metrics and monitoring (GetUnprocessedEventCount)
// - Resume token comparisons
//
// For PostgreSQL, this returns the datastore_changelog.id (integer) as a string,
// NOT the document's UUID. To get the document UUID, use GetRecordUUID().
//
// This maintains consistency with the resume token and ensures the returned
// value can be parsed as an integer for metrics queries.
func (e *PostgreSQLEventAdapter) GetDocumentID() (string, error) {
	slog.Info("[GETDOCID-DEBUG] GetDocumentID called - retrieving changelog sequence ID",
		"eventData_keys", getMapKeys(e.eventData))

	// Get the changelog sequence ID from _id._data
	// After the fix at line 256, this contains the integer changelog ID (not the document UUID)
	if idData, exists := e.eventData["_id"]; exists {
		slog.Info("[GETDOCID-DEBUG] Found _id in event", "idData", idData)

		if idMap, ok := idData.(map[string]interface{}); ok {
			if dataVal, ok := idMap["_data"]; ok {
				slog.Info("[GETDOCID-DEBUG] SUCCESS - found changelog ID in _id._data",
					"value", dataVal,
					"type", fmt.Sprintf("%T", dataVal))

				return fmt.Sprintf("%v", dataVal), nil
			}
		}

		// Fallback: use _id directly if _data not present
		slog.Info("[GETDOCID-DEBUG] Using _id directly", "value", idData)

		return fmt.Sprintf("%v", idData), nil
	}

	slog.Error("[GETDOCID-DEBUG] FAILED - no changelog sequence ID found in _id field")

	return "", fmt.Errorf("changelog sequence ID not found in event")
}

// GetRecordUUID returns the actual document UUID from the fullDocument.
// This should be used when you need the business entity ID, not for
// changestream tracking or resume tokens.
//
// For example, use this when:
// - Correlating with other systems that reference the document UUID
// - Business logic that needs the actual document identifier
// - Deduplication based on document identity
func (e *PostgreSQLEventAdapter) GetRecordUUID() (string, error) {
	slog.Info("[GETUUID-DEBUG] GetRecordUUID called",
		"hasFullDocument", e.eventData["fullDocument"] != nil,
		"eventDataKeys", getMapKeys(e.eventData))

	if fullDoc, ok := e.eventData["fullDocument"].(map[string]interface{}); ok {
		slog.Info("[GETUUID-DEBUG] fullDocument structure",
			"fullDocKeys", getMapKeys(fullDoc))

		// Try "id" field (PostgreSQL lowercase)
		if id, exists := fullDoc["id"]; exists {
			slog.Info("[GETUUID-DEBUG] Found 'id' field", "value", id)
			return fmt.Sprintf("%v", id), nil
		}
		// Try "_id" field (MongoDB compatibility)
		if id, exists := fullDoc["_id"]; exists {
			slog.Info("[GETUUID-DEBUG] Found '_id' field", "value", id)
			return fmt.Sprintf("%v", id), nil
		}

		slog.Error("[GETUUID-DEBUG] Neither 'id' nor '_id' found in fullDocument",
			"fullDocKeys", getMapKeys(fullDoc))
	} else {
		slog.Error("[GETUUID-DEBUG] fullDocument is not a map or doesn't exist")
	}

	return "", fmt.Errorf("record UUID not found in fullDocument")
}

// getMapKeys returns the keys of a map as a slice (helper for debugging)
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

// GetNodeName extracts the node name from the event's fullDocument
func (e *PostgreSQLEventAdapter) GetNodeName() (string, error) {
	//nolint:nestif // Nested complexity required for dual-case field name lookups
	if fullDoc, ok := e.eventData["fullDocument"].(map[string]interface{}); ok {
		if healthEvent, ok := fullDoc["healthevent"].(map[string]interface{}); ok {
			// Try lowercase first (MongoDB compatibility)
			if nodeName, ok := healthEvent["nodename"].(string); ok {
				return nodeName, nil
			}
			// Try camelCase (PostgreSQL JSON)
			if nodeName, ok := healthEvent["nodeName"].(string); ok {
				return nodeName, nil
			}
		}
	}

	return "", fmt.Errorf("node name not found in event")
}

// GetResumeToken returns the resume token for this event
func (e *PostgreSQLEventAdapter) GetResumeToken() []byte {
	return e.resumeToken
}

// UnmarshalDocument unmarshals the event data into the provided interface
func (e *PostgreSQLEventAdapter) UnmarshalDocument(v interface{}) error {
	slog.Info("[CHANGESTREAM-DEBUG] UnmarshalDocument called",
		"hasFullDocument", e.eventData["fullDocument"] != nil)

	// The fullDocument contains the actual document data
	fullDoc, ok := e.eventData["fullDocument"]
	if !ok {
		return fmt.Errorf("fullDocument not found in event")
	}

	// Convert to map for easier manipulation
	docMap, ok := fullDoc.(map[string]interface{})
	if !ok {
		slog.Error("[CHANGESTREAM-DEBUG] fullDocument is not a map",
			"type", fmt.Sprintf("%T", fullDoc))

		return fmt.Errorf("fullDocument is not a map[string]interface{}")
	}

	slog.Info("[CHANGESTREAM-DEBUG] fullDocument structure",
		"keys", func() []string {
			keys := make([]string, 0, len(docMap))
			for k := range docMap {
				keys = append(keys, k)
			}

			return keys
		}())

	// The PostgreSQL provider stores the document in a nested "document" field
	// Extract just the document field which contains the actual HealthEventWithStatus
	// but preserve the top-level id field
	actualDoc := e.extractActualDocument(docMap)

	slog.Info("[CHANGESTREAM-DEBUG] After extractActualDocument",
		"keys", func() []string {
			keys := make([]string, 0, len(actualDoc))
			for k := range actualDoc {
				keys = append(keys, k)
			}

			return keys
		}())

	// Transform lowercase keys to match the struct field names
	// This handles the case where PostgreSQL stores lowercase JSON field names
	// but Go struct tags may expect different casing
	transformedDoc := transformJSONKeys(actualDoc)

	slog.Info("[CHANGESTREAM-DEBUG] After transformJSONKeys",
		"keys", func() []string {
			keys := make([]string, 0, len(transformedDoc))
			for k := range transformedDoc {
				keys = append(keys, k)
			}

			return keys
		}())

	// Use JSON marshaling/unmarshaling for type conversion
	jsonData, err := json.Marshal(transformedDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal event document: %w", err)
	}

	slog.Info("[UNMARSHAL-DEBUG] About to unmarshal into target",
		"jsonDataLength", len(jsonData),
		"jsonDataPreview", func() string {
			if len(jsonData) > 500 {
				return string(jsonData[:500]) + "..."
			}

			return string(jsonData)
		}(),
		"targetType", fmt.Sprintf("%T", v))

	if err := json.Unmarshal(jsonData, v); err != nil {
		slog.Error("[UNMARSHAL-DEBUG] Unmarshal FAILED",
			"error", err,
			"jsonData", string(jsonData))

		return fmt.Errorf("failed to unmarshal event document: %w", err)
	}

	slog.Info("[UNMARSHAL-DEBUG] Unmarshal successful",
		"result", fmt.Sprintf("%+v", v))

	return nil
}

// extractActualDocument extracts the actual document from the nested structure
// and preserves the top-level id field from the database row
func (e *PostgreSQLEventAdapter) extractActualDocument(docMap map[string]interface{}) map[string]interface{} {
	if nestedDoc, ok := docMap["document"].(map[string]interface{}); ok {
		// Preserve the id field from the top-level docMap
		if id, hasID := docMap["id"]; hasID {
			nestedDoc["_id"] = id
		}

		return nestedDoc
	}
	// Fall back to using the whole docMap if there's no nested document
	return docMap
}

// transformJSONKeys transforms lowercase JSON keys to match Go struct field names
// This is needed because PostgreSQL stores lowercase JSON field names from bson tags
// but protobuf fields need specific casing for proper unmarshaling
func transformJSONKeys(doc map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range doc {
		// Handle nested maps recursively
		if nestedMap, ok := value.(map[string]interface{}); ok {
			value = transformJSONKeys(nestedMap)
		}

		// Apply specific transformations for known fields
		// Map lowercase keys from bson tags to proper JSON keys
		transformedKey := getTransformedKey(key)
		result[transformedKey] = value
	}

	return result
}

// getTransformedKey returns the transformed key for known fields
func getTransformedKey(key string) string {
	keyMap := map[string]string{
		"healthevent":              "healthevent",
		"healtheventstatus":        "healtheventstatus",
		"createdat":                "createdAt",
		"nodequarantined":          "nodequarantined",
		"userpodsevictionstatus":   "userpodsevictionstatus",
		"faultremediated":          "faultremediated",
		"lastremediationtimestamp": "lastremediationtimestamp",
	}

	if transformedKey, ok := keyMap[key]; ok {
		return transformedKey
	}

	// Keep other keys as-is
	return key
}

// Verify PostgreSQLEventAdapter implements client.Event interface at compile time
var _ client.Event = (*PostgreSQLEventAdapter)(nil)

// PostgreSQLChangeStreamAdapter wraps PostgreSQLChangeStreamWatcher and implements client.ChangeStreamWatcher
// This provides backward compatibility with services using the old EventProcessor/EventWatcher
type PostgreSQLChangeStreamAdapter struct {
	watcher       *PostgreSQLChangeStreamWatcher
	eventChan     chan client.Event
	stopConverter chan struct{}
	initOnce      sync.Once
}

// NewPostgreSQLChangeStreamAdapter creates a new adapter for backward compatibility
func NewPostgreSQLChangeStreamAdapter(watcher *PostgreSQLChangeStreamWatcher) *PostgreSQLChangeStreamAdapter {
	return &PostgreSQLChangeStreamAdapter{
		watcher:       watcher,
		stopConverter: make(chan struct{}),
	}
}

// Events returns a channel of client.Event
func (a *PostgreSQLChangeStreamAdapter) Events() <-chan client.Event {
	a.initOnce.Do(func() {
		a.eventChan = make(chan client.Event, 100)

		go func() {
			defer close(a.eventChan)

			for {
				select {
				case eventWithToken, ok := <-a.watcher.Events():
					if !ok {
						return // Channel closed
					}

					// Convert datastore.EventWithToken to client.Event
					adapter := &PostgreSQLEventAdapter{
						eventData:   eventWithToken.Event,
						resumeToken: eventWithToken.ResumeToken,
					}

					select {
					case a.eventChan <- adapter:
					case <-a.stopConverter:
						return
					}
				case <-a.stopConverter:
					return
				}
			}
		}()
	})

	return a.eventChan
}

// Start starts the underlying watcher
func (a *PostgreSQLChangeStreamAdapter) Start(ctx context.Context) {
	a.watcher.Start(ctx)
}

// MarkProcessed marks events as processed
func (a *PostgreSQLChangeStreamAdapter) MarkProcessed(ctx context.Context, token []byte) error {
	return a.watcher.MarkProcessed(ctx, token)
}

// Close closes the adapter and underlying watcher
func (a *PostgreSQLChangeStreamAdapter) Close(ctx context.Context) error {
	close(a.stopConverter)

	return a.watcher.Close(ctx)
}

// PostgreSQLChangeStreamWatcherWithUnwrap wraps PostgreSQLChangeStreamWatcher
// and provides the Unwrap() method without creating interface conflicts.
// This wrapper implements datastore.ChangeStreamWatcher and can be unwrapped to client.ChangeStreamWatcher.
type PostgreSQLChangeStreamWatcherWithUnwrap struct {
	watcher *PostgreSQLChangeStreamWatcher
	adapter *PostgreSQLChangeStreamAdapter
}

// NewPostgreSQLChangeStreamWatcherWithUnwrap creates a wrapper that supports unwrapping
func NewPostgreSQLChangeStreamWatcherWithUnwrap(
	watcher *PostgreSQLChangeStreamWatcher,
) *PostgreSQLChangeStreamWatcherWithUnwrap {
	return &PostgreSQLChangeStreamWatcherWithUnwrap{
		watcher: watcher,
		adapter: NewPostgreSQLChangeStreamAdapter(watcher),
	}
}

// Events implements datastore.ChangeStreamWatcher by delegating to the wrapped watcher
func (w *PostgreSQLChangeStreamWatcherWithUnwrap) Events() <-chan datastore.EventWithToken {
	return w.watcher.Events()
}

// Start implements datastore.ChangeStreamWatcher by delegating to the wrapped watcher
func (w *PostgreSQLChangeStreamWatcherWithUnwrap) Start(ctx context.Context) {
	w.watcher.Start(ctx)
}

// MarkProcessed implements datastore.ChangeStreamWatcher by delegating to the wrapped watcher
func (w *PostgreSQLChangeStreamWatcherWithUnwrap) MarkProcessed(ctx context.Context, token []byte) error {
	return w.watcher.MarkProcessed(ctx, token)
}

// Close implements datastore.ChangeStreamWatcher by delegating to the wrapped watcher
func (w *PostgreSQLChangeStreamWatcherWithUnwrap) Close(ctx context.Context) error {
	return w.watcher.Close(ctx)
}

// Unwrap returns the adapter as client.ChangeStreamWatcher for backward compatibility
// This allows services to unwrap the PostgreSQL watcher to the legacy interface
func (w *PostgreSQLChangeStreamWatcherWithUnwrap) Unwrap() client.ChangeStreamWatcher {
	return w.adapter
}

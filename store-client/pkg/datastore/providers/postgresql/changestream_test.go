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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPostgreSQLChangeStreamWatcher(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name       string
		tableName  string
		clientName string
	}{
		{
			name:       "valid parameters for health events",
			tableName:  "health_events",
			clientName: "test-client",
		},
		{
			name:       "valid parameters for maintenance events",
			tableName:  "maintenance_events",
			clientName: "another-client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watcher := NewPostgreSQLChangeStreamWatcher(db, tt.clientName, tt.tableName)

			assert.NotNil(t, watcher)
			assert.Equal(t, tt.tableName, watcher.tableName)
			assert.Equal(t, tt.clientName, watcher.clientName)
			assert.Equal(t, 5*time.Second, watcher.pollInterval)
			assert.NotNil(t, watcher.Events())
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_MarkProcessed(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	tests := []struct {
		name        string
		token       []byte
		setupMock   func()
		expectError bool
		errorMsg    string
	}{
		{
			name:  "successful marking",
			token: []byte("123"),
			setupMock: func() {
				mock.ExpectExec("UPDATE datastore_changelog SET processed = TRUE").
					WithArgs(int64(123), "health_events").
					WillReturnResult(sqlmock.NewResult(0, 5))

				// Mock save resume position
				mock.ExpectExec("INSERT INTO resume_tokens").
					WithArgs("test-client", sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
		{
			name:  "invalid token format",
			token: []byte("invalid"),
			setupMock: func() {
				// No mock setup needed as it fails before database call
			},
			expectError: true,
			errorMsg:    "invalid token format",
		},
		{
			name:  "database error",
			token: []byte("123"),
			setupMock: func() {
				mock.ExpectExec("UPDATE datastore_changelog SET processed = TRUE").
					WithArgs(int64(123), "health_events").
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectError: true,
			errorMsg:    "failed to mark events as processed",
		},
		{
			name:        "empty token",
			token:       []byte(""),
			setupMock:   func() {},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := watcher.MarkProcessed(context.Background(), tt.token)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			// Note: We can't always check mock expectations due to conditional logic
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_Close(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	err = watcher.Close(context.Background())
	assert.NoError(t, err)
}

func TestPostgreSQLChangeStreamWatcher_loadResumePosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	tests := []struct {
		name            string
		setupMock       func()
		expectedEventID int64
		expectError     bool
		errorMsg        string
	}{
		{
			name: "token exists",
			setupMock: func() {
				tokenJSON := `{"eventID": 123, "timestamp": "2023-01-01T00:00:00Z"}`
				rows := sqlmock.NewRows([]string{"resume_token"}).
					AddRow([]byte(tokenJSON))
				mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
					WithArgs("test-client").
					WillReturnRows(rows)
			},
			expectedEventID: 123,
			expectError:     false,
		},
		{
			name: "token not found",
			setupMock: func() {
				mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
					WithArgs("test-client").
					WillReturnError(sql.ErrNoRows)
			},
			expectedEventID: 0,
			expectError:     false,
		},
		{
			name: "database error",
			setupMock: func() {
				mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
					WithArgs("test-client").
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectedEventID: 0,
			expectError:     true,
			errorMsg:        "failed to load resume position",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := watcher.loadResumePosition(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedEventID, watcher.lastEventID)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_saveResumePosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	tests := []struct {
		name        string
		eventID     int64
		setupMock   func()
		expectError bool
		errorMsg    string
	}{
		{
			name:    "successful save",
			eventID: 456,
			setupMock: func() {
				mock.ExpectExec("INSERT INTO resume_tokens").
					WithArgs("test-client", sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
		{
			name:    "database error",
			eventID: 456,
			setupMock: func() {
				mock.ExpectExec("INSERT INTO resume_tokens").
					WithArgs("test-client", sqlmock.AnyArg()).
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectError: true,
			errorMsg:    "failed to save resume position",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := watcher.saveResumePosition(context.Background(), tt.eventID)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_fetchNewChanges(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*sqlmock.Sqlmock)
		expectError bool
		errorMsg    string
	}{
		{
			name: "successful fetch with changes",
			setupMock: func(mock *sqlmock.Sqlmock) {
				changeData := map[string]interface{}{
					"id":         "test-id",
					"node_name":  "test-node",
					"event_type": "GPU_ERROR",
				}
				changeJSON, _ := json.Marshal(changeData)

				rows := sqlmock.NewRows([]string{"id", "record_id", "operation", "old_values", "new_values", "changed_at"}).
					AddRow(101, "test-record-id", "INSERT", nil, string(changeJSON), time.Now())
				(*mock).ExpectQuery("SELECT id, record_id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnRows(rows)
			},
			expectError: false,
		},
		{
			name: "no changes found",
			setupMock: func(mock *sqlmock.Sqlmock) {
				(*mock).ExpectQuery("SELECT id, record_id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnRows(sqlmock.NewRows([]string{"id", "record_id", "operation", "old_values", "new_values", "changed_at"}))
			},
			expectError: false,
		},
		{
			name: "database error",
			setupMock: func(mock *sqlmock.Sqlmock) {
				(*mock).ExpectQuery("SELECT id, record_id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectError: true,
			errorMsg:    "failed to query changelog",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
			watcher.lastEventID = 100

			tt.setupMock(&mock)

			err = watcher.fetchNewChanges(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_buildEventDocument(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	changeData := map[string]interface{}{
		"id":         "test-id",
		"node_name":  "test-node",
		"event_type": "GPU_ERROR",
	}
	changeJSON, _ := json.Marshal(changeData)
	newValues := sql.NullString{String: string(changeJSON), Valid: true}
	oldValues := sql.NullString{Valid: false}
	changedAt := time.Now()

	event := watcher.buildEventDocument(123, "test-record-id", "INSERT", oldValues, newValues, changedAt)

	assert.Equal(t, "insert", event["operationType"])
	assert.Equal(t, changedAt, event["clusterTime"])
	assert.Contains(t, event, "_id")
	assert.Contains(t, event, "fullDocument")

	// Check _id structure
	idMap, ok := event["_id"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test-record-id", idMap["_data"])

	// Check fullDocument content
	fullDoc, ok := event["fullDocument"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test-id", fullDoc["id"])
	assert.Equal(t, "test-node", fullDoc["node_name"])
	assert.Equal(t, "GPU_ERROR", fullDoc["event_type"])
}

func TestMapOperation(t *testing.T) {
	tests := []struct {
		pgOp     string
		expected string
	}{
		{"INSERT", "insert"},
		{"UPDATE", "update"},
		{"DELETE", "delete"},
		{"UNKNOWN", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.pgOp, func(t *testing.T) {
			result := mapOperation(tt.pgOp)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPostgreSQLChangeStreamWatcher_sendEventsToChannel(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")

	// Create test events
	events := []datastore.EventWithToken{
		{
			Event: map[string]interface{}{
				"operationType": "insert",
				"fullDocument": map[string]interface{}{
					"id": "test-1",
				},
			},
			ResumeToken: []byte("1"),
		},
		{
			Event: map[string]interface{}{
				"operationType": "update",
				"fullDocument": map[string]interface{}{
					"id": "test-2",
				},
			},
			ResumeToken: []byte("2"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Send events in a goroutine
	go func() {
		err := watcher.sendEventsToChannel(ctx, events)
		assert.NoError(t, err)
	}()

	// Receive events
	receivedCount := 0
	eventChan := watcher.Events()

	for receivedCount < len(events) {
		select {
		case event := <-eventChan:
			assert.NotNil(t, event.Event)
			assert.NotNil(t, event.ResumeToken)
			receivedCount++
		case <-ctx.Done():
			break
		}
	}

	assert.Equal(t, len(events), receivedCount)
}

func TestPostgreSQLChangeStreamWatcher_GetUnprocessedEventCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	watcher.lastEventID = 100

	tests := []struct {
		name            string
		lastProcessedID string
		setupMock       func()
		expectedCount   int64
		expectError     bool
		errorMsg        string
	}{
		{
			name:            "successful count with explicit lastProcessedID",
			lastProcessedID: "50",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(25)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM datastore_changelog").
					WithArgs("health_events", int64(50)).
					WillReturnRows(rows)
			},
			expectedCount: 25,
			expectError:   false,
		},
		{
			name:            "successful count with empty lastProcessedID",
			lastProcessedID: "",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(15)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnRows(rows)
			},
			expectedCount: 15,
			expectError:   false,
		},
		{
			name:            "zero unprocessed events",
			lastProcessedID: "200",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(0)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM datastore_changelog").
					WithArgs("health_events", int64(200)).
					WillReturnRows(rows)
			},
			expectedCount: 0,
			expectError:   false,
		},
		{
			name:            "invalid lastProcessedID format",
			lastProcessedID: "invalid-id",
			setupMock:       func() {},
			expectedCount:   0,
			expectError:     true,
			errorMsg:        "invalid lastProcessedID format",
		},
		{
			name:            "database error",
			lastProcessedID: "50",
			setupMock: func() {
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM datastore_changelog").
					WithArgs("health_events", int64(50)).
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectedCount: 0,
			expectError:   true,
			errorMsg:      "failed to count unprocessed events",
		},
		{
			name:            "large count value",
			lastProcessedID: "10",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(99999)
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM datastore_changelog").
					WithArgs("health_events", int64(10)).
					WillReturnRows(rows)
			},
			expectedCount: 99999,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			count, err := watcher.GetUnprocessedEventCount(context.Background(), tt.lastProcessedID)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedCount, count)
			}

			// Verify all expectations were met (skip for tests that don't make DB calls)
			if tt.lastProcessedID != "invalid-id" {
				assert.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}

// TestNewPostgreSQLChangeStreamWatcherWithUnwrap tests wrapper creation
func TestNewPostgreSQLChangeStreamWatcherWithUnwrap(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	assert.NotNil(t, wrappedWatcher)
	assert.NotNil(t, wrappedWatcher.watcher)
	assert.NotNil(t, wrappedWatcher.adapter)
	assert.Equal(t, watcher, wrappedWatcher.watcher)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_ImplementsDatastoreInterface tests that
// the wrapper correctly implements datastore.ChangeStreamWatcher
func TestPostgreSQLChangeStreamWatcherWithUnwrap_ImplementsDatastoreInterface(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Type assertion to datastore.ChangeStreamWatcher should succeed
	var _ datastore.ChangeStreamWatcher = wrappedWatcher

	// Verify all required methods are available
	assert.NotNil(t, wrappedWatcher.Events())

	// These methods should be callable (we're just checking they exist and are callable)
	ctx := context.Background()
	wrappedWatcher.Start(ctx)

	err = wrappedWatcher.MarkProcessed(ctx, []byte(""))
	assert.NoError(t, err)

	err = wrappedWatcher.Close(ctx)
	assert.NoError(t, err)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_Unwrap tests the Unwrap method
func TestPostgreSQLChangeStreamWatcherWithUnwrap_Unwrap(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Unwrap should return a valid client.ChangeStreamWatcher
	unwrapped := wrappedWatcher.Unwrap()
	assert.NotNil(t, unwrapped)

	// The unwrapped adapter should be a PostgreSQLChangeStreamAdapter
	adapter, ok := unwrapped.(*PostgreSQLChangeStreamAdapter)
	assert.True(t, ok, "unwrapped watcher should be a PostgreSQLChangeStreamAdapter")
	assert.NotNil(t, adapter)
	assert.Equal(t, wrappedWatcher.adapter, adapter)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_EventsDelegation tests Events() delegation
func TestPostgreSQLChangeStreamWatcherWithUnwrap_EventsDelegation(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Events() should return the same channel from the underlying watcher
	wrappedEvents := wrappedWatcher.Events()
	underlyingEvents := watcher.Events()

	assert.Equal(t, underlyingEvents, wrappedEvents, "Events() should delegate to underlying watcher")
	assert.NotNil(t, wrappedEvents)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_MarkProcessedDelegation tests MarkProcessed delegation
func TestPostgreSQLChangeStreamWatcherWithUnwrap_MarkProcessedDelegation(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	token := []byte("123")

	// Setup mock for MarkProcessed
	mock.ExpectExec("UPDATE datastore_changelog SET processed = TRUE").
		WithArgs(int64(123), "health_events").
		WillReturnResult(sqlmock.NewResult(0, 5))

	mock.ExpectExec("INSERT INTO resume_tokens").
		WithArgs("test-client", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Call MarkProcessed on wrapper
	err = wrappedWatcher.MarkProcessed(context.Background(), token)
	assert.NoError(t, err)

	// Verify the delegation worked by checking mock expectations
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_CloseDelegation tests Close delegation
func TestPostgreSQLChangeStreamWatcherWithUnwrap_CloseDelegation(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Close should delegate to underlying watcher
	err = wrappedWatcher.Close(context.Background())
	assert.NoError(t, err)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_ReconcilerPattern tests the unwrapper
// interface pattern used by reconcilers (fault-quarantine, health-events-analyzer, etc.)
func TestPostgreSQLChangeStreamWatcherWithUnwrap_ReconcilerPattern(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Simulate the reconciler pattern that expects an unwrapper interface
	// This matches the actual interface in fault-quarantine/pkg/reconciler/reconciler.go:167-169
	type unwrapper interface {
		Unwrap() client.ChangeStreamWatcher
	}

	// The wrapper should satisfy this interface
	var changeStreamWatcher datastore.ChangeStreamWatcher = wrappedWatcher

	// Type assertion to unwrapper interface (simulating reconciler.go:171)
	unwrapable, ok := changeStreamWatcher.(unwrapper)
	assert.True(t, ok, "watcher should support unwrapping")
	assert.NotNil(t, unwrapable)

	// Unwrap should return a non-nil client.ChangeStreamWatcher
	unwrapped := unwrapable.Unwrap()
	assert.NotNil(t, unwrapped)

	// Verify it's the correct type
	adapter, ok := unwrapped.(*PostgreSQLChangeStreamAdapter)
	assert.True(t, ok, "unwrapped should be a PostgreSQLChangeStreamAdapter")
	assert.NotNil(t, adapter)
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_InterfaceCompatibility verifies that
// the wrapper pattern resolves the impossible type assertion issue
func TestPostgreSQLChangeStreamWatcherWithUnwrap_InterfaceCompatibility(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Test 1: Wrapper implements datastore.ChangeStreamWatcher
	var datastoreWatcher datastore.ChangeStreamWatcher = wrappedWatcher
	assert.NotNil(t, datastoreWatcher)

	// Verify datastore.ChangeStreamWatcher methods
	datastoreEvents := datastoreWatcher.Events()
	assert.NotNil(t, datastoreEvents)

	// Test 2: Unwrapped adapter is a different type implementing client.ChangeStreamWatcher
	adapter := wrappedWatcher.Unwrap()
	assert.NotNil(t, adapter)

	// Test 3: The wrapper and adapter are different types
	assert.IsType(t, &PostgreSQLChangeStreamWatcherWithUnwrap{}, wrappedWatcher)
	assert.IsType(t, &PostgreSQLChangeStreamAdapter{}, adapter)

	// Test 4: This demonstrates that we've separated the two conflicting interfaces
	// wrappedWatcher implements datastore.ChangeStreamWatcher (Events() returns EventWithToken)
	// adapter implements client.ChangeStreamWatcher (Events() returns Event)
	// They cannot be the same type because Events() has different signatures
}

// TestPostgreSQLChangeStreamWatcherWithUnwrap_StartDelegation tests Start delegation
func TestPostgreSQLChangeStreamWatcherWithUnwrap_StartDelegation(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	wrappedWatcher := NewPostgreSQLChangeStreamWatcherWithUnwrap(watcher)

	// Setup mock for loadResumePosition (called by Start)
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs("test-client").
		WillReturnError(sql.ErrNoRows)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should delegate to underlying watcher
	wrappedWatcher.Start(ctx)

	// Give the goroutine a moment to start
	time.Sleep(10 * time.Millisecond)

	// Cancel context to stop the polling
	cancel()

	// Close to cleanup
	err = wrappedWatcher.Close(ctx)
	assert.NoError(t, err)
}

func TestPostgreSQLEventAdapter_extractActualDocument(t *testing.T) {
	tests := []struct {
		name     string
		docMap   map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "extracts nested document and preserves id",
			docMap: map[string]interface{}{
				"id":       "test-uuid-123",
				"node_name": "test-node",
				"document": map[string]interface{}{
					"healthevent": map[string]interface{}{
						"nodeName": "test-node",
						"checkName": "TestCheck",
					},
					"healtheventstatus": map[string]interface{}{
						"status": "healthy",
					},
				},
			},
			expected: map[string]interface{}{
				"_id": "test-uuid-123",
				"healthevent": map[string]interface{}{
					"nodeName": "test-node",
					"checkName": "TestCheck",
				},
				"healtheventstatus": map[string]interface{}{
					"status": "healthy",
				},
			},
		},
		{
			name: "returns whole docMap when no nested document",
			docMap: map[string]interface{}{
				"id":       "test-uuid-456",
				"nodeName": "test-node-2",
			},
			expected: map[string]interface{}{
				"id":       "test-uuid-456",
				"nodeName": "test-node-2",
			},
		},
		{
			name: "handles missing id field gracefully",
			docMap: map[string]interface{}{
				"node_name": "test-node",
				"document": map[string]interface{}{
					"healthevent": map[string]interface{}{
						"checkName": "TestCheck",
					},
				},
			},
			expected: map[string]interface{}{
				"healthevent": map[string]interface{}{
					"checkName": "TestCheck",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &PostgreSQLEventAdapter{
				eventData: map[string]interface{}{},
			}

			result := adapter.extractActualDocument(tt.docMap)

			assert.Equal(t, tt.expected, result)
		})
	}
}

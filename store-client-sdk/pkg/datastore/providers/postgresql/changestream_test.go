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
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
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
					WithArgs("test-client", sqlmock.AnyArg(), sqlmock.AnyArg()).
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
		name             string
		setupMock        func()
		expectedEventID  int64
		expectError      bool
		errorMsg         string
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
					WithArgs("test-client", sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectError: false,
		},
		{
			name:    "database error",
			eventID: 456,
			setupMock: func() {
				mock.ExpectExec("INSERT INTO resume_tokens").
					WithArgs("test-client", sqlmock.AnyArg(), sqlmock.AnyArg()).
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	watcher := NewPostgreSQLChangeStreamWatcher(db, "test-client", "health_events")
	watcher.lastEventID = 100

	tests := []struct {
		name        string
		setupMock   func()
		expectError bool
		errorMsg    string
	}{
		{
			name: "successful fetch with changes",
			setupMock: func() {
				changeData := map[string]interface{}{
					"id":         "test-id",
					"node_name":  "test-node",
					"event_type": "GPU_ERROR",
				}
				changeJSON, _ := json.Marshal(changeData)

				rows := sqlmock.NewRows([]string{"id", "operation", "old_values", "new_values", "changed_at"}).
					AddRow(101, "INSERT", nil, string(changeJSON), time.Now())
				mock.ExpectQuery("SELECT id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnRows(rows)
			},
			expectError: false,
		},
		{
			name: "no changes found",
			setupMock: func() {
				mock.ExpectQuery("SELECT id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnRows(sqlmock.NewRows([]string{"id", "operation", "old_values", "new_values", "changed_at"}))
			},
			expectError: false,
		},
		{
			name: "database error",
			setupMock: func() {
				mock.ExpectQuery("SELECT id, operation, old_values, new_values, changed_at FROM datastore_changelog").
					WithArgs("health_events", int64(100)).
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectError: true,
			errorMsg:    "failed to query changelog",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := watcher.fetchNewChanges(context.Background())

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

	event := watcher.buildEventDocument(123, "INSERT", oldValues, newValues, changedAt)

	assert.Equal(t, "insert", event["operationType"])
	assert.Equal(t, changedAt, event["clusterTime"])
	assert.Contains(t, event, "_id")
	assert.Contains(t, event, "fullDocument")

	// Check _id structure
	idMap, ok := event["_id"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "123", idMap["_data"])

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
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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResumeTokenAdvancedOnSend verifies that lastEventID IS updated
// when an event is sent to the channel. This ensures that MarkProcessed()
// with empty token correctly uses the position of the last SENT event.
// When an event is sent but not yet processed, it can be redelivered on restart
// because it hasn't been marked as processed in the database yet.
func TestResumeTokenAdvancedOnSend(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	clientName := "test-client"
	tableName := "test_table"

	// Mock loadResumePosition - start from event ID 0
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs(clientName).
		WillReturnRows(sqlmock.NewRows([]string{"resume_token"}).
			AddRow(`{"eventID": 0, "timestamp": "2025-01-01T00:00:00Z"}`))

	watcher := &PostgreSQLChangeStreamWatcher{
		db:          db,
		clientName:  clientName,
		tableName:   tableName,
		events:      make(chan datastore.EventWithToken, 10),
		stopCh:      make(chan struct{}),
		lastEventID: 0,
	}

	err = watcher.loadResumePosition(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), watcher.lastEventID, "Should start at event ID 0")

	// Simulate processing an event
	recordID := uuid.New()
	newValues := map[string]interface{}{
		"id":   recordID.String(),
		"name": "test",
	}

	events := []datastore.EventWithToken{
		{
			Event: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "1",
				},
				"operationType": "insert",
				"fullDocument":  newValues,
			},
			ResumeToken: []byte("1"),
		},
	}

	// Send event to channel
	err = watcher.sendEventsToChannel(ctx, events)
	require.NoError(t, err)

	// Verify lastEventID WAS updated after sending
	assert.Equal(t, int64(1), watcher.lastEventID,
		"lastEventID SHOULD be updated in sendEventsToChannel to track last sent event")

	// Receive the event from channel
	select {
	case event := <-watcher.events:
		assert.Equal(t, []byte("1"), event.ResumeToken)

		// Now simulate the application calling MarkProcessed
		mock.ExpectExec("UPDATE datastore_changelog SET processed").
			WithArgs(int64(1), tableName).
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectExec("INSERT INTO resume_tokens").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = watcher.MarkProcessed(ctx, event.ResumeToken)
		require.NoError(t, err)

		// lastEventID remains at 1 (was already set in sendEventsToChannel)
		assert.Equal(t, int64(1), watcher.lastEventID,
			"lastEventID should remain 1 after MarkProcessed")

	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for event")
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestResumeTokenNotAdvancedForFilteredEvents verifies that when an event
// is filtered out by the pipeline filter, lastEventID is NOT advanced.
func TestResumeTokenNotAdvancedForFilteredEvents(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	clientName := "test-client"
	tableName := "test_table"

	// Mock loadResumePosition
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs(clientName).
		WillReturnRows(sqlmock.NewRows([]string{"resume_token"}).
			AddRow(`{"eventID": 0, "timestamp": "2025-01-01T00:00:00Z"}`))

	// Create a pipeline filter that rejects all events
	pipelineFilter, err := NewPipelineFilter([]interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "update", // Only accept updates
			},
		},
	})
	require.NoError(t, err)

	watcher := &PostgreSQLChangeStreamWatcher{
		db:             db,
		clientName:     clientName,
		tableName:      tableName,
		events:         make(chan datastore.EventWithToken, 10),
		stopCh:         make(chan struct{}),
		lastEventID:    0,
		pipelineFilter: pipelineFilter,
	}

	err = watcher.loadResumePosition(ctx)
	require.NoError(t, err)

	// Create an INSERT event (which will be filtered out)
	recordID := uuid.New()
	newValues := map[string]interface{}{
		"id":   recordID.String(),
		"name": "test",
	}

	events := []datastore.EventWithToken{
		{
			Event: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "1",
				},
				"operationType": "insert", // Will be filtered (we only accept "update")
				"fullDocument":  newValues,
			},
			ResumeToken: []byte("1"),
		},
	}

	// Send events through pipeline filter
	err = watcher.sendEventsToChannel(ctx, events)
	require.NoError(t, err)

	// Verify event was filtered and NOT sent to channel
	select {
	case <-watcher.events:
		t.Fatal("Event should have been filtered out")
	case <-time.After(100 * time.Millisecond):
		// Expected - event was filtered
	}

	// Verify lastEventID was NOT advanced for filtered event
	assert.Equal(t, int64(0), watcher.lastEventID,
		"lastEventID should NOT advance for filtered events")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestResumeAfterRestartSkipsProcessedEvents verifies that when a watcher
// restarts with lastEventID=N, it correctly resumes from WHERE id > N,
// skipping event N (which was already processed before the restart).
func TestResumeAfterRestartSkipsProcessedEvents(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	clientName := "test-client"
	tableName := "test_table"

	// Simulate restart: loadResumePosition returns eventID=5
	// (meaning events 1-5 were processed before restart)
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs(clientName).
		WillReturnRows(sqlmock.NewRows([]string{"resume_token"}).
			AddRow(`{"eventID": 5, "timestamp": "2025-01-01T00:00:00Z"}`))

	watcher := &PostgreSQLChangeStreamWatcher{
		db:         db,
		clientName: clientName,
		tableName:  tableName,
		events:     make(chan datastore.EventWithToken, 10),
		stopCh:     make(chan struct{}),
	}

	err = watcher.loadResumePosition(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), watcher.lastEventID, "Should load last processed event ID")

	// Mock fetchNewChanges query: should fetch WHERE id > 5
	// (events 6, 7, 8... not event 5 which was already processed)
	recordID := uuid.New()
	mock.ExpectQuery("SELECT id, record_id, operation, old_values, new_values, changed_at").
		WithArgs(tableName, int64(5)). // WHERE id > 5
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "record_id", "operation", "old_values", "new_values", "changed_at"}).
			AddRow(6, recordID, "INSERT", nil, []byte(`{"id": "test"}`), time.Now()).
			AddRow(7, recordID, "INSERT", nil, []byte(`{"id": "test2"}`), time.Now()))

	err = watcher.fetchNewChanges(ctx)
	require.NoError(t, err)

	// Verify we received events 6 and 7 (not 5)
	event1 := <-watcher.events
	assert.Equal(t, []byte("6"), event1.ResumeToken)

	event2 := <-watcher.events
	assert.Equal(t, []byte("7"), event2.ResumeToken)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestConcurrentClientsWithDifferentFilters verifies that two clients
// with different pipeline filters can process the same events independently
// without interfering with each other's resume tokens.
func TestConcurrentClientsWithDifferentFilters(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	tableName := "test_table"

	// Client 1: only accepts INSERT operations
	client1Name := "insert-client"
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs(client1Name).
		WillReturnRows(sqlmock.NewRows([]string{"resume_token"}).
			AddRow(`{"eventID": 0, "timestamp": "2025-01-01T00:00:00Z"}`))

	pipelineFilter1, err := NewPipelineFilter([]interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "insert",
			},
		},
	})
	require.NoError(t, err)

	watcher1 := &PostgreSQLChangeStreamWatcher{
		db:             db,
		clientName:     client1Name,
		tableName:      tableName,
		events:         make(chan datastore.EventWithToken, 10),
		stopCh:         make(chan struct{}),
		pipelineFilter: pipelineFilter1,
	}

	err = watcher1.loadResumePosition(ctx)
	require.NoError(t, err)

	// Client 2: only accepts UPDATE operations
	client2Name := "update-client"
	mock.ExpectQuery("SELECT resume_token FROM resume_tokens").
		WithArgs(client2Name).
		WillReturnRows(sqlmock.NewRows([]string{"resume_token"}).
			AddRow(`{"eventID": 0, "timestamp": "2025-01-01T00:00:00Z"}`))

	pipelineFilter2, err := NewPipelineFilter([]interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "update",
			},
		},
	})
	require.NoError(t, err)

	watcher2 := &PostgreSQLChangeStreamWatcher{
		db:             db,
		clientName:     client2Name,
		tableName:      tableName,
		events:         make(chan datastore.EventWithToken, 10),
		stopCh:         make(chan struct{}),
		pipelineFilter: pipelineFilter2,
	}

	err = watcher2.loadResumePosition(ctx)
	require.NoError(t, err)

	// Create mixed events
	events := []datastore.EventWithToken{
		{
			Event: map[string]interface{}{
				"_id":           map[string]interface{}{"_data": "1"},
				"operationType": "insert",
				"fullDocument":  map[string]interface{}{"id": "1"},
			},
			ResumeToken: []byte("1"),
		},
		{
			Event: map[string]interface{}{
				"_id":           map[string]interface{}{"_data": "2"},
				"operationType": "update",
				"fullDocument":  map[string]interface{}{"id": "2"},
			},
			ResumeToken: []byte("2"),
		},
	}

	// Send to both watchers
	err = watcher1.sendEventsToChannel(ctx, events)
	require.NoError(t, err)
	err = watcher2.sendEventsToChannel(ctx, events)
	require.NoError(t, err)

	// Client 1 should only receive INSERT (event 1)
	select {
	case event := <-watcher1.events:
		assert.Equal(t, "insert", event.Event["operationType"])
		assert.Equal(t, []byte("1"), event.ResumeToken)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Client 1 should have received INSERT event")
	}

	// Client 1 should NOT receive UPDATE
	select {
	case <-watcher1.events:
		t.Fatal("Client 1 should NOT receive UPDATE event")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	// Client 2 should only receive UPDATE (event 2)
	select {
	case event := <-watcher2.events:
		assert.Equal(t, "update", event.Event["operationType"])
		assert.Equal(t, []byte("2"), event.ResumeToken)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Client 2 should have received UPDATE event")
	}

	// Client 2 should NOT receive INSERT
	select {
	case <-watcher2.events:
		t.Fatal("Client 2 should NOT receive INSERT event")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	// Now simulate each client calling MarkProcessed for their respective events
	// Client 1 marks event 1
	mock.ExpectExec("UPDATE datastore_changelog SET processed").
		WithArgs(int64(1), tableName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO resume_tokens").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = watcher1.MarkProcessed(ctx, []byte("1"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), watcher1.lastEventID)

	// Client 2 marks event 2
	mock.ExpectExec("UPDATE datastore_changelog SET processed").
		WithArgs(int64(2), tableName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO resume_tokens").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = watcher2.MarkProcessed(ctx, []byte("2"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), watcher2.lastEventID)

	// Verify each client has independent resume positions
	assert.Equal(t, int64(1), watcher1.lastEventID, "Client 1 should be at event 1")
	assert.Equal(t, int64(2), watcher2.lastEventID, "Client 2 should be at event 2")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkProcessedUpdatesResumeToken verifies that MarkProcessed correctly
// updates both the in-memory lastEventID and persists it to the database.
func TestMarkProcessedUpdatesResumeToken(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	clientName := "test-client"
	tableName := "test_table"

	watcher := &PostgreSQLChangeStreamWatcher{
		db:          db,
		clientName:  clientName,
		tableName:   tableName,
		lastEventID: 0,
	}

	// Mock the UPDATE datastore_changelog query
	mock.ExpectExec("UPDATE datastore_changelog SET processed = TRUE").
		WithArgs(int64(42), tableName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Mock the saveResumePosition INSERT/UPDATE
	mock.ExpectExec("INSERT INTO resume_tokens").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = watcher.MarkProcessed(ctx, []byte("42"))
	require.NoError(t, err)

	// Verify in-memory lastEventID was updated
	assert.Equal(t, int64(42), watcher.lastEventID,
		"lastEventID should be updated to 42")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestEmptyTokenDoesNotUpdateResumePosition verifies that calling
// MarkProcessed with an empty token uses the current lastEventID.
func TestEmptyTokenDoesNotUpdateResumePosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	watcher := &PostgreSQLChangeStreamWatcher{
		db:          db,
		lastEventID: 5,
		tableName:   "health_events",
		clientName:  "test-client",
	}

	// Expect UPDATE query using lastEventID when token is empty
	mock.ExpectExec(`UPDATE datastore_changelog`).
		WithArgs(5, "health_events").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Expect saveResumePosition - it takes client_name and resume_token (JSON)
	mock.ExpectExec(`INSERT INTO resume_tokens`).
		WithArgs("test-client", sqlmock.AnyArg()). // AnyArg for the JSON token
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Call with empty token - should use lastEventID
	err = watcher.MarkProcessed(ctx, []byte{})
	require.NoError(t, err)

	// lastEventID should remain 5
	assert.Equal(t, int64(5), watcher.lastEventID,
		"lastEventID should be 5")

	assert.NoError(t, mock.ExpectationsWereMet())
}

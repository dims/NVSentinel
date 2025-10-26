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

package common

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	platformconnector "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	"github.com/stretchr/testify/suite"
)

// BaseDataStoreTestSuite provides common test patterns for all datastore implementations
type BaseDataStoreTestSuite struct {
	suite.Suite
	DataStore    datastore.DataStore
	Ctx          context.Context
	SkipEnvVar   string
	SkipMessage  string
	SetupFunc    func() (datastore.DataStore, error)
	TeardownFunc func()
}

// SetupSuite initializes the test suite
func (s *BaseDataStoreTestSuite) SetupSuite() {
	s.Ctx = context.Background()

	// Check if we should skip integration tests
	if s.SkipEnvVar != "" && os.Getenv(s.SkipEnvVar) != "" {
		s.T().Skip(s.SkipMessage)
	}

	if s.SetupFunc != nil {
		var err error

		s.DataStore, err = s.SetupFunc()
		if err != nil {
			s.T().Skipf("Failed to setup datastore: %v", err)
		}
	}
}

// TearDownSuite cleans up the test suite
func (s *BaseDataStoreTestSuite) TearDownSuite() {
	if s.DataStore != nil {
		s.DataStore.Close(s.Ctx)
	}

	if s.TeardownFunc != nil {
		s.TeardownFunc()
	}
}

// TestConnectionAndPing tests basic database connectivity
func (s *BaseDataStoreTestSuite) TestConnectionAndPing() {
	if s.DataStore == nil {
		s.T().Skip("No datastore available")
	}

	err := s.DataStore.Ping(s.Ctx)
	s.Assert().NoError(err, "Database ping should succeed")
}

// TestMaintenanceEventOperations tests maintenance event CRUD operations
func (s *BaseDataStoreTestSuite) TestMaintenanceEventOperations() {
	if s.DataStore == nil {
		s.T().Skip("No datastore available")
	}

	store := s.DataStore.MaintenanceEventStore()

	// Create test event
	event := CreateTestMaintenanceEvent("test-event-1", "test-node-1")

	// Test upsert (insert)
	err := store.UpsertMaintenanceEvent(s.Ctx, event)
	s.Assert().NoError(err, "Upsert should succeed")

	// Test upsert (update)
	event.Status = model.StatusQuarantineTriggered
	err = store.UpsertMaintenanceEvent(s.Ctx, event)
	s.Assert().NoError(err, "Upsert update should succeed")

	// Test status update
	err = store.UpdateEventStatus(s.Ctx, event.EventID, model.StatusMaintenanceOngoing)
	s.Assert().NoError(err, "Status update should succeed")

	// Test finding latest ongoing event
	foundEvent, found, err := store.FindLatestOngoingEventByNode(s.Ctx, "test-node-1")
	s.Assert().NoError(err, "Find latest ongoing event should succeed")
	s.Assert().True(found, "Event should be found")
	s.Assert().Equal(model.StatusMaintenanceOngoing, foundEvent.Status, "Status should be updated")
}

// TestHealthEventOperations tests health event operations
func (s *BaseDataStoreTestSuite) TestHealthEventOperations() {
	if s.DataStore == nil {
		s.T().Skip("No datastore available")
	}

	healthStore := s.DataStore.HealthEventStore()

	// Create test health event
	eventWithStatus := CreateTestHealthEvent("test-node-1")

	// Test insert
	err := healthStore.InsertHealthEvents(s.Ctx, eventWithStatus)
	s.Assert().NoError(err, "Insert health event should succeed")

	// Test find by node
	events, err := healthStore.FindHealthEventsByNode(s.Ctx, "test-node-1")
	s.Assert().NoError(err, "Find by node should succeed")
	s.Assert().Len(events, 1, "Should find one event")

	// Type assert to get the concrete type
	healthEvent, ok := events[0].HealthEvent.(*platformconnector.HealthEvent)
	s.Assert().True(ok, "HealthEvent should be of type *platformconnector.HealthEvent")
	s.Assert().Equal("test-node-1", healthEvent.NodeName, "Node name should match")
}

// TestConfigValidation tests configuration validation with common test cases
func (s *BaseDataStoreTestSuite) TestConfigValidation() {
	testCases := GetCommonConfigValidationTestCases()

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			err := datastore.ValidateConfig(tc.Config)
			if tc.ExpectError {
				s.Assert().Error(err, "Expected validation error")
			} else {
				s.Assert().NoError(err, "Expected no validation error")
			}
		})
	}
}

// CreateTestMaintenanceEvent creates a standardized test maintenance event
func CreateTestMaintenanceEvent(eventID, nodeName string) *model.MaintenanceEvent {
	return &model.MaintenanceEvent{
		EventID:                eventID,
		CSP:                    model.CSPAWS,
		ClusterName:            "test-cluster",
		NodeName:               nodeName,
		Status:                 model.StatusDetected,
		CSPStatus:              model.CSPStatusPending,
		MaintenanceType:        model.TypeScheduled,
		EventReceivedTimestamp: time.Now().UTC(),
		LastUpdatedTimestamp:   time.Now().UTC(),
		Metadata:               map[string]string{"test": "value"},
	}
}

// CreateTestHealthEvent creates a standardized test health event
func CreateTestHealthEvent(nodeName string) *datastore.HealthEventWithStatus {
	return &datastore.HealthEventWithStatus{
		CreatedAt: time.Now().UTC(),
		HealthEvent: &platformconnector.HealthEvent{
			NodeName:       nodeName,
			Agent:          "test-agent",
			ComponentClass: "GPU",
			CheckName:      "GPU_ERROR",
			IsFatal:        true,
			Message:        "Test GPU error",
		},
		HealthEventStatus: datastore.HealthEventStatus{
			UserPodsEvictionStatus: datastore.OperationStatus{
				Status:  datastore.StatusNotStarted,
				Message: "Not started",
			},
		},
	}
}

// ConfigValidationTestCase represents a configuration validation test case
type ConfigValidationTestCase struct {
	Name        string
	Config      datastore.DataStoreConfig
	ExpectError bool
}

// GetCommonConfigValidationTestCases returns common configuration validation test cases
func GetCommonConfigValidationTestCases() []ConfigValidationTestCase {
	return []ConfigValidationTestCase{
		{
			Name: "missing provider",
			Config: datastore.DataStoreConfig{
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Database: "test",
				},
			},
			ExpectError: true,
		},
		{
			Name: "missing host",
			Config: datastore.DataStoreConfig{
				Provider: "testprovider",
				Connection: datastore.ConnectionConfig{
					Database: "test",
				},
			},
			ExpectError: true,
		},
		{
			Name: "missing database",
			Config: datastore.DataStoreConfig{
				Provider: "testprovider",
				Connection: datastore.ConnectionConfig{
					Host: "localhost",
				},
			},
			ExpectError: true,
		},
	}
}

// MockChangeStreamWatcher provides a mock implementation for testing
type MockChangeStreamWatcher struct {
	events   chan datastore.EventWithToken
	closed   bool
	closeErr error
}

// NewMockChangeStreamWatcher creates a new mock change stream watcher
func NewMockChangeStreamWatcher() *MockChangeStreamWatcher {
	return &MockChangeStreamWatcher{
		events: make(chan datastore.EventWithToken, 10),
		closed: false,
	}
}

// Events returns the events channel
func (m *MockChangeStreamWatcher) Events() <-chan datastore.EventWithToken {
	return m.events
}

// Start begins watching (mock implementation)
func (m *MockChangeStreamWatcher) Start(ctx context.Context) {
	// Mock implementation - no-op
}

// Close closes the watcher
func (m *MockChangeStreamWatcher) Close(ctx context.Context) error {
	if m.closed {
		return nil
	}

	m.closed = true
	close(m.events)

	return m.closeErr
}

// MarkProcessed marks events as processed
func (m *MockChangeStreamWatcher) MarkProcessed(ctx context.Context, token []byte) error {
	return nil
}

// SendEvent sends a mock event (for testing)
func (m *MockChangeStreamWatcher) SendEvent(event map[string]interface{}) {
	if !m.closed {
		eventWithToken := datastore.EventWithToken{
			Event:       event,
			ResumeToken: nil, // Mock events don't have real tokens
		}
		select {
		case m.events <- eventWithToken:
		default:
		}
	}
}

// TestChangeStreamWatcher provides a common test for change stream functionality
func (s *BaseDataStoreTestSuite) TestChangeStreamWatcher(createWatcher func() (datastore.ChangeStreamWatcher, error)) {
	if s.DataStore == nil {
		s.T().Skip("No datastore available")
	}

	watcher, err := createWatcher()
	s.Require().NoError(err, "Creating change stream watcher should succeed")

	// Start watching
	watcher.Start(s.Ctx)
	defer watcher.Close(s.Ctx)

	// Insert an event to trigger change
	event := CreateTestMaintenanceEvent("test-change-event", "test-node-change")

	err = s.DataStore.MaintenanceEventStore().UpsertMaintenanceEvent(s.Ctx, event)
	s.Require().NoError(err, "Upsert should succeed")

	// Wait for change event
	var changeEvent datastore.EventWithToken
	select {
	case changeEvent = <-watcher.Events():
		s.Assert().NotNil(changeEvent.Event, "Should receive change event")
	case <-time.After(5 * time.Second):
		s.T().Fatal("Timeout waiting for change event")
	}

	// Test mark processed (using token from the event)
	err = watcher.MarkProcessed(s.Ctx, changeEvent.ResumeToken)
	s.Assert().NoError(err, "Mark processed should succeed")
}

// AssertMaintenanceEvent validates a maintenance event has expected properties
func AssertMaintenanceEvent(t *testing.T, event *model.MaintenanceEvent, expectedID, expectedNode string) {
	if event == nil {
		t.Fatal("Event should not be nil")
	}

	if event.EventID != expectedID {
		t.Errorf("Expected EventID %s, got %s", expectedID, event.EventID)
	}

	if event.NodeName != expectedNode {
		t.Errorf("Expected NodeName %s, got %s", expectedNode, event.NodeName)
	}
}

// AssertHealthEvent validates a health event has expected properties
func AssertHealthEvent(t *testing.T, eventWithStatus *datastore.HealthEventWithStatus, expectedNode string) {
	if eventWithStatus == nil {
		t.Fatal("Event should not be nil")
	}

	healthEvent, ok := eventWithStatus.HealthEvent.(*platformconnector.HealthEvent)
	if !ok {
		t.Fatal("HealthEvent should be of type *platformconnector.HealthEvent")
	}

	if healthEvent.NodeName != expectedNode {
		t.Errorf("Expected NodeName %s, got %s", expectedNode, healthEvent.NodeName)
	}
}

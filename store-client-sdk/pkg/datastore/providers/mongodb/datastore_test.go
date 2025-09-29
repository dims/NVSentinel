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

package mongodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
	platformconnector "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// MongoDBTestSuite contains tests for MongoDB datastore
type MongoDBTestSuite struct {
	suite.Suite
	datastore datastore.DataStore
	ctx       context.Context
}

// SetupSuite initializes the test suite with a test database
func (s *MongoDBTestSuite) SetupSuite() {
	s.ctx = context.Background()

	// Check if we should skip integration tests
	if os.Getenv("SKIP_MONGODB_TESTS") != "" {
		s.T().Skip("Skipping MongoDB integration tests")
	}

	// Use test database configuration
	config := datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			Host:     getEnvOrDefault("MONGODB_TEST_URI", "mongodb://localhost:27017"),
			Database: getEnvOrDefault("MONGODB_TEST_DB", "nvsentinel_test"),
		},
	}

	var err error
	s.datastore, err = NewMongoDBDataStore(s.ctx, config)
	if err != nil {
		s.T().Skipf("Failed to connect to test database: %v", err)
	}
}

// TearDownSuite cleans up the test suite
func (s *MongoDBTestSuite) TearDownSuite() {
	if s.datastore != nil {
		s.datastore.Close(s.ctx)
	}
}

// TestConnectionAndPing tests basic database connectivity
func (s *MongoDBTestSuite) TestConnectionAndPing() {
	if s.datastore == nil {
		s.T().Skip("No datastore available")
	}

	err := s.datastore.Ping(s.ctx)
	s.Assert().NoError(err, "Database ping should succeed")
}

// TestMaintenanceEventOperations tests maintenance event CRUD operations
func (s *MongoDBTestSuite) TestMaintenanceEventOperations() {
	if s.datastore == nil {
		s.T().Skip("No datastore available")
	}

	store := s.datastore.MaintenanceEventStore()

	// Create test event
	event := &model.MaintenanceEvent{
		EventID:                "test-event-1",
		CSP:                    model.CSPAWS,
		ClusterName:            "test-cluster",
		NodeName:               "test-node-1",
		Status:                 model.StatusDetected,
		CSPStatus:              model.CSPStatusPending,
		MaintenanceType:        model.TypeScheduled,
		EventReceivedTimestamp: time.Now().UTC(),
		LastUpdatedTimestamp:   time.Now().UTC(),
		Metadata:               map[string]string{"test": "value"},
	}

	// Test upsert (insert)
	err := store.UpsertMaintenanceEvent(s.ctx, event)
	s.Assert().NoError(err, "Upsert should succeed")

	// Test upsert (update)
	event.Status = model.StatusQuarantineTriggered
	err = store.UpsertMaintenanceEvent(s.ctx, event)
	s.Assert().NoError(err, "Upsert update should succeed")

	// Test status update
	err = store.UpdateEventStatus(s.ctx, event.EventID, model.StatusMaintenanceOngoing)
	s.Assert().NoError(err, "Status update should succeed")

	// Test finding latest ongoing event
	foundEvent, found, err := store.FindLatestOngoingEventByNode(s.ctx, "test-node-1")
	s.Assert().NoError(err, "Find latest ongoing event should succeed")
	s.Assert().True(found, "Event should be found")
	s.Assert().Equal(model.StatusMaintenanceOngoing, foundEvent.Status, "Status should be updated")
}

// TestHealthEventOperations tests health event operations
func (s *MongoDBTestSuite) TestHealthEventOperations() {
	if s.datastore == nil {
		s.T().Skip("No datastore available")
	}

	healthStore := s.datastore.HealthEventStore()

	// Create test health event
	eventWithStatus := &datastore.HealthEventWithStatus{
		CreatedAt: time.Now().UTC(),
		HealthEvent: &platformconnector.HealthEvent{
			NodeName:       "test-node-1",
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

	// Test insert
	err := healthStore.InsertHealthEvents(s.ctx, eventWithStatus)
	s.Assert().NoError(err, "Insert health event should succeed")

	// Test find by node
	events, err := healthStore.FindHealthEventsByNode(s.ctx, "test-node-1")
	s.Assert().NoError(err, "Find by node should succeed")
	s.Assert().Len(events, 1, "Should find one event")
	// Type assert to get the concrete type
	healthEvent, ok := events[0].HealthEvent.(*platformconnector.HealthEvent)
	s.Assert().True(ok, "HealthEvent should be of type *platformconnector.HealthEvent")
	s.Assert().Equal("test-node-1", healthEvent.NodeName, "Node name should match")
}

// TestConfigValidation tests configuration validation
func (s *MongoDBTestSuite) TestConfigValidation() {
	testCases := []struct {
		name        string
		config      datastore.DataStoreConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderMongoDB,
				Connection: datastore.ConnectionConfig{
					Host:     "mongodb://localhost:27017",
					Database: "test",
				},
			},
			expectError: false,
		},
		{
			name: "missing provider",
			config: datastore.DataStoreConfig{
				Connection: datastore.ConnectionConfig{
					Host:     "mongodb://localhost:27017",
					Database: "test",
				},
			},
			expectError: true,
		},
		{
			name: "missing host",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderMongoDB,
				Connection: datastore.ConnectionConfig{
					Database: "test",
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			err := datastore.ValidateConfig(tc.config)
			if tc.expectError {
				assert.Error(t, err, "Expected validation error")
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// TestRunMongoDBTestSuite runs the MongoDB test suite
func TestMongoDBTestSuite(t *testing.T) {
	suite.Run(t, new(MongoDBTestSuite))
}

// TestNewMongoDBDataStore tests the MongoDB datastore constructor
func TestNewMongoDBDataStore(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		config      datastore.DataStoreConfig
		expectError bool
		skipTest    bool
	}{
		{
			name: "valid config with URI in host",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderMongoDB,
				Connection: datastore.ConnectionConfig{
					Host:     "mongodb://localhost:27017",
					Database: "test",
				},
			},
			expectError: false,
			skipTest:    true, // Skip if no MongoDB available
		},
		{
			name: "config with TLS cert path",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderMongoDB,
				Connection: datastore.ConnectionConfig{
					Host:     "mongodb://localhost:27017",
					Database: "test",
					TLSConfig: &datastore.TLSConfig{
						CertPath: "/tmp/test.crt",
						KeyPath:  "/tmp/test.key",
						CAPath:   "/tmp/ca.crt",
					},
				},
			},
			expectError: true, // Will fail due to missing cert files
			skipTest:    false,
		},
		{
			name: "config with empty host defaults to localhost",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderMongoDB,
				Connection: datastore.ConnectionConfig{
					Host:     "",
					Database: "test",
				},
			},
			expectError: false,
			skipTest:    true, // Skip if no MongoDB available
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewMongoDBDataStore(ctx, tt.config)

			if tt.expectError {
				assert.Error(t, err, "Expected error for test case: %s", tt.name)
				assert.Nil(t, ds, "Datastore should be nil on error")
			} else {
				if tt.skipTest && err != nil {
					t.Skipf("MongoDB not available for testing: %v", err)
					return
				}
				assert.NoError(t, err, "Should create MongoDB datastore successfully")
				assert.NotNil(t, ds, "Datastore should not be nil")

				if ds != nil {
					// Test basic functionality
					pingErr := ds.Ping(ctx)
					if pingErr != nil {
						t.Skipf("Cannot ping MongoDB: %v", pingErr)
						return
					}

					// Cleanup
					ds.Close(ctx)
				}
			}
		})
	}
}

// TestMongoDBProviderRegistration tests that the MongoDB provider is properly registered
func TestMongoDBProviderRegistration(t *testing.T) {
	config := datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			Host:     "mongodb://localhost:27017",
			Database: "test",
		},
	}

	// This should not panic and should return either a valid datastore or an error
	_, err := NewMongoDBDataStore(context.Background(), config)

	// We expect either success (if MongoDB is available) or a connection error
	// We should NOT get a "provider not found" type error
	if err != nil {
		assert.NotContains(t, err.Error(), "provider", "Should not be a provider registration error")
		assert.NotContains(t, err.Error(), "unsupported", "Should not be an unsupported provider error")
	}
}

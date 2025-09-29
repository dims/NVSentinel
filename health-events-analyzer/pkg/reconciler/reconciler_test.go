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

package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	platform_connectors "github.com/nvidia/nvsentinel/data-models/pkg/protos"

	// Generic datastore interfaces (no MongoDB dependencies)
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Mock Publisher
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) HealthEventOccuredV1(ctx context.Context, events *platform_connectors.HealthEvents, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	args := m.Called(ctx, events)
	return args.Get(0).(*emptypb.Empty), args.Error(1)
}

// Mock DataStore and HealthEventStore
type mockDataStore struct {
	mock.Mock
}

func (m *mockDataStore) MaintenanceEventStore() datastore.MaintenanceEventStore {
	args := m.Called()
	return args.Get(0).(datastore.MaintenanceEventStore)
}

func (m *mockDataStore) HealthEventStore() datastore.HealthEventStore {
	args := m.Called()
	return args.Get(0).(datastore.HealthEventStore)
}

func (m *mockDataStore) InsertMany(ctx context.Context, documents []interface{}) error {
	args := m.Called(ctx, documents)
	return args.Error(0)
}

func (m *mockDataStore) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockDataStore) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type mockHealthEventStore struct {
	mock.Mock
}

func (m *mockHealthEventStore) InsertHealthEvents(ctx context.Context, events *datastore.HealthEventWithStatus) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *mockHealthEventStore) UpdateHealthEventStatus(ctx context.Context, id string, status datastore.HealthEventStatus) error {
	args := m.Called(ctx, id, status)
	return args.Error(0)
}

func (m *mockHealthEventStore) UpdateHealthEventStatusByNode(ctx context.Context, nodeName string, status datastore.HealthEventStatus) error {
	args := m.Called(ctx, nodeName, status)
	return args.Error(0)
}

func (m *mockHealthEventStore) FindHealthEventsByNode(ctx context.Context, nodeName string) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, nodeName)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

func (m *mockHealthEventStore) FindHealthEventsByFilter(ctx context.Context, filter map[string]interface{}) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

func (m *mockHealthEventStore) FindHealthEventsByStatus(ctx context.Context, status datastore.Status) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, status)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

var (
	rules = []config.HealthEventsAnalyzerRule{
		{
			Name:        "rule1",
			Description: "check the occurrence of XID error 13",
			TimeWindow:  "2m",
			Sequence: []config.SequenceStep{{
				Criteria: map[string]interface{}{
					"healthevent.entitiesimpacted.0.entitytype":  "GPU",
					"healthevent.entitiesimpacted.0.entityvalue": "1",
					"healthevent.errorcode.0":                    "13",
					"healthevent.nodename":                       "this.healthevent.nodename",
				},
				ErrorCount: 3,
			}},
			RecommendedAction: "REPORT_ERROR",
		},
		{
			Name:        "rule2",
			Description: "check the occurrence of XID error 13 and XID error 31",
			TimeWindow:  "3m",
			Sequence: []config.SequenceStep{
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "1",
						"healthevent.errorcode.0":                    "13",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				},
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "1",
						"healthevent.errorcode.0":                    "31",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				}},
			RecommendedAction: "COMPONENT_RESET",
		},
	}
	healthEvent = datastore.HealthEventWithStatus{
		CreatedAt: time.Now(),
		HealthEvent: &platform_connectors.HealthEvent{
			NodeName: "node1",
			EntitiesImpacted: []*platform_connectors.Entity{{
				EntityType:  "GPU",
				EntityValue: "1",
			}},
			ErrorCode: []string{"13"},
			CheckName: "GpuXidError",
		},
	}
)

func TestEvaluateRule(t *testing.T) {
	ctx := context.TODO()

	mockDataStore := new(mockDataStore)
	mockHealthEventStore := new(mockHealthEventStore)

	// Setup mock to return our health event store
	mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)

	reconciler := &Reconciler{
		config: HealthEventsAnalyzerReconcilerConfig{
			DataStore: mockDataStore,
		},
	}

	t.Run("rule1 matches - enough events found", func(t *testing.T) {
		// Mock returning 5 events (more than the required 3)
		matchingEvents := make([]datastore.HealthEventWithStatus, 5)
		mockHealthEventStore.On("FindHealthEventsByFilter", ctx, mock.AnythingOfType("map[string]interface {}")).Return(matchingEvents, nil).Once()

		result := reconciler.evaluateRule(ctx, rules[0], healthEvent)
		assert.True(t, result)

		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})

	t.Run("rule1 does not match - not enough events", func(t *testing.T) {
		// Mock returning only 2 events (less than the required 3)
		matchingEvents := make([]datastore.HealthEventWithStatus, 2)
		mockHealthEventStore.On("FindHealthEventsByFilter", ctx, mock.AnythingOfType("map[string]interface {}")).Return(matchingEvents, nil).Once()

		result := reconciler.evaluateRule(ctx, rules[0], healthEvent)
		assert.False(t, result)

		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})

	t.Run("datastore query fails", func(t *testing.T) {
		mockHealthEventStore.On("FindHealthEventsByFilter", ctx, mock.AnythingOfType("map[string]interface {}")).Return([]datastore.HealthEventWithStatus{}, errors.New("query failed")).Once()

		result := reconciler.evaluateRule(ctx, rules[0], healthEvent)
		assert.False(t, result)

		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})

	t.Run("invalid time window", func(t *testing.T) {
		invalidRule := rules[0]
		invalidRule.TimeWindow = "invalid"

		result := reconciler.evaluateRule(ctx, invalidRule, healthEvent)
		assert.False(t, result)

		// Note: HealthEventStore may still be called during setup, just not for query
		// The important thing is that the rule evaluation returns false
	})
}

func TestBuildFilterFromSequenceCriteria(t *testing.T) {
	reconciler := &Reconciler{}

	criteria := map[string]interface{}{
		"healthevent.errorcode.0": "13",
		"healthevent.nodename":    "this.healthevent.nodename",
		"static_field":            "static_value",
	}

	timeWindow := 5 * time.Minute

	// Type assert for the test
	testHealthEvent, ok := healthEvent.HealthEvent.(*platform_connectors.HealthEvent)
	assert.True(t, ok, "HealthEvent should be of correct type")

	filter, err := reconciler.buildFilterFromSequenceCriteria(criteria, timeWindow, testHealthEvent)

	assert.NoError(t, err)
	assert.NotNil(t, filter)

	// Should have time window filter
	assert.Contains(t, filter, "created_at")

	// Should have static field
	assert.Equal(t, "static_value", filter["static_field"])

	// Should resolve "this." references
	assert.Equal(t, "node1", filter["healthevent.nodename"]) // Should be resolved to actual node name
}

func TestHandleEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("rule matches and event is published", func(t *testing.T) {
		mockDataStore := new(mockDataStore)
		mockHealthEventStore := new(mockHealthEventStore)
		mockPublisher := &mockPublisher{}

		// Type assert for this test scope
		expectedHealthEvent, ok := healthEvent.HealthEvent.(*platform_connectors.HealthEvent)
		assert.True(t, ok, "HealthEvent should be of correct type")

		expectedHealthEvents := &platform_connectors.HealthEvents{
			Version: 1,
			Events:  []*platform_connectors.HealthEvent{expectedHealthEvent},
		}
		mockPublisher.On("HealthEventOccuredV1", ctx, expectedHealthEvents).Return(&emptypb.Empty{}, nil)
		mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)

		// Mock enough events to satisfy the rule
		matchingEvents := make([]datastore.HealthEventWithStatus, 5)
		mockHealthEventStore.On("FindHealthEventsByFilter", ctx, mock.AnythingOfType("map[string]interface {}")).Return(matchingEvents, nil)

		reconciler := Reconciler{
			config: HealthEventsAnalyzerReconcilerConfig{
				HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
				DataStore:                 mockDataStore,
				Publisher:                 publisher.NewPublisher(mockPublisher),
			},
		}

		published, err := reconciler.handleEvent(ctx, &healthEvent)
		assert.NoError(t, err)
		assert.True(t, published)
		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("no rules match", func(t *testing.T) {
		nonMatchingEvent := datastore.HealthEventWithStatus{
			CreatedAt: time.Now(),
			HealthEvent: &platform_connectors.HealthEvent{
				NodeName: "node1",
				EntitiesImpacted: []*platform_connectors.Entity{{
					EntityType:  "GPU",
					EntityValue: "0",
				}},
				ErrorCode: []string{"43"}, // Different error code
				CheckName: "GpuXidError",
			},
		}

		reconciler := Reconciler{
			config: HealthEventsAnalyzerReconcilerConfig{
				HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
				Publisher:                 publisher.NewPublisher(&mockPublisher{}),
			},
		}

		published, err := reconciler.handleEvent(ctx, &nonMatchingEvent)
		assert.NoError(t, err)
		assert.False(t, published)
	})
}

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
	"fmt"
	"testing"
	"time"

	datamodels "github.com/nvidia/nvsentinel/data-models/pkg/model"
	protos "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mock Publisher
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) HealthEventOccurredV1(ctx context.Context, events *protos.HealthEvents, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	args := m.Called(ctx, events)
	return args.Get(0).(*emptypb.Empty), args.Error(1)
}

// Mock DatabaseClient
type mockDatabaseClient struct {
	mock.Mock
}

func (m *mockDatabaseClient) UpdateDocumentStatus(ctx context.Context, documentID string, statusPath string, status interface{}) error {
	args := m.Called(ctx, documentID, statusPath, status)
	return args.Error(0)
}

func (m *mockDatabaseClient) CountDocuments(ctx context.Context, filter interface{}, options *client.CountOptions) (int64, error) {
	args := m.Called(ctx, filter, options)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockDatabaseClient) UpdateDocument(ctx context.Context, filter interface{}, update interface{}) (*client.UpdateResult, error) {
	args := m.Called(ctx, filter, update)
	return args.Get(0).(*client.UpdateResult), args.Error(1)
}

func (m *mockDatabaseClient) UpsertDocument(ctx context.Context, filter interface{}, document interface{}) (*client.UpdateResult, error) {
	args := m.Called(ctx, filter, document)
	return args.Get(0).(*client.UpdateResult), args.Error(1)
}

func (m *mockDatabaseClient) FindOne(ctx context.Context, filter interface{}, options *client.FindOneOptions) (client.SingleResult, error) {
	args := m.Called(ctx, filter, options)
	return args.Get(0).(client.SingleResult), args.Error(1)
}

func (m *mockDatabaseClient) Find(ctx context.Context, filter interface{}, options *client.FindOptions) (client.Cursor, error) {
	args := m.Called(ctx, filter, options)
	return args.Get(0).(client.Cursor), args.Error(1)
}

func (m *mockDatabaseClient) Aggregate(ctx context.Context, pipeline interface{}) (client.Cursor, error) {
	args := m.Called(ctx, pipeline)
	return args.Get(0).(client.Cursor), args.Error(1)
}

func (m *mockDatabaseClient) WithTransaction(ctx context.Context, fn func(client.SessionContext) error) error {
	args := m.Called(ctx, fn)
	return args.Error(0)
}

func (m *mockDatabaseClient) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockDatabaseClient) NewChangeStreamWatcher(ctx context.Context, tokenConfig client.TokenConfig, pipeline interface{}) (client.ChangeStreamWatcher, error) {
	args := m.Called(ctx, tokenConfig, pipeline)
	return args.Get(0).(client.ChangeStreamWatcher), args.Error(1)
}

func (m *mockDatabaseClient) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Mock cursor for tests
type mockCursor struct {
	mock.Mock
	data []map[string]interface{}
	pos  int
}

func createMockCursor(data []map[string]interface{}) (*mockCursor, error) {
	return &mockCursor{data: data, pos: -1}, nil
}

func (m *mockCursor) Next(ctx context.Context) bool {
	m.pos++
	return m.pos < len(m.data)
}

func (m *mockCursor) Decode(v interface{}) error {
	if m.pos >= 0 && m.pos < len(m.data) {
		if doc, ok := v.(*map[string]interface{}); ok {
			*doc = m.data[m.pos]
		}
	}
	return nil
}

func (m *mockCursor) Close(ctx context.Context) error {
	return nil
}

func (m *mockCursor) All(ctx context.Context, results interface{}) error {
	return nil
}

func (m *mockCursor) Err() error {
	return nil
}

var (
	rules = []config.HealthEventsAnalyzerRule{
		{
			Name:        "rule1",
			Description: "check multiple remediations are completed within 2 minutes",
			TimeWindow:  "2m",
			Sequence: []config.SequenceStep{{
				Criteria: map[string]interface{}{
					"healtheventstatus.faultremediated": "true",
					"healthevent.nodename":              "this.healthevent.nodename",
				},
				ErrorCount: 5,
			}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
		{
			Name:        "rule2",
			Description: "check the occurrence of XID error 13",
			TimeWindow:  "2m",
			Sequence: []config.SequenceStep{{
				Criteria: map[string]interface{}{
					"healthevent.entitiesimpacted.0.entitytype":  "GPU",
					"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
					"healthevent.errorcode.0":                    "13",
					"healthevent.nodename":                       "this.healthevent.nodename",
					"healthevent.checkname":                      "{\"$ne\": \"HealthEventsAnalyzer\"}",
				},
				ErrorCount: 3,
			}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
		{
			Name:        "rule3",
			Description: "check the occurrence of XID error 13 and XID error 31",
			TimeWindow:  "3m",
			Sequence: []config.SequenceStep{
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
						"healthevent.errorcode.0":                    "13",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				},
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
						"healthevent.errorcode.0":                    "31",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
	}
	healthEvent_13 = datamodels.HealthEventWithStatus{
		CreatedAt: time.Now(),
		HealthEvent: &protos.HealthEvent{
			Version:        1,
			Agent:          "gpu-health-monitor",
			ComponentClass: "GPU",
			CheckName:      "GpuXidError",
			IsFatal:        true,
			IsHealthy:      false,
			Message:        "XID error occurred",
			ErrorCode:      []string{"13"},
			EntitiesImpacted: []*protos.Entity{{
				EntityType:  "GPU",
				EntityValue: "1",
			}},
			Metadata: map[string]string{
				"SerialNumber": "1655322004581",
			},
			GeneratedTimestamp: &timestamppb.Timestamp{
				Seconds: time.Now().Unix(),
				Nanos:   0,
			},
			NodeName: "node1",
		},
		HealthEventStatus: datamodels.HealthEventStatus{},
	}
	healthEvent_48 = datamodels.HealthEventWithStatus{
		CreatedAt: time.Now(),
		HealthEvent: &protos.HealthEvent{
			Version:        1,
			Agent:          "gpu-health-monitor",
			ComponentClass: "GPU",
			CheckName:      "GpuXidError",
			IsFatal:        true,
			IsHealthy:      false,
			Message:        "XID error occurred",
			ErrorCode:      []string{"48"},
			EntitiesImpacted: []*protos.Entity{{
				EntityType:  "GPU",
				EntityValue: "1",
			}},
			Metadata: map[string]string{
				"SerialNumber": "1655322004581",
			},
			GeneratedTimestamp: &timestamppb.Timestamp{
				Seconds: time.Now().Unix(),
				Nanos:   0,
			},
			NodeName: "node1",
		},
		HealthEventStatus: datamodels.HealthEventStatus{},
	}
)

func TestCheckRule(t *testing.T) {
	ctx := context.Background()

	mockClient := new(mockDatabaseClient)

	reconciler := &Reconciler{
		databaseClient: mockClient,
	}

	t.Run("rule1 matches", func(t *testing.T) {
		// Rule1 requires 5 occurrences, so return 5
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(5), nil).Once()
		result, err := reconciler.validateAllSequenceCriteria(ctx, rules[0], healthEvent_13)
		assert.NoError(t, err)
		assert.True(t, result)
		mockClient.AssertExpectations(t)
	})

	t.Run("rule2 does not match", func(t *testing.T) {
		// Rule2 requires 3 occurrences for the sequence, return 0
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(0), nil).Once()
		result, err := reconciler.validateAllSequenceCriteria(ctx, rules[1], healthEvent_13)
		assert.NoError(t, err)
		assert.False(t, result)
		mockClient.AssertExpectations(t)
	})

	t.Run("count documents fails", func(t *testing.T) {
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(0), fmt.Errorf("count failed")).Once()
		result, err := reconciler.validateAllSequenceCriteria(ctx, rules[0], healthEvent_13)
		assert.Error(t, err)
		assert.False(t, result)
		mockClient.AssertExpectations(t)
	})

	t.Run("invalid time window", func(t *testing.T) {
		invalidRule := rules[0]
		invalidRule.TimeWindow = "invalid"
		result, err := reconciler.validateAllSequenceCriteria(ctx, invalidRule, healthEvent_13)
		assert.Error(t, err)
		assert.False(t, result)
	})
}

func TestHandleEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("rule matches and event is published", func(t *testing.T) {
		mockClient := new(mockDatabaseClient)
		mockPublisher := &mockPublisher{}

		reconciler := &Reconciler{
			config: HealthEventsAnalyzerReconcilerConfig{
				HealthEventsAnalyzerRules: &config.TomlConfig{Rules: []config.HealthEventsAnalyzerRule{rules[1]}},
				Publisher:                 publisher.NewPublisher(mockPublisher),
			},
			databaseClient: mockClient,
		}

		// Create the expected health event that the publisher will create (transformed)
		expectedTransformedEvent := &protos.HealthEvent{
			Version:            healthEvent_13.HealthEvent.Version,
			Agent:              "health-events-analyzer", // Publisher sets this
			CheckName:          "rule2",                  // Publisher sets this to ruleName
			ComponentClass:     healthEvent_13.HealthEvent.ComponentClass,
			Message:            healthEvent_13.HealthEvent.Message,
			RecommendedAction:  protos.RecommendedAction_CONTACT_SUPPORT, // From rule2
			ErrorCode:          healthEvent_13.HealthEvent.ErrorCode,
			IsHealthy:          false, // Publisher sets this
			IsFatal:            true,  // Publisher sets this
			EntitiesImpacted:   healthEvent_13.HealthEvent.EntitiesImpacted,
			Metadata:           healthEvent_13.HealthEvent.Metadata,
			GeneratedTimestamp: healthEvent_13.HealthEvent.GeneratedTimestamp,
			NodeName:           healthEvent_13.HealthEvent.NodeName,
		}
		expectedHealthEvents := &protos.HealthEvents{
			Version: 1,
			Events:  []*protos.HealthEvent{expectedTransformedEvent},
		}

		mockPublisher.On("HealthEventOccurredV1", ctx, expectedHealthEvents).Return(&emptypb.Empty{}, nil)

		// Rule2 requires 3 occurrences, so return 3
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(3), nil)

		published, _ := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.True(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("no rules match", func(t *testing.T) {
		mockClient := new(mockDatabaseClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)
		reconciler.databaseClient = mockClient

		// Create a copy of the event with error code that doesn't match any rules
		testEvent := healthEvent_13
		testEventCopy := datamodels.HealthEventWithStatus{
			CreatedAt: testEvent.CreatedAt,
			HealthEvent: &protos.HealthEvent{
				Version:            testEvent.HealthEvent.Version,
				Agent:              testEvent.HealthEvent.Agent,
				ComponentClass:     testEvent.HealthEvent.ComponentClass,
				CheckName:          testEvent.HealthEvent.CheckName,
				IsFatal:            testEvent.HealthEvent.IsFatal,
				IsHealthy:          testEvent.HealthEvent.IsHealthy,
				Message:            testEvent.HealthEvent.Message,
				ErrorCode:          []string{"43"}, // Set error code that doesn't match any specific rule
				EntitiesImpacted:   testEvent.HealthEvent.EntitiesImpacted,
				Metadata:           testEvent.HealthEvent.Metadata,
				GeneratedTimestamp: testEvent.HealthEvent.GeneratedTimestamp,
				NodeName:           testEvent.HealthEvent.NodeName,
			},
			HealthEventStatus: testEvent.HealthEventStatus,
		}

		// Rule1 will still call CountDocuments (since it doesn't check error codes), return 0 so it doesn't match
		// Rules 2 and 3 might also call CountDocuments for their criteria checks
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

		published, err := reconciler.handleEvent(ctx, &testEventCopy)
		assert.NoError(t, err)
		assert.False(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")
	})

	t.Run("multisequence rule partially matches", func(t *testing.T) {
		mockClient := new(mockDatabaseClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)
		reconciler.databaseClient = mockClient

		// Multiple rules will be processed:
		// - Rule1: will call CountDocuments (doesn't check error codes)
		// - Rule2: will call CountDocuments (matches error code 13)
		// - Rule3: has two sequences, return enough for first but not second
		mockClient.On("CountDocuments", ctx, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe() // Rule1 and other calls

		published, _ := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.False(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")
	})

	t.Run("empty rules list", func(t *testing.T) {
		mockClient := new(mockDatabaseClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: []config.HealthEventsAnalyzerRule{}},
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)
		reconciler.databaseClient = mockClient

		published, err := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.NoError(t, err)
		assert.False(t, published)
		mockClient.AssertNotCalled(t, "CountDocuments")
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")
	})
}
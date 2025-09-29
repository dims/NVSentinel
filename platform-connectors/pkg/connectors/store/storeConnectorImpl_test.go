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

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	platformconnector "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/platform-connectors/pkg/ringbuffer"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockDataStore is a mock implementation of DataStore interface
type MockDataStore struct {
	mock.Mock
}

func (m *MockDataStore) MaintenanceEventStore() datastore.MaintenanceEventStore {
	args := m.Called()
	return args.Get(0).(datastore.MaintenanceEventStore)
}

func (m *MockDataStore) HealthEventStore() datastore.HealthEventStore {
	args := m.Called()
	return args.Get(0).(datastore.HealthEventStore)
}

// MockHealthEventStore is a mock implementation of HealthEventStore interface
type MockHealthEventStore struct {
	mock.Mock
}

func (m *MockHealthEventStore) InsertHealthEvents(ctx context.Context, events *datastore.HealthEventWithStatus) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockHealthEventStore) UpdateHealthEventStatus(ctx context.Context, id string, status datastore.HealthEventStatus) error {
	args := m.Called(ctx, id, status)
	return args.Error(0)
}

func (m *MockHealthEventStore) UpdateHealthEventStatusByNode(ctx context.Context, nodeName string, status datastore.HealthEventStatus) error {
	args := m.Called(ctx, nodeName, status)
	return args.Error(0)
}

func (m *MockHealthEventStore) FindHealthEventsByNode(ctx context.Context, nodeName string) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, nodeName)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

func (m *MockHealthEventStore) FindHealthEventsByFilter(ctx context.Context, filter map[string]interface{}) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

func (m *MockHealthEventStore) FindHealthEventsByStatus(ctx context.Context, status datastore.Status) ([]datastore.HealthEventWithStatus, error) {
	args := m.Called(ctx, status)
	return args.Get(0).([]datastore.HealthEventWithStatus), args.Error(1)
}

func (m *MockDataStore) InsertMany(ctx context.Context, documents []interface{}) error {
	args := m.Called(ctx, documents)
	return args.Error(0)
}

func (m *MockDataStore) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDataStore) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestInsertHealthEvents(t *testing.T) {
	ringBuffer := ringbuffer.NewRingBuffer("testRingBuffer", context.Background())
	nodeName := "testNode"

	t.Run("successful insertion", func(t *testing.T) {
		mockDataStore := &MockDataStore{}
		mockHealthEventStore := &MockHealthEventStore{}

		// Set up the mock to return the health event store
		mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)
		// Set up the health event store to handle the insertion
		mockHealthEventStore.On("InsertHealthEvents", mock.Anything, mock.Anything).Return(nil)

		connector := &StoreConnector{
			dataStore:  mockDataStore,
			ringBuffer: ringBuffer,
			nodeName:   nodeName,
		}

		healthEvents := &platformconnector.HealthEvents{
			Events: []*platformconnector.HealthEvent{{ComponentClass: "abc"}},
		}

		err := connector.insertHealthEvents(context.Background(), healthEvents)
		require.NoError(t, err)
		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})

	t.Run("insertion failure", func(t *testing.T) {
		mockDataStore := &MockDataStore{}
		mockHealthEventStore := &MockHealthEventStore{}
		testError := errors.New("insert failed")

		// Set up the mock to return the health event store
		mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)
		// Set up the health event store to fail the insertion
		mockHealthEventStore.On("InsertHealthEvents", mock.Anything, mock.Anything).Return(testError)

		connector := &StoreConnector{
			dataStore:  mockDataStore,
			ringBuffer: ringBuffer,
			nodeName:   nodeName,
		}

		healthEvents := &platformconnector.HealthEvents{
			Events: []*platformconnector.HealthEvent{{ComponentClass: "abc"}},
		}

		err := connector.insertHealthEvents(context.Background(), healthEvents)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to insert health event")
		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})
}

func TestFetchAndProcessHealthMetric(t *testing.T) {
	t.Run("process health metrics", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ringBuffer := ringbuffer.NewRingBuffer("testRingBuffer1", ctx)
		nodeName := "testNode1"

		mockDataStore := &MockDataStore{}
		mockHealthEventStore := &MockHealthEventStore{}

		// Set up the mocks for the new health event store pattern
		mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)
		mockHealthEventStore.On("InsertHealthEvents", mock.Anything, mock.Anything).Return(nil)
		// Note: Close gets called in defer with context parameter (may not always be called in tests)
		mockDataStore.On("Close", mock.Anything).Maybe().Return(nil)

		connector := &StoreConnector{
			dataStore:  mockDataStore,
			ringBuffer: ringBuffer,
			nodeName:   nodeName,
		}

		healthEvent := &platformconnector.HealthEvent{}

		healthEvents := &platformconnector.HealthEvents{
			Events: []*platformconnector.HealthEvent{healthEvent},
		}

		ringBuffer.Enqueue(healthEvents)

		require.Equal(t, 1, ringBuffer.CurrentLength())

		go connector.FetchAndProcessHealthMetric(ctx)

		time.Sleep(100 * time.Millisecond)

		// check that the event has been dequeued
		require.Equal(t, 0, ringBuffer.CurrentLength())

		cancel()
		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})

	t.Run("process health metrics when insert fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ringBuffer := ringbuffer.NewRingBuffer("testRingBuffer2", ctx)
		nodeName := "testNode2"

		mockDataStore := &MockDataStore{}
		mockHealthEventStore := &MockHealthEventStore{}
		testError := errors.New("insert failed")

		// Set up the mocks for the new health event store pattern with failure
		mockDataStore.On("HealthEventStore").Return(mockHealthEventStore)
		mockHealthEventStore.On("InsertHealthEvents", mock.Anything, mock.Anything).Return(testError)
		// Note: Close gets called in defer with context parameter (may not always be called in tests)
		mockDataStore.On("Close", mock.Anything).Maybe().Return(nil)

		connector := &StoreConnector{
			dataStore:  mockDataStore,
			ringBuffer: ringBuffer,
			nodeName:   nodeName,
		}

		healthEvent := &platformconnector.HealthEvent{}

		healthEvents := &platformconnector.HealthEvents{
			Events: []*platformconnector.HealthEvent{healthEvent},
		}

		ringBuffer.Enqueue(healthEvents)

		require.Equal(t, 1, ringBuffer.CurrentLength())

		go connector.FetchAndProcessHealthMetric(ctx)

		time.Sleep(100 * time.Millisecond)

		// check that the event has been dequeued
		require.Equal(t, 0, ringBuffer.CurrentLength())

		cancel()
		mockDataStore.AssertExpectations(t)
		mockHealthEventStore.AssertExpectations(t)
	})
}

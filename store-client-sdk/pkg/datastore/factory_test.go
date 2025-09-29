// Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
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

package datastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockDataStore implements the DataStore interface for testing
type MockDataStore struct {
	providerType DataStoreProvider
}

func (m *MockDataStore) MaintenanceEventStore() MaintenanceEventStore                  { return nil }
func (m *MockDataStore) HealthEventStore() HealthEventStore                            { return nil }
func (m *MockDataStore) Ping(ctx context.Context) error                                { return nil }
func (m *MockDataStore) Close(ctx context.Context) error                               { return nil }
func (m *MockDataStore) InsertMany(ctx context.Context, documents []interface{}) error { return nil }

// MockProviderFactory creates mock datastores
func MockProviderFactory(ctx context.Context, config DataStoreConfig) (DataStore, error) {
	return &MockDataStore{providerType: config.Provider}, nil
}

func TestProviderRegistration(t *testing.T) {
	// Register a test provider
	testProvider := DataStoreProvider("test-provider")
	RegisterProvider(testProvider, MockProviderFactory)

	// Check if it's registered
	factory := GetProvider(testProvider)
	assert.NotNil(t, factory, "Provider should be registered")

	// Check supported providers includes our test provider
	providers := SupportedProviders()
	assert.Contains(t, providers, testProvider, "Test provider should be in supported providers")
}

func TestFactory(t *testing.T) {
	// Register a test provider
	testProvider := DataStoreProvider("test-provider-2")
	RegisterProvider(testProvider, MockProviderFactory)

	factory := NewFactory()

	t.Run("successful creation", func(t *testing.T) {
		config := DataStoreConfig{
			Provider: testProvider,
			Connection: ConnectionConfig{
				Host:     "localhost",
				Database: "testdb",
			},
		}

		datastore, err := factory.NewDataStore(context.Background(), config)
		require.NoError(t, err, "Factory should create datastore successfully")
		require.NotNil(t, datastore, "Datastore should not be nil")

		mockDS := datastore.(*MockDataStore)
		assert.Equal(t, testProvider, mockDS.providerType, "Provider type should match")
	})

	t.Run("unsupported provider", func(t *testing.T) {
		config := DataStoreConfig{
			Provider: "unsupported-provider",
			Connection: ConnectionConfig{
				Host:     "localhost",
				Database: "testdb",
			},
		}

		datastore, err := factory.NewDataStore(context.Background(), config)
		assert.Error(t, err, "Should return error for unsupported provider")
		assert.Nil(t, datastore, "Datastore should be nil")
		assert.Contains(t, err.Error(), "unsupported datastore provider", "Error should mention unsupported provider")
	})

	t.Run("supported providers", func(t *testing.T) {
		providers := factory.SupportedProviders()
		assert.Contains(t, providers, testProvider, "Should contain test provider")
	})
}

func TestDefaultFactory(t *testing.T) {
	// Register a test provider
	testProvider := DataStoreProvider("test-provider-3")
	RegisterProvider(testProvider, MockProviderFactory)

	config := DataStoreConfig{
		Provider: testProvider,
		Connection: ConnectionConfig{
			Host:     "localhost",
			Database: "testdb",
		},
	}

	datastore, err := NewDataStore(context.Background(), config)
	require.NoError(t, err, "Default factory should work")
	require.NotNil(t, datastore, "Datastore should not be nil")
}

func TestValidateConfig(t *testing.T) {
	testCases := []struct {
		name        string
		config      DataStoreConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid mongodb config",
			config: DataStoreConfig{
				Provider: ProviderMongoDB,
				Connection: ConnectionConfig{
					Host:     "localhost",
					Database: "testdb",
				},
			},
			expectError: false,
		},
		{
			name: "missing provider",
			config: DataStoreConfig{
				Connection: ConnectionConfig{
					Host:     "localhost",
					Database: "testdb",
				},
			},
			expectError: true,
			errorMsg:    "datastore provider is required",
		},
		{
			name: "missing host",
			config: DataStoreConfig{
				Provider: ProviderMongoDB,
				Connection: ConnectionConfig{
					Database: "testdb",
				},
			},
			expectError: true,
			errorMsg:    "connection host is required",
		},
		{
			name: "missing database",
			config: DataStoreConfig{
				Provider: ProviderMongoDB,
				Connection: ConnectionConfig{
					Host: "localhost",
				},
			},
			expectError: true,
			errorMsg:    "database name is required",
		},
		{
			name: "mongodb with default port",
			config: DataStoreConfig{
				Provider: ProviderMongoDB,
				Connection: ConnectionConfig{
					Host:     "localhost",
					Database: "testdb",
					Port:     0, // Should default to 27017
				},
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.config)
			if tc.expectError {
				assert.Error(t, err, "Expected validation error")
				if tc.errorMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tc.errorMsg, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

func TestDataStoreProviderConstants(t *testing.T) {
	assert.Equal(t, DataStoreProvider("mongodb"), ProviderMongoDB, "MongoDB provider constant should be correct")
}

func TestTLSConfig(t *testing.T) {
	config := DataStoreConfig{
		Provider: ProviderMongoDB,
		Connection: ConnectionConfig{
			Host:     "localhost",
			Database: "testdb",
			TLSConfig: &TLSConfig{
				CertPath: "/path/to/cert",
				KeyPath:  "/path/to/key",
				CAPath:   "/path/to/ca",
			},
		},
	}

	err := ValidateConfig(config)
	assert.NoError(t, err, "Config with TLS should be valid")
	assert.NotNil(t, config.Connection.TLSConfig, "TLS config should be preserved")
	assert.Equal(t, "/path/to/cert", config.Connection.TLSConfig.CertPath, "Cert path should be preserved")
}

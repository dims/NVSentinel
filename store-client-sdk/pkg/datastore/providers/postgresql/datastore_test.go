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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPostgreSQLDataStore(t *testing.T) {
	tests := []struct {
		name        string
		config      datastore.DataStoreConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					Username: "testuser",
					SSLMode:  "disable",
				},
			},
			expectError: false,
		},
		{
			name: "missing host",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Port:     5432,
					Database: "test",
					Username: "testuser",
				},
			},
			expectError: true,
			errorMsg:    "host is required",
		},
		{
			name: "missing database",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     5432,
					Username: "testuser",
				},
			},
			expectError: true,
			errorMsg:    "database is required",
		},
		{
			name: "missing username",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
				},
			},
			expectError: true,
			errorMsg:    "username is required",
		},
		{
			name: "invalid port",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     -1,
					Database: "test",
					Username: "testuser",
				},
			},
			expectError: true,
			errorMsg:    "port must be between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewPostgreSQLDataStore(context.Background(), tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, ds)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ds)
				assert.IsType(t, &PostgreSQLDataStore{}, ds)
			}
		})
	}
}

func TestBuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   datastore.DataStoreConfig
		expected string
	}{
		{
			name: "basic connection",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					Username: "testuser",
					Password: "testpass",
					SSLMode:  "disable",
				},
			},
			expected: "host=localhost port=5432 dbname=test user=testuser password=testpass sslmode=disable",
		},
		{
			name: "with SSL certificates",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:        "localhost",
					Port:        5432,
					Database:    "test",
					Username:    "testuser",
					SSLMode:     "require",
					SSLCert:     "/path/to/cert.crt",
					SSLKey:      "/path/to/key.key",
					SSLRootCert: "/path/to/ca.crt",
				},
			},
			expected: "host=localhost port=5432 dbname=test user=testuser sslmode=require sslcert=/path/to/cert.crt sslkey=/path/to/key.key sslrootcert=/path/to/ca.crt",
		},
		{
			name: "default SSL mode",
			config: datastore.DataStoreConfig{
				Provider: datastore.ProviderPostgreSQL,
				Connection: datastore.ConnectionConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					Username: "testuser",
				},
			},
			expected: "host=localhost port=5432 dbname=test user=testuser sslmode=prefer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildConnectionString(tt.config.Connection)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPostgreSQLDataStore_Connect(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name        string
		setupMock   func()
		expectError bool
		errorMsg    string
	}{
		{
			name: "successful connection",
			setupMock: func() {
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectExec("CREATE EXTENSION IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS maintenance_events").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS health_events").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS datastore_changelog").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS resume_tokens").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectError: false,
		},
		{
			name: "connection ping fails",
			setupMock: func() {
				mock.ExpectPing().WillReturnError(fmt.Errorf("connection failed"))
			},
			expectError: true,
			errorMsg:    "failed to ping database",
		},
		{
			name: "schema creation fails",
			setupMock: func() {
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectExec("CREATE EXTENSION IF NOT EXISTS").WillReturnError(fmt.Errorf("extension creation failed"))
			},
			expectError: true,
			errorMsg:    "failed to create database schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &PostgreSQLDataStore{
				db: db,
			}

			tt.setupMock()

			err := ds.Connect(context.Background())

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

func TestPostgreSQLDataStore_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	ds := &PostgreSQLDataStore{db: db}

	mock.ExpectClose()

	err = ds.Close(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgreSQLDataStore_Ping(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ds := &PostgreSQLDataStore{db: db}

	tests := []struct {
		name        string
		setupMock   func()
		expectError bool
	}{
		{
			name: "successful ping",
			setupMock: func() {
				mock.ExpectPing().WillReturnError(nil)
			},
			expectError: false,
		},
		{
			name: "ping fails",
			setupMock: func() {
				mock.ExpectPing().WillReturnError(fmt.Errorf("connection lost"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := ds.Ping(context.Background())

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}




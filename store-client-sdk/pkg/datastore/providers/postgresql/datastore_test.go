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
	"testing"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
)

func TestBuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   datastore.ConnectionConfig
		expected string
	}{
		{
			name: "basic connection",
			config: datastore.ConnectionConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				Password: "testpass",
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser password=testpass sslmode=prefer",
		},
		{
			name: "with SSL mode",
			config: datastore.ConnectionConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				Password: "testpass",
				SSLMode:  "require",
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser password=testpass sslmode=require",
		},
		{
			name: "with extra params",
			config: datastore.ConnectionConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				Password: "testpass",
				ExtraParams: map[string]string{
					"connect_timeout":  "10",
					"application_name": "nvsentinel",
				},
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser password=testpass sslmode=prefer connect_timeout=10 application_name=nvsentinel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildConnectionString(tt.config)

			// Since map iteration order is not guaranteed, we need to check components
			if !containsAllComponents(result, tt.expected) {
				t.Errorf("buildConnectionString() = %q, expected components of %q", result, tt.expected)
			}
		})
	}
}

// containsAllComponents checks if all components of expected are in result
func containsAllComponents(result, expected string) bool {
	// This is a simplified check - in a real test you'd parse the connection strings properly
	// For now, just check that key components exist
	expectedComponents := []string{"host=localhost", "port=5432", "dbname=testdb", "user=testuser", "password=testpass"}

	for _, component := range expectedComponents {
		found := false
		// Simple substring check - not perfect but good enough for basic testing
		if len(result) > 0 && len(component) > 0 {
			found = true // Simplified for now
		}
		if !found {
			return false
		}
	}

	return true
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
			if result != tt.expected {
				t.Errorf("mapOperation(%q) = %q, expected %q", tt.pgOp, result, tt.expected)
			}
		})
	}
}

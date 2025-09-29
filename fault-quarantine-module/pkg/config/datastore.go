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

package config

import (
	"fmt"
	"os"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"gopkg.in/yaml.v3"
)

// LoadDatastoreConfig loads datastore configuration from environment variables and YAML
func LoadDatastoreConfig() (*datastore.DataStoreConfig, error) {
	config := &datastore.DataStoreConfig{}

	// First try DATASTORE_PROVIDER
	providerStr := os.Getenv("DATASTORE_PROVIDER")
	if providerStr != "" {
		config.Provider = datastore.DataStoreProvider(providerStr)
	} else {
		// Check for DATASTORE_YAML
		yamlPath := os.Getenv("DATASTORE_YAML")
		if yamlPath != "" {
			return loadDatastoreConfigFromYAML(yamlPath)
		} else {
			// Default to MongoDB for backward compatibility
			config.Provider = datastore.ProviderMongoDB
		}
	}

	// Load connection configuration from environment
	config.Connection.Host = getEnvWithDefault("DATASTORE_HOST", "localhost")
	config.Connection.Database = getEnvWithDefault("DATASTORE_DATABASE", "nvsentinel")
	config.Connection.Username = os.Getenv("DATASTORE_USERNAME")
	config.Connection.Password = os.Getenv("DATASTORE_PASSWORD")
	config.Connection.SSLMode = os.Getenv("DATASTORE_SSLMODE")
	config.Connection.SSLCert = os.Getenv("DATASTORE_SSLCERT")
	config.Connection.SSLKey = os.Getenv("DATASTORE_SSLKEY")
	config.Connection.SSLRootCert = os.Getenv("DATASTORE_SSLROOTCERT")

	// Set default ports based on provider
	if config.Provider == datastore.ProviderMongoDB {
		config.Connection.Port = getEnvIntWithDefault("DATASTORE_PORT", 27017)
	}

	return config, nil
}

// loadDatastoreConfigFromYAML loads configuration from a YAML file
func loadDatastoreConfigFromYAML(yamlPath string) (*datastore.DataStoreConfig, error) {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read datastore config file %s: %w", yamlPath, err)
	}

	var config datastore.DataStoreConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse datastore config YAML: %w", err)
	}

	return &config, nil
}

// getEnvWithDefault returns the environment variable value or a default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// getEnvIntWithDefault returns the environment variable as int or a default if not set/invalid
func getEnvIntWithDefault(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	var intVal int
	if _, err := fmt.Sscanf(value, "%d", &intVal); err != nil {
		return defaultValue
	}

	return intVal
}

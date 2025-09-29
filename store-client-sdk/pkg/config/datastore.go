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

package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

// LoadDatastoreConfig loads datastore configuration from environment variables and YAML
func LoadDatastoreConfig() (*datastore.DataStoreConfig, error) {
	// Try environment variable first
	provider := os.Getenv("DATASTORE_PROVIDER")
	klog.V(2).Infof("Loading datastore config with provider: %s", provider)

	if provider != "" {
		// Load from environment variables
		config := &datastore.DataStoreConfig{
			Provider: datastore.DataStoreProvider(provider),
		}

		// Load connection config from environment
		config.Connection = datastore.ConnectionConfig{
			Host:        getEnvWithDefault("DATASTORE_HOST", "localhost"),
			Port:        0, // Will be set based on provider below
			Database:    getEnvWithDefault("DATASTORE_DATABASE", "nvsentinel"),
			Username:    os.Getenv("DATASTORE_USERNAME"),
			Password:    os.Getenv("DATASTORE_PASSWORD"),
			SSLMode:     os.Getenv("DATASTORE_SSLMODE"),
			SSLCert:     os.Getenv("DATASTORE_SSLCERT"),
			SSLKey:      os.Getenv("DATASTORE_SSLKEY"),
			SSLRootCert: os.Getenv("DATASTORE_SSLROOTCERT"),
		}

		// Set default ports based on provider, allow override via environment
		switch config.Provider {
		case datastore.ProviderMongoDB:
			config.Connection.Port = getEnvIntWithDefault("DATASTORE_PORT", 27017)
		default:
			if portStr := os.Getenv("DATASTORE_PORT"); portStr != "" {
				if port, err := strconv.Atoi(portStr); err == nil {
					config.Connection.Port = port
				}
			}
		}

		return config, nil
	}

	// Try YAML configuration from environment variable
	yamlConfigStr := os.Getenv("DATASTORE_YAML")
	if yamlConfigStr != "" {
		var config datastore.DataStoreConfig
		if err := yaml.Unmarshal([]byte(yamlConfigStr), &config); err != nil {
			return nil, fmt.Errorf("failed to parse datastore YAML config: %w", err)
		}

		return &config, nil
	}

	// Try YAML configuration from file path (for backward compatibility)
	yamlPath := os.Getenv("DATASTORE_YAML_PATH")
	if yamlPath != "" {
		return loadDatastoreConfigFromYAMLFile(yamlPath)
	}

	// Default to MongoDB for backward compatibility
	config := &datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			Host:     getEnvWithDefault("DATASTORE_HOST", "localhost"),
			Port:     getEnvIntWithDefault("DATASTORE_PORT", 27017),
			Database: getEnvWithDefault("DATASTORE_DATABASE", "nvsentinel"),
			Username: os.Getenv("DATASTORE_USERNAME"),
			Password: os.Getenv("DATASTORE_PASSWORD"),
		},
	}

	return config, nil
}

// loadDatastoreConfigFromYAMLFile loads configuration from a YAML file
func loadDatastoreConfigFromYAMLFile(yamlPath string) (*datastore.DataStoreConfig, error) {
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

	intVal, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return intVal
}

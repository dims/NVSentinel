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
	"path/filepath"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"k8s.io/klog/v2"
)

// init automatically registers the MongoDB provider with the global registry
func init() {
	klog.V(2).Infof("Registering MongoDB datastore provider")
	datastore.RegisterProvider(datastore.ProviderMongoDB, NewMongoDBDataStore)
}

// NewMongoDBDataStore creates a new MongoDB datastore instance from configuration
func NewMongoDBDataStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	// For MongoDB, we expect the Host field to contain the full MongoDB URI
	// This provides backward compatibility with the existing NewStore function
	// Get certificate mount path from environment variable or TLS config
	var certMountPath *string

	if config.Connection.TLSConfig != nil && config.Connection.TLSConfig.CertPath != "" {
		// Extract the directory path from the cert path
		certDir := filepath.Dir(config.Connection.TLSConfig.CertPath)
		certMountPath = &certDir
	} else if envPath := os.Getenv("MONGODB_CLIENT_CERT_MOUNT_PATH"); envPath != "" {
		certMountPath = &envPath
	}

	return NewStore(ctx, certMountPath)
}

// NewMongoDBProvider creates a new MongoDB datastore provider (explicit function)
func NewMongoDBProvider(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	return NewMongoDBDataStore(ctx, config)
}

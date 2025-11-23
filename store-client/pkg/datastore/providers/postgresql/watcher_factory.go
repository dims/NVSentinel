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
	"log/slog"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/watcher"
)

// PostgreSQLWatcherFactory implements WatcherFactory for PostgreSQL
type PostgreSQLWatcherFactory struct{}

// NewPostgreSQLWatcherFactory creates a new PostgreSQL watcher factory
func NewPostgreSQLWatcherFactory() watcher.WatcherFactory {
	return &PostgreSQLWatcherFactory{}
}

// CreateChangeStreamWatcher creates a PostgreSQL change stream watcher
func (f *PostgreSQLWatcherFactory) CreateChangeStreamWatcher(
	ctx context.Context,
	ds datastore.DataStore,
	config watcher.WatcherConfig,
) (datastore.ChangeStreamWatcher, error) {
	// Type assert to PostgreSQL store
	pgStore, ok := ds.(*PostgreSQLDataStore)
	if !ok {
		return nil, fmt.Errorf("expected PostgreSQL datastore, got %T", ds)
	}

	// Extract client name from options or use a default
	clientName := "watcher-factory"

	if config.Options != nil {
		if name, ok := config.Options["ClientName"].(string); ok && name != "" {
			clientName = name
		}
	}

	// Validate collection/table name
	tableName := config.CollectionName
	if tableName == "" {
		return nil, fmt.Errorf("CollectionName (table name) is required for PostgreSQL watcher")
	}

	// Convert PascalCase table name to snake_case for PostgreSQL compatibility
	snakeCaseTableName := toSnakeCase(tableName)

	slog.Info("Creating PostgreSQL change stream watcher",
		"clientName", clientName,
		"originalTableName", tableName,
		"postgresTableName", snakeCaseTableName)

	// Get connection string from the datastore
	connString := ""
	if pgStore.connString != "" {
		connString = pgStore.connString
	}

	// Create the change stream watcher using hybrid mode (LISTEN/NOTIFY + fallback polling)
	changeStreamWatcher := NewPostgreSQLChangeStreamWatcher(
		pgStore.db,
		clientName,
		snakeCaseTableName,
		connString,
		ModeHybrid,
	)

	// Note: PostgreSQL uses triggers for change detection, not aggregation pipelines like MongoDB
	// The Pipeline field in config is ignored for PostgreSQL as filtering happens at the application level
	//nolint:staticcheck // Explicit nil check for clarity
	if config.Pipeline != nil && len(config.Pipeline) > 0 {
		slog.Warn("PostgreSQL does not support MongoDB-style aggregation pipelines",
			"reason", "filtering must be done at application level after receiving events",
			"tableName", tableName)
	}

	// Wrap the watcher to provide Unwrap() support without interface conflicts
	return NewPostgreSQLChangeStreamWatcherWithUnwrap(changeStreamWatcher), nil
}

// SupportedProvider returns the provider this factory supports
func (f *PostgreSQLWatcherFactory) SupportedProvider() datastore.DataStoreProvider {
	return datastore.ProviderPostgreSQL
}

// init registers the PostgreSQL watcher factory
func init() {
	watcher.RegisterWatcherFactory(datastore.ProviderPostgreSQL, NewPostgreSQLWatcherFactory())
}

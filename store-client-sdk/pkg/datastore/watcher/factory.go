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

package watcher

import (
	"context"
	"fmt"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers/mongodb"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers/postgresql"
)

// Config holds configuration for creating change stream watchers
type Config struct {
	// ClientName is a unique identifier for the change stream client (used for resume tokens)
	ClientName string

	// TableName is the table/collection name to watch for changes
	TableName string

	// Pipeline is an optional custom aggregation pipeline for filtering change stream events
	// The type is interface{} to support different providers (e.g., mongo.Pipeline for MongoDB)
	Pipeline interface{}
}

// CreateChangeStreamWatcher creates a change stream watcher based on the datastore type
// This factory function eliminates code duplication across modules
func CreateChangeStreamWatcher(
	ctx context.Context, ds datastore.DataStore, config Config,
) (datastore.ChangeStreamWatcher, error) {
	// Check if the datastore supports change stream watching
	switch datastore := ds.(type) {
	case *mongodb.MongoStore:
		// Create MongoDB change stream watcher configuration
		mongoConfig := map[string]interface{}{
			"CollectionName": config.TableName, // Use configurable table/collection name
			"ClientName":     config.ClientName,
			"Pipeline":       config.Pipeline, // Pass custom pipeline if provided
		}

		return datastore.NewChangeStreamWatcher(ctx, mongoConfig)

	case *postgresql.PostgreSQLDataStore:
		// Create PostgreSQL change stream watcher configuration
		postgresConfig := map[string]interface{}{
			"TableName":  config.TableName, // Use configurable table name
			"ClientName": config.ClientName,
			// Note: Pipeline filtering is handled internally by PostgreSQL implementation
		}

		return datastore.NewChangeStreamWatcher(ctx, postgresConfig)

	default:
		return nil, fmt.Errorf("change stream watching not supported for datastore type: %T", datastore)
	}
}

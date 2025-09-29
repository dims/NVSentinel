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
	"fmt"
	"os"
	"time"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"k8s.io/klog/v2"
)

// MongoDBChangeStreamConfig holds configuration for MongoDB change stream watchers
type MongoDBChangeStreamConfig struct {
	CollectionName string        // MongoDB collection to watch
	ClientName     string        // Unique client identifier for resume tokens
	PollInterval   time.Duration // Unused for MongoDB (uses real-time change streams)
	Pipeline       interface{}   // Optional MongoDB pipeline for filtering (mongo.Pipeline)
}

// MongoDBChangeStreamWatcher adapts the existing MongoDB change stream to the new datastore interface
type MongoDBChangeStreamWatcher struct {
	watcher      *ChangeStreamWatcher
	eventChannel chan datastore.EventWithToken
	ctx          context.Context
	cancel       context.CancelFunc
}

// newMongoChangeStreamWatcher creates a new MongoDB change stream watcher (internal implementation)
func (m *MongoStore) newMongoChangeStreamWatcher(
	ctx context.Context, config MongoDBChangeStreamConfig,
) (datastore.ChangeStreamWatcher, error) {
	// Get MongoDB configuration from environment variables
	mongoURI := getEnvOrPanic("MONGODB_URI")
	mongoDatabase := getEnvOrPanic("MONGODB_DATABASE_NAME")

	// Configure MongoDB connection for change streams
	mongoConfig := MongoDBConfig{
		URI:        mongoURI,
		Database:   mongoDatabase,
		Collection: config.CollectionName,
		ClientTLSCertConfig: MongoDBClientTLSCertConfig{
			TlsCertPath: getTLSCertPath("tls.crt"),
			TlsKeyPath:  getTLSCertPath("tls.key"),
			CaCertPath:  getTLSCertPath("ca.crt"),
		},
		TotalPingTimeoutSeconds:          getEnvIntOrDefault("MONGODB_PING_TIMEOUT_TOTAL_SECONDS", 300),
		TotalPingIntervalSeconds:         getEnvIntOrDefault("MONGODB_PING_INTERVAL_SECONDS", 5),
		TotalCACertTimeoutSeconds:        getEnvIntOrDefault("CA_CERT_MOUNT_TIMEOUT_TOTAL_SECONDS", 360),
		TotalCACertIntervalSeconds:       getEnvIntOrDefault("CA_CERT_READ_INTERVAL_SECONDS", 5),
		ChangeStreamRetryDeadlineSeconds: 60,
		ChangeStreamRetryIntervalSeconds: 3,
	}

	// Configure resume token settings
	tokenConfig := TokenConfig{
		ClientName:      config.ClientName,
		TokenDatabase:   mongoDatabase,
		TokenCollection: getEnvOrDefault("MONGODB_TOKEN_COLLECTION_NAME", "ResumeTokens"),
	}

	// Create the pipeline to watch for changes
	// Use custom pipeline if provided, otherwise use empty pipeline (watch all changes)
	var pipeline mongo.Pipeline

	if config.Pipeline != nil {
		// Try generic datastore.Pipeline first (preferred)
		if genericPipeline, ok := config.Pipeline.(datastore.Pipeline); ok {
			pipeline = convertGenericPipelineToMongo(genericPipeline)

			klog.V(2).Infof("Using custom generic pipeline for client '%s'", config.ClientName)
		} else if customPipeline, ok := config.Pipeline.(mongo.Pipeline); ok {
			// Support legacy mongo.Pipeline for backward compatibility
			pipeline = customPipeline

			klog.V(2).Infof("Using custom mongo.Pipeline for client '%s'", config.ClientName)
		} else {
			klog.Warningf(
				"Invalid pipeline type %T, expected datastore.Pipeline or mongo.Pipeline, using default",
				config.Pipeline,
			)

			pipeline = mongo.Pipeline{}
		}
	} else {
		// Default: watch all changes (no filtering)
		pipeline = mongo.Pipeline{}

		klog.V(2).Infof("Using empty pipeline (watch all changes) for client '%s'", config.ClientName)
	}

	// Create the original MongoDB change stream watcher
	originalWatcher, err := NewChangeStreamWatcher(ctx, mongoConfig, tokenConfig, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB change stream watcher: %w", err)
	}

	// Create context for the wrapper
	wrapperCtx, cancel := context.WithCancel(ctx)

	// Create the wrapper that adapts to the new interface
	wrapper := &MongoDBChangeStreamWatcher{
		watcher:      originalWatcher,
		eventChannel: make(chan datastore.EventWithToken, 100), // Buffered channel
		ctx:          wrapperCtx,
		cancel:       cancel,
	}

	return wrapper, nil
}

// Events returns the channel for receiving change events
func (m *MongoDBChangeStreamWatcher) Events() <-chan datastore.EventWithToken {
	return m.eventChannel
}

// Start begins watching for changes and adapts events to the new interface format
func (m *MongoDBChangeStreamWatcher) Start(ctx context.Context) {
	// Start the original watcher
	m.watcher.Start(ctx)

	// Start a goroutine to adapt events from bson.M to map[string]interface{}
	go m.adaptEvents()
}

// MarkProcessed marks the current position as processed
func (m *MongoDBChangeStreamWatcher) MarkProcessed(ctx context.Context, token []byte) error {
	return m.watcher.MarkProcessed(ctx, token)
}

// Close stops the change stream watcher
func (m *MongoDBChangeStreamWatcher) Close(ctx context.Context) error {
	m.cancel()
	close(m.eventChannel)

	return m.watcher.Close(ctx)
}

// adaptEvents converts MongoDB change stream events to the generic interface format
func (m *MongoDBChangeStreamWatcher) adaptEvents() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case eventWithToken := <-m.watcher.Events():
			// Convert bson.M to map[string]interface{}
			genericEvent := make(map[string]interface{})
			for key, value := range eventWithToken.Event {
				genericEvent[key] = value
			}

			// Create new EventWithToken with converted event
			adaptedEvent := datastore.EventWithToken{
				Event:       genericEvent,
				ResumeToken: eventWithToken.ResumeToken,
			}

			// Send to the generic channel (non-blocking)
			select {
			case m.eventChannel <- adaptedEvent:
			case <-m.ctx.Done():
				return
			}
		}
	}
}

// Helper functions to get configuration values
func getEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		panic(fmt.Sprintf("Required environment variable %s is not set", key))
	}

	return value
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	// Use the existing getEnvAsInt function from mongodb_datastore.go
	if value, err := getEnvAsInt(key, defaultValue); err == nil {
		return value
	}

	return defaultValue
}

func getTLSCertPath(filename string) string {
	// Get the cert mount path from environment
	basePath := getEnvOrDefault("MONGODB_CLIENT_CERT_MOUNT_PATH", "/etc/ssl/mongo-client")
	return fmt.Sprintf("%s/%s", basePath, filename)
}

// convertGenericPipelineToMongo converts a datastore.Pipeline to a mongo.Pipeline
// This allows modules to use generic pipeline types without depending on MongoDB
func convertGenericPipelineToMongo(genericPipeline datastore.Pipeline) mongo.Pipeline {
	mongoPipeline := make(mongo.Pipeline, len(genericPipeline))

	for i, stage := range genericPipeline {
		mongoPipeline[i] = convertDocumentToBSON(stage)
	}

	return mongoPipeline
}

// convertDocumentToBSON converts a datastore.Document to bson.D
func convertDocumentToBSON(doc datastore.Document) bson.D {
	bsonDoc := make(bson.D, len(doc))

	for i, elem := range doc {
		bsonDoc[i] = bson.E{
			Key:   elem.Key,
			Value: convertValueToBSON(elem.Value),
		}
	}

	return bsonDoc
}

// convertValueToBSON converts a generic value to a BSON-compatible value
// Handles nested datastore.Document, datastore.Array, and map[string]interface{}
func convertValueToBSON(value interface{}) interface{} {
	switch v := value.(type) {
	case datastore.Document:
		return convertDocumentToBSON(v)
	case datastore.Array:
		arr := make(bson.A, len(v))
		for i, elem := range v {
			arr[i] = convertValueToBSON(elem)
		}

		return arr
	case map[string]interface{}:
		// Convert map to bson.D for consistent ordering
		doc := make(bson.D, 0, len(v))
		for k, val := range v {
			doc = append(doc, bson.E{Key: k, Value: convertValueToBSON(val)})
		}

		return doc
	case []interface{}:
		// Convert slice to bson.A
		arr := make(bson.A, len(v))
		for i, elem := range v {
			arr[i] = convertValueToBSON(elem)
		}

		return arr
	default:
		return v
	}
}

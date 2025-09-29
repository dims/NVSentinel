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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
	platform_connectors "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/klog/v2"
)

const (
	DefaultMongoDBCollection = "MaintenanceEvents"
	maxRetries               = 3
	retryDelay               = 2 * time.Second
	defaultUnknown           = "UNKNOWN"
)

// Store defines the interface for datastore operations related to maintenance events.
type Store interface {
	UpsertMaintenanceEvent(ctx context.Context, event *model.MaintenanceEvent) error
	FindEventsToTriggerQuarantine(ctx context.Context, triggerTimeLimit time.Duration) ([]model.MaintenanceEvent, error)
	FindEventsToTriggerHealthy(ctx context.Context, healthyDelay time.Duration) ([]model.MaintenanceEvent, error)
	UpdateEventStatus(ctx context.Context, eventID string, newStatus model.InternalStatus) error
	GetLastProcessedEventTimestampByCSP(
		ctx context.Context,
		clusterName string,
		cspType model.CSP,
		cspNameForLog string,
	) (timestamp time.Time, found bool, err error)
	FindLatestActiveEventByNodeAndType(
		ctx context.Context,
		nodeName string,
		maintenanceType model.MaintenanceType,
		statuses []model.InternalStatus,
	) (*model.MaintenanceEvent, bool, error)
	FindLatestOngoingEventByNode(ctx context.Context, nodeName string) (*model.MaintenanceEvent, bool, error)
	FindActiveEventsByStatuses(ctx context.Context, csp model.CSP, statuses []string) ([]model.MaintenanceEvent, error)
}

// MongoStore implements the DataStore interface using MongoDB.
type MongoStore struct {
	client         *mongo.Collection
	collectionName string
}

var _ Store = (*MongoStore)(nil)
var _ datastore.DataStore = (*MongoStore)(nil)
var _ datastore.MaintenanceEventStore = (*MongoStore)(nil)
var _ datastore.HealthEventStore = (*MongoStore)(nil)

// NewStore creates a new MongoDB store client.
func NewStore(ctx context.Context, mongoClientCertMountPath *string) (*MongoStore, error) {
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		return nil, fmt.Errorf("MONGODB_URI environment variable is not set")
	}

	mongoDatabase := os.Getenv("MONGODB_DATABASE_NAME")
	if mongoDatabase == "" {
		return nil, fmt.Errorf("MONGODB_DATABASE_NAME environment variable is not set")
	}

	mongoCollection := os.Getenv("MONGODB_MAINTENANCE_EVENT_COLLECTION_NAME")
	if mongoCollection == "" {
		slog.Warn("MONGODB_MAINTENANCE_EVENT_COLLECTION_NAME not set, using default",
			"defaultCollection", DefaultMongoDBCollection)

		mongoCollection = DefaultMongoDBCollection
	}

	totalTimeoutSeconds, _ := getEnvAsInt("MONGODB_PING_TIMEOUT_TOTAL_SECONDS", 300)
	intervalSeconds, _ := getEnvAsInt("MONGODB_PING_INTERVAL_SECONDS", 5)
	totalCACertTimeoutSeconds, _ := getEnvAsInt("CA_CERT_MOUNT_TIMEOUT_TOTAL_SECONDS", 360)
	intervalCACertSeconds, _ := getEnvAsInt("CA_CERT_READ_INTERVAL_SECONDS", 5)

	if mongoClientCertMountPath == nil || *mongoClientCertMountPath == "" {
		return nil, fmt.Errorf("mongo client certificate mount path is required")
	}

	mongoConfig := MongoDBConfig{
		URI:        mongoURI,
		Database:   mongoDatabase,
		Collection: mongoCollection,
		ClientTLSCertConfig: MongoDBClientTLSCertConfig{
			TlsCertPath: filepath.Join(*mongoClientCertMountPath, "tls.crt"),
			TlsKeyPath:  filepath.Join(*mongoClientCertMountPath, "tls.key"),
			CaCertPath:  filepath.Join(*mongoClientCertMountPath, "ca.crt"),
		},
		TotalPingTimeoutSeconds:    totalTimeoutSeconds,
		TotalPingIntervalSeconds:   intervalSeconds,
		TotalCACertTimeoutSeconds:  totalCACertTimeoutSeconds,
		TotalCACertIntervalSeconds: intervalCACertSeconds,
	}

	slog.Info("Initializing MongoDB connection",
		"mongoURI", mongoURI,
		"database", mongoDatabase,
		"collection", mongoCollection)

	collection, err := GetCollectionClient(ctx, mongoConfig)
	if err != nil {
		// Consider adding a datastore connection metric error here
		return nil, fmt.Errorf("error initializing MongoDB collection client: %w", err)
	}

	slog.Info("MongoDB collection client initialized successfully.")

	// Ensure Indexes Exist
	indexModels := []mongo.IndexModel{
		{
			Keys:    bson.D{bson.E{Key: "eventId", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("unique_eventid"),
		},
		{
			Keys:    bson.D{bson.E{Key: "status", Value: 1}, bson.E{Key: "scheduledStartTime", Value: 1}},
			Options: options.Index().SetName("status_scheduledstart"),
		},
		{
			Keys:    bson.D{bson.E{Key: "status", Value: 1}, bson.E{Key: "actualEndTime", Value: 1}},
			Options: options.Index().SetName("status_actualend"),
		},
		{Keys: bson.D{
			bson.E{Key: "csp", Value: 1},
			bson.E{Key: "clusterName", Value: 1},
			bson.E{Key: "eventReceivedTimestamp", Value: -1},
		}, Options: options.Index().SetName("csp_cluster_received_desc")},
		{
			Keys:    bson.D{bson.E{Key: "cspStatus", Value: 1}},
			Options: options.Index().SetName("csp_status"),
		},
	}

	indexView := collection.Indexes()

	_, indexErr := indexView.CreateMany(ctx, indexModels)
	if indexErr != nil {
		// Consider adding a datastore index creation metric error here (but maybe only warning level)
		slog.Warn("Failed to create indexes (they might already exist)", "error", indexErr)
	} else {
		slog.Info("Successfully created or ensured MongoDB indexes exist.")
	}

	return &MongoStore{
		client:         collection,
		collectionName: mongoCollection,
	}, nil
}

// NewChangeStreamWatcher creates a new change stream watcher for the MongoDB datastore
// This method makes MongoDB compatible with the new datastore abstraction layer
func (m *MongoStore) NewChangeStreamWatcher(
	ctx context.Context, config interface{},
) (datastore.ChangeStreamWatcher, error) {
	// Convert the generic config to MongoDB-specific config
	var mongoConfig MongoDBChangeStreamConfig

	// Handle different config types that might be passed
	switch c := config.(type) {
	case MongoDBChangeStreamConfig:
		mongoConfig = c
	case map[string]interface{}:
		// Support generic config format
		if collectionName, ok := c["CollectionName"].(string); ok {
			mongoConfig.CollectionName = collectionName
		}

		if clientName, ok := c["ClientName"].(string); ok {
			mongoConfig.ClientName = clientName
		}

		// Support custom pipeline for filtering
		if pipeline, ok := c["Pipeline"]; ok {
			mongoConfig.Pipeline = pipeline
		}

		// PollInterval is unused for MongoDB but included for interface compatibility
		mongoConfig.PollInterval = 5 * time.Second
	default:
		return nil, fmt.Errorf("unsupported config type for MongoDB change stream: %T", config)
	}

	// Validate required fields

	if mongoConfig.CollectionName == "" {
		mongoConfig.CollectionName = "health_events" // Default collection
	}

	if mongoConfig.ClientName == "" {
		return nil, fmt.Errorf("ClientName is required for MongoDB change stream watcher")
	}

	return m.newMongoChangeStreamWatcher(ctx, mongoConfig)
}

// getEnvAsInt parses an integer environment variable.
func getEnvAsInt(name string, defaultVal int) (int, error) {
	valueStr := os.Getenv(name)
	if valueStr == "" {
		return defaultVal, nil
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		slog.Warn("Invalid integer value for environment variable; using default",
			"name", name,
			"value", valueStr,
			"default", defaultVal,
			"error", err)

		return defaultVal, fmt.Errorf("invalid value for %s: %s", name, valueStr)
	}

	return value, nil
}

// executeUpsert performs the UpdateOne with retries for the given merged event.
func (s *MongoStore) executeUpsert(ctx context.Context, filter bson.D, event *model.MaintenanceEvent) error {
	update := bson.M{"$set": event}
	opts := options.Update().SetUpsert(true)

	var lastErr error

	for i := 1; i <= maxRetries; i++ {
		slog.Debug("Attempt to upsert maintenance event",
			"attempt", i,
			"eventID", event.EventID)

		result, err := s.client.UpdateOne(ctx, filter, update, opts)
		if err == nil {
			switch {
			case result.UpsertedCount > 0:
				slog.Debug("Inserted new maintenance event", "eventID", event.EventID)
			case result.ModifiedCount > 0:
				slog.Debug("Updated existing maintenance event", "eventID", event.EventID)
			default:
				slog.Debug("Matched existing maintenance event but no fields changed",
					"eventID", event.EventID)
			}

			return nil
		}

		lastErr = err
		slog.Warn("Attempt failed to upsert event; retrying",
			"attempt", i,
			"eventID", event.EventID,
			"error", err)
		time.Sleep(retryDelay)
	}

	return fmt.Errorf("upsert failed for event %s after %d retries: %w", event.EventID, maxRetries, lastErr)
}

// UpsertMaintenanceEvent inserts or updates a maintenance event.
// Metrics are handled by the caller (Processor).
func (s *MongoStore) UpsertMaintenanceEvent(ctx context.Context, event *model.MaintenanceEvent) error {
	if event == nil || event.EventID == "" {
		return fmt.Errorf("invalid event passed to UpsertMaintenanceEvent (nil or empty EventID)")
	}

	filter := bson.D{bson.E{Key: "eventId", Value: event.EventID}}
	event.LastUpdatedTimestamp = time.Now().UTC()

	// Since Processor now prepares the event fully, we directly upsert.
	// The fetchExistingEvent and mergeEvents logic is removed based on the confidence
	// that each EventID is processed once in its final state by the Processor.
	slog.Debug("Upserting event directly as prepared by Processor", "eventID", event.EventID)

	return s.executeUpsert(ctx, filter, event)
}

// FindEventsToTriggerQuarantine finds events ready for quarantine trigger.
// Metrics (duration, errors) handled by the caller (Trigger Engine).
func (s *MongoStore) FindEventsToTriggerQuarantine(
	ctx context.Context,
	triggerTimeLimit time.Duration,
) ([]model.MaintenanceEvent, error) {
	now := time.Now().UTC()
	triggerBefore := now.Add(triggerTimeLimit)

	filter := bson.D{
		bson.E{Key: "status", Value: model.StatusDetected},
		bson.E{Key: "scheduledStartTime", Value: bson.D{
			bson.E{Key: "$gt", Value: now},
			bson.E{Key: "$lte", Value: triggerBefore},
		}},
	}

	slog.Debug("Querying for quarantine triggers",
		"status", model.StatusDetected,
		"currentTime", now.Format(time.RFC3339),
		"triggerBefore", triggerBefore.Format(time.RFC3339))

	cursor, err := s.client.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query events for quarantine trigger: %w", err)
	}

	defer cursor.Close(ctx)

	var results []model.MaintenanceEvent
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode maintenance events for quarantine trigger: %w", err)
	}

	slog.Debug("Found events potentially ready for quarantine trigger", "count", len(results))

	return results, nil
}

// FindEventsToTriggerHealthy finds completed events ready for healthy trigger.
// Metrics (duration, errors) handled by the caller (Trigger Engine).
func (s *MongoStore) FindEventsToTriggerHealthy(
	ctx context.Context,
	healthyDelay time.Duration,
) ([]model.MaintenanceEvent, error) {
	now := time.Now().UTC()
	triggerIfEndedBefore := now.Add(-healthyDelay) // Event must have ended *before* or *at* this time

	filter := bson.D{
		bson.E{Key: "status", Value: model.StatusMaintenanceComplete},
		bson.E{Key: "actualEndTime", Value: bson.D{
			bson.E{Key: "$ne", Value: nil},                   // actualEndTime must exist
			bson.E{Key: "$lte", Value: triggerIfEndedBefore}, // and be sufficiently in the past
		}},
	}

	slog.Debug("Querying for healthy triggers",
		"status", model.StatusMaintenanceComplete,
		"actualEndTimeBefore", triggerIfEndedBefore.Format(time.RFC3339))

	cursor, err := s.client.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query events for healthy trigger: %w", err)
	}

	defer cursor.Close(ctx)

	var results []model.MaintenanceEvent
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode maintenance events for healthy trigger: %w", err)
	}

	slog.Debug("Found events potentially ready for healthy trigger", "count", len(results))

	return results, nil
}

// UpdateEventStatus updates only the status and timestamp.
// Metrics handled by the caller (Trigger Engine).
func (s *MongoStore) UpdateEventStatus(ctx context.Context, eventID string, newStatus model.InternalStatus) error {
	if eventID == "" {
		return fmt.Errorf("cannot update status for empty eventID")
	}

	filter := bson.D{bson.E{Key: "eventId", Value: eventID}}
	update := bson.D{
		bson.E{Key: "$set", Value: bson.D{
			bson.E{Key: "status", Value: newStatus},
			bson.E{Key: "lastUpdatedTimestamp", Value: time.Now().UTC()},
		}},
	}

	var err error

	var result *mongo.UpdateResult

	for i := 1; i <= maxRetries; i++ {
		slog.Debug("Attempt to update status for event",
			"attempt", i,
			"newStatus", newStatus,
			"eventID", eventID)

		result, err = s.client.UpdateOne(ctx, filter, update)
		if err == nil {
			if result.MatchedCount == 0 {
				slog.Warn("Attempted to update status for non-existent event", "eventID", eventID)
				return nil // Not an error if event is gone
			}

			slog.Debug("Successfully updated status for event",
				"newStatus", newStatus,
				"eventID", eventID)

			return nil // Success
		}

		slog.Warn("Attempt failed to update status for event; retrying",
			"attempt", i,
			"eventID", eventID,
			"error", err,
			"retryDelay", retryDelay)
		time.Sleep(retryDelay)
	}

	return fmt.Errorf(
		"failed to update status for event (EventID: %s) after %d retries: %w",
		eventID, maxRetries, err)
}

// GetLastProcessedEventTimestampByCSP is a helper to get the latest event timestamp for a given CSP.
func (s *MongoStore) GetLastProcessedEventTimestampByCSP(
	ctx context.Context,
	clusterName string,
	cspType model.CSP,
	cspNameForLog string,
) (timestamp time.Time, found bool, err error) {
	filter := bson.D{bson.E{Key: "csp", Value: cspType}}
	if clusterName != "" {
		filter = append(filter, bson.E{Key: "clusterName", Value: clusterName})
	}

	findOptions := options.FindOne().
		SetSort(bson.D{bson.E{Key: "eventReceivedTimestamp", Value: -1}})
		// Sort by internal received time

	slog.Debug("Querying for last processed timestamp",
		"csp", cspNameForLog)

	var latestEvent model.MaintenanceEvent
	dbErr := s.client.FindOne(ctx, filter, findOptions).Decode(&latestEvent)

	if dbErr != nil {
		if errors.Is(dbErr, mongo.ErrNoDocuments) {
			slog.Debug("No previous event timestamp found in datastore",
				"csp", cspNameForLog,
				"cluster", clusterName)

			return time.Time{}, false, nil
		}

		slog.Error("Failed to query last processed log timestamp",
			"csp", cspNameForLog,
			"cluster", clusterName,
			"error", dbErr)

		return time.Time{}, false, fmt.Errorf("failed to query last %s log timestamp: %w", cspNameForLog, dbErr)
	}

	// Use EventReceivedTimestamp as the marker for when we processed it
	slog.Debug("Found last processed log timestamp",
		"csp", cspNameForLog,
		"cluster", clusterName,
		"timestamp", latestEvent.EventReceivedTimestamp,
		"eventID", latestEvent.EventID)

	return latestEvent.EventReceivedTimestamp, true, nil
}

// FindLatestActiveEventByNodeAndType finds the most recently updated event for a
// given node, type, and one of several statuses.
func (s *MongoStore) FindLatestActiveEventByNodeAndType(
	ctx context.Context,
	nodeName string,
	maintenanceType model.MaintenanceType,
	statuses []model.InternalStatus,
) (*model.MaintenanceEvent, bool, error) {
	if nodeName == "" || maintenanceType == "" || len(statuses) == 0 {
		return nil, false, fmt.Errorf("nodeName, maintenanceType, and at least one status are required")
	}

	filter := bson.D{
		bson.E{Key: "nodeName", Value: nodeName},
		bson.E{Key: "maintenanceType", Value: maintenanceType},
		bson.E{Key: "status", Value: bson.D{bson.E{Key: "$in", Value: statuses}}},
	}

	// Sort by LastUpdatedTimestamp descending to get the latest one.
	// If multiple have the exact same LastUpdatedTimestamp, this will pick one arbitrarily among them.
	// Consider adding a secondary sort key if more deterministic behavior is needed in such rare cases.
	findOptions := options.FindOne().SetSort(bson.D{bson.E{Key: "lastUpdatedTimestamp", Value: -1}})

	slog.Debug("Querying for latest active event",
		"node", nodeName,
		"maintenanceType", maintenanceType,
		"sort", "lastUpdatedTimestamp: -1")

	var latestEvent model.MaintenanceEvent

	err := s.client.FindOne(ctx, filter, findOptions).Decode(&latestEvent)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			slog.Info("No active event found", "node", nodeName, "type", maintenanceType, "statuses", statuses)
			return nil, false, nil
		}

		slog.Error("Failed to query latest active event",
			"node", nodeName,
			"type", maintenanceType,
			"statuses", statuses,
			"error", err)

		return nil, false, fmt.Errorf("failed to query latest active event: %w", err)
	}

	slog.Info("Found latest active event",
		"eventID", latestEvent.EventID,
		"node", nodeName,
		"type", maintenanceType,
		"statuses", statuses)

	return &latestEvent, true, nil
}

// FindLatestOngoingEventByNode finds the most recently updated ONGOING event for a given node.
func (s *MongoStore) FindLatestOngoingEventByNode(
	ctx context.Context,
	nodeName string,
) (*model.MaintenanceEvent, bool, error) {
	if nodeName == "" {
		return nil, false, fmt.Errorf("nodeName is required")
	}

	filter := bson.D{
		bson.E{Key: "nodeName", Value: nodeName},
		bson.E{Key: "status", Value: model.StatusMaintenanceOngoing},
	}
	opts := options.FindOne().SetSort(bson.D{bson.E{Key: "lastUpdatedTimestamp", Value: -1}})

	var event model.MaintenanceEvent

	err := s.client.FindOne(ctx, filter, opts).Decode(&event)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			slog.Debug("No ongoing event found for node", "node", nodeName)

			return nil, false, nil
		}

		return nil, false, fmt.Errorf("query latest ongoing event for node %s: %w", nodeName, err)
	}

	slog.Debug("Found ongoing event", "eventID", event.EventID, "node", nodeName)

	return &event, true, nil
}

// FindActiveEventsByStatuses finds active events by their csp status.
func (s *MongoStore) FindActiveEventsByStatuses(
	ctx context.Context,
	csp model.CSP,
	statuses []string,
) ([]model.MaintenanceEvent, error) {
	if len(statuses) == 0 {
		return nil, fmt.Errorf("at least one status is required")
	}

	filter := bson.D{
		bson.E{Key: "csp", Value: csp},
		bson.E{Key: "cspStatus", Value: bson.D{bson.E{Key: "$in", Value: statuses}}},
	}

	slog.Debug("Querying for active events",
		"csp", csp,
		"statuses", statuses)

	cursor, err := s.client.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query active events: %w", err)
	}

	defer cursor.Close(ctx)

	var results []model.MaintenanceEvent
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode maintenance events for active events: %w", err)
	}

	slog.Debug("Found active events", "count", len(results))

	return results, nil
}

// Ping checks if the MongoDB connection is alive
func (s *MongoStore) Ping(ctx context.Context) error {
	if err := s.client.Database().Client().Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return nil
}

// Close closes the MongoDB connection
func (s *MongoStore) Close(ctx context.Context) error {
	if s.client != nil {
		return s.client.Database().Client().Disconnect(ctx)
	}

	return nil
}

// MaintenanceEventStore returns the maintenance event store interface
func (s *MongoStore) MaintenanceEventStore() datastore.MaintenanceEventStore {
	return s
}

// HealthEventStore returns the health event store interface
func (s *MongoStore) HealthEventStore() datastore.HealthEventStore {
	return s
}

// InsertHealthEvents inserts health events into MongoDB
func (s *MongoStore) InsertHealthEvents(ctx context.Context, eventWithStatus *datastore.HealthEventWithStatus) error {
	if eventWithStatus == nil || eventWithStatus.HealthEvent == nil {
		return fmt.Errorf("invalid health event (nil)")
	}

	// Get health events collection (separate from maintenance events)
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Type assert the HealthEvent to the proper concrete type
	healthEvent, ok := eventWithStatus.HealthEvent.(*platform_connectors.HealthEvent)
	if !ok {
		return fmt.Errorf("expected *platform_connectors.HealthEvent, got %T", eventWithStatus.HealthEvent)
	}

	// Create a document with both structured fields for indexing and the full document
	document := bson.M{
		"nodeName":                 healthEvent.NodeName,
		"eventType":                healthEvent.CheckName,      // Use CheckName as event type
		"severity":                 healthEvent.ComponentClass, // Use ComponentClass as severity
		"recommendedAction":        healthEvent.RecommendedAction.String(),
		"nodeQuarantined":          eventWithStatus.HealthEventStatus.NodeQuarantined,
		"userPodsEvictionStatus":   eventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status,
		"userPodsEvictionMessage":  eventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Message,
		"faultRemediated":          eventWithStatus.HealthEventStatus.FaultRemediated,
		"lastRemediationTimestamp": eventWithStatus.HealthEventStatus.LastRemediationTimestamp,
		"document":                 eventWithStatus, // Store the full document
		"createdAt":                eventWithStatus.CreatedAt,
		"updatedAt":                time.Now().UTC(),
	}

	_, err := healthEventsCollection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to insert health event for node %s: %w", healthEvent.NodeName, err)
	}

	klog.V(2).Infof("Successfully inserted health event for node: %s", healthEvent.NodeName)

	return nil
}

// UpdateHealthEventStatus updates the status of a health event in MongoDB
func (s *MongoStore) UpdateHealthEventStatus(ctx context.Context, id string, status datastore.HealthEventStatus) error {
	if id == "" {
		return fmt.Errorf("health event ID is required")
	}

	// Convert string ID to ObjectID
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid health event ID format: %w", err)
	}

	// Get health events collection
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Build update document - only set non-nil fields to avoid overwriting existing values
	setFields := bson.M{
		"userPodsEvictionStatus":     status.UserPodsEvictionStatus.Status,
		"userPodsEvictionMessage":    status.UserPodsEvictionStatus.Message,
		"document.healtheventstatus": status, // Update the nested document
		"updatedAt":                  time.Now().UTC(),
	}

	// Only set these fields if they are non-nil
	if status.NodeQuarantined != nil {
		setFields["nodeQuarantined"] = status.NodeQuarantined
	}

	if status.FaultRemediated != nil {
		setFields["faultRemediated"] = status.FaultRemediated
	}

	if status.LastRemediationTimestamp != nil {
		setFields["lastRemediationTimestamp"] = status.LastRemediationTimestamp
	}

	update := bson.M{"$set": setFields}

	result, err := healthEventsCollection.UpdateOne(ctx, bson.M{"_id": objectID}, update)
	if err != nil {
		return fmt.Errorf("failed to update health event status for ID %s: %w", id, err)
	}

	if result.MatchedCount == 0 {
		klog.Warningf("Attempted to update status for non-existent health event: %s", id)
	} else {
		klog.V(2).Infof("Successfully updated health event status for ID: %s", id)
	}

	return nil
}

// UpdateHealthEventStatusByNode updates the status of all unremediated health events for a given node.
// This method is used by node-drainer to update MongoDB after drain completion, which triggers
// change stream events for fault-remediation to pick up immediately.
//
//nolint:cyclop // Function handles multiple validation steps and error cases sequentially
func (s *MongoStore) UpdateHealthEventStatusByNode(
	ctx context.Context, nodeName string, status datastore.HealthEventStatus,
) error {
	if nodeName == "" {
		return fmt.Errorf("node name is required")
	}

	// Get health events collection
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Find all health events for this node
	filter := bson.M{
		"nodeName": nodeName,
		// Only update unremediated events
		"$or": []bson.M{
			{"document.healtheventstatus.faultRemediated": nil},
			{"document.healtheventstatus.faultRemediated": false},
		},
	}

	// Build update document
	updateDoc := bson.M{
		"$set": bson.M{
			"document.healtheventstatus.userPodsEvictionStatus": status.UserPodsEvictionStatus,
			"updatedAt": time.Now().UTC(),
		},
	}

	// Update all matching documents
	result, err := healthEventsCollection.UpdateMany(ctx, filter, updateDoc)
	if err != nil {
		return fmt.Errorf("failed to update health event status for node %s: %w", nodeName, err)
	}

	if result.MatchedCount == 0 {
		klog.V(2).Infof("No unremediated health events found for node %s", nodeName)
	} else {
		klog.Infof("Successfully updated %d health event(s) for node %s", result.ModifiedCount, nodeName)
	}

	return nil
}

// FindHealthEventsByNode finds all health events for a specific node in MongoDB
func (s *MongoStore) FindHealthEventsByNode(
	ctx context.Context, nodeName string,
) ([]datastore.HealthEventWithStatus, error) {
	if nodeName == "" {
		return nil, fmt.Errorf("node name is required")
	}

	// Get health events collection
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Query for events by node name, sorted by creation time (newest first)
	filter := bson.M{"nodeName": nodeName}
	options := options.Find().SetSort(bson.M{"createdAt": -1})

	cursor, err := healthEventsCollection.Find(ctx, filter, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query health events for node %s: %w", nodeName, err)
	}
	defer cursor.Close(ctx)

	var results []datastore.HealthEventWithStatus

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			klog.Warningf("Failed to decode health event: %v", err)
			continue
		}

		// Extract the full document
		if documentRaw, exists := doc["document"]; exists {
			// Convert to HealthEventWithStatus
			documentBytes, err := bson.Marshal(documentRaw)
			if err != nil {
				klog.Warningf("Failed to marshal health event document: %v", err)
				continue
			}

			var event datastore.HealthEventWithStatus
			if err := bson.Unmarshal(documentBytes, &event); err != nil {
				klog.Warningf("Failed to unmarshal health event: %v", err)
				continue
			}

			results = append(results, event)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("error iterating health events: %w", err)
	}

	klog.V(2).Infof("Found %d health events for node %s", len(results), nodeName)

	return results, nil
}

// FindHealthEventsByFilter finds health events based on filter criteria in MongoDB
func (s *MongoStore) FindHealthEventsByFilter(
	ctx context.Context, filter map[string]interface{},
) ([]datastore.HealthEventWithStatus, error) {
	if len(filter) == 0 {
		return nil, fmt.Errorf("filter cannot be empty")
	}

	// Get health events collection
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Convert filter to MongoDB query
	mongoFilter := s.convertFilterToMongoQuery(filter)

	// Execute query and return results
	return s.executeHealthEventsQuery(ctx, healthEventsCollection, mongoFilter)
}

// FindHealthEventsByStatus finds health events matching a specific status
// Used for recovery scenarios (e.g., finding in-progress drain operations on restart)
func (s *MongoStore) FindHealthEventsByStatus(
	ctx context.Context, status datastore.Status,
) ([]datastore.HealthEventWithStatus, error) {
	if status == "" {
		return nil, fmt.Errorf("status cannot be empty")
	}

	// Get health events collection
	healthEventsCollection := s.client.Database().Collection("HealthEvents")

	// Query for events by status
	// Note: This queries the UserPodsEvictionStatus for node-drainer's use case
	mongoFilter := bson.M{
		"healtheventstatus.userpodsevictionstatus.status": string(status),
	}

	klog.V(2).Infof("Querying health events by status: %s", status)

	// Execute query and return results
	return s.executeHealthEventsQuery(ctx, healthEventsCollection, mongoFilter)
}

// convertFilterToMongoQuery converts filter map to MongoDB query format
func (s *MongoStore) convertFilterToMongoQuery(filter map[string]interface{}) bson.M {
	mongoFilter := bson.M{}

	for key, value := range filter {
		if mongoKey := s.getMongoFieldKey(key); mongoKey != "" {
			mongoFilter[mongoKey] = value
		} else {
			// For other fields, query the nested document
			mongoFilter[fmt.Sprintf("document.%s", key)] = value
		}
	}

	return mongoFilter
}

// getMongoFieldKey maps filter keys to MongoDB field names
func (s *MongoStore) getMongoFieldKey(key string) string {
	fieldMappings := map[string]string{
		"node_name":                 "nodeName",
		"nodeName":                  "nodeName",
		"event_type":                "eventType",
		"eventType":                 "eventType",
		"severity":                  "severity",
		"recommended_action":        "recommendedAction",
		"recommendedAction":         "recommendedAction",
		"node_quarantined":          "nodeQuarantined",
		"nodeQuarantined":           "nodeQuarantined",
		"user_pods_eviction_status": "userPodsEvictionStatus",
		"userPodsEvictionStatus":    "userPodsEvictionStatus",
		"fault_remediated":          "faultRemediated",
		"faultRemediated":           "faultRemediated",
	}

	return fieldMappings[key]
}

// executeHealthEventsQuery executes the MongoDB query and processes results
func (s *MongoStore) executeHealthEventsQuery(
	ctx context.Context,
	healthEventsCollection *mongo.Collection,
	mongoFilter bson.M,
) ([]datastore.HealthEventWithStatus, error) {
	// Query with sorting (newest first)
	options := options.Find().SetSort(bson.M{"createdAt": -1})

	cursor, err := healthEventsCollection.Find(ctx, mongoFilter, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query health events with filter: %w", err)
	}
	defer cursor.Close(ctx)

	return s.processHealthEventsCursor(ctx, cursor)
}

// processHealthEventsCursor processes the MongoDB cursor and extracts health events
func (s *MongoStore) processHealthEventsCursor(
	ctx context.Context,
	cursor *mongo.Cursor,
) ([]datastore.HealthEventWithStatus, error) {
	var results []datastore.HealthEventWithStatus

	for cursor.Next(ctx) {
		event, err := s.decodeHealthEventFromCursor(ctx, cursor)
		if err != nil {
			// Log warning and continue with next document
			klog.Warningf("Failed to decode health event: %v", err)
			continue
		}

		if event != nil {
			results = append(results, *event)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("error iterating health events: %w", err)
	}

	klog.V(2).Infof("Found %d health events matching filter", len(results))

	return results, nil
}

// decodeHealthEventFromCursor decodes a single health event from cursor
func (s *MongoStore) decodeHealthEventFromCursor(
	ctx context.Context,
	cursor *mongo.Cursor,
) (*datastore.HealthEventWithStatus, error) {
	var doc bson.M
	if err := cursor.Decode(&doc); err != nil {
		return nil, err
	}

	// Extract the full document
	documentRaw, exists := doc["document"]
	if !exists {
		return nil, nil // No document field, skip
	}

	// Convert to HealthEventWithStatus
	documentBytes, err := bson.Marshal(documentRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal health event document: %w", err)
	}

	var event datastore.HealthEventWithStatus
	if err := bson.Unmarshal(documentBytes, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal health event: %w", err)
	}

	return &event, nil
}

// InsertMany inserts multiple documents into the default collection
func (s *MongoStore) InsertMany(ctx context.Context, documents []interface{}) error {
	if len(documents) == 0 {
		return nil
	}

	// Use the default collection for the store
	collection := s.client.Database().Collection(s.collectionName)

	_, err := collection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert documents: %w", err)
	}

	klog.V(2).Infof("Successfully inserted %d documents", len(documents))

	return nil
}

// ExtractDocumentID provides MongoDB-specific document ID extraction
// This method is called via duck typing by common package utilities (see ExtractDocumentIDFromRawEvent)
// It keeps MongoDB-specific ObjectID handling in the MongoDB package
func (s *MongoStore) ExtractDocumentID(rawEvent map[string]interface{}) string {
	return ExtractRawObjectID(rawEvent)
}

// Verify that MongoStore implements DataStore interface
var _ datastore.DataStore = (*MongoStore)(nil)

/*
Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
)

// UpdateHealthEventStatusFromRaw updates MongoDB health event status using raw event
// This replaces the TEMPORARY WORKAROUNDs in fault-quarantine and node-drainer
func UpdateHealthEventStatusFromRaw(
	ctx context.Context,
	collection *mongo.Collection,
	rawEvent map[string]interface{},
	status datastore.HealthEventStatus,
) error {
	// Extract MongoDB-specific filter
	filter, err := ExtractUpdateFilter(rawEvent)
	if err != nil {
		return fmt.Errorf("failed to extract update filter: %w", err)
	}

	// Create MongoDB-specific update document
	update := CreateStatusUpdateDocument(status)

	// Validate the operation before executing
	if err := ValidateMongoUpdateOperation(filter, update); err != nil {
		return fmt.Errorf("invalid update operation: %w", err)
	}

	// Execute the update
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update health event status: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("no document found matching the filter")
	}

	return nil
}

// UpdateHealthEventStatusFromRawWithNodeQuarantined is a specialized version for node quarantine status
func UpdateHealthEventStatusFromRawWithNodeQuarantined(
	ctx context.Context,
	collection *mongo.Collection,
	rawEvent map[string]interface{},
	nodeQuarantinedStatus datastore.Status,
) error {
	status := datastore.HealthEventStatus{
		NodeQuarantined: &nodeQuarantinedStatus,
	}

	return UpdateHealthEventStatusFromRaw(ctx, collection, rawEvent, status)
}

// UpdateHealthEventStatusFromRawWithUserPodsEviction is a specialized version for user pods eviction status
func UpdateHealthEventStatusFromRawWithUserPodsEviction(
	ctx context.Context,
	collection *mongo.Collection,
	rawEvent map[string]interface{},
	userPodsEvictionStatus datastore.OperationStatus,
) error {
	status := datastore.HealthEventStatus{
		UserPodsEvictionStatus: userPodsEvictionStatus,
	}

	return UpdateHealthEventStatusFromRaw(ctx, collection, rawEvent, status)
}

// ExtractUpdateFilter creates MongoDB filter from raw change stream event
func ExtractUpdateFilter(rawEvent map[string]interface{}) (bson.M, error) {
	// Try to get document ID from fullDocument
	if fullDoc, ok := rawEvent["fullDocument"].(map[string]interface{}); ok {
		if id, exists := fullDoc["_id"]; exists {
			return bson.M{"_id": id}, nil
		}
	}

	// Fallback: try documentKey
	if docKey, ok := rawEvent["documentKey"].(map[string]interface{}); ok {
		if id, exists := docKey["_id"]; exists {
			return bson.M{"_id": id}, nil
		}
	}

	return nil, fmt.Errorf("could not extract document ID for filter")
}

// CreateStatusUpdateDocument creates MongoDB update document for status
func CreateStatusUpdateDocument(status datastore.HealthEventStatus) bson.M {
	update := bson.M{"$set": bson.M{}}

	if status.NodeQuarantined != nil {
		update["$set"].(bson.M)["healtheventstatus.nodequarantined"] = *status.NodeQuarantined
	}

	if status.UserPodsEvictionStatus.Status != "" {
		update["$set"].(bson.M)["healtheventstatus.userpodsevictionstatus"] = status.UserPodsEvictionStatus
	}

	if status.FaultRemediated != nil {
		update["$set"].(bson.M)["healtheventstatus.faultremediated"] = *status.FaultRemediated
	}

	if status.LastRemediationTimestamp != nil {
		update["$set"].(bson.M)["healtheventstatus.lastremediationtimestamp"] = *status.LastRemediationTimestamp
	}

	return update
}

// ValidateMongoUpdateOperation validates MongoDB filter and update documents
func ValidateMongoUpdateOperation(filter, update bson.M) error {
	if len(filter) == 0 {
		return fmt.Errorf("filter cannot be empty")
	}

	if _, exists := filter["_id"]; !exists {
		return fmt.Errorf("filter must contain _id field")
	}

	if len(update) == 0 {
		return fmt.Errorf("update cannot be empty")
	}

	return nil
}

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

package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

// ExtractHealthEventFromRawEvent extracts a HealthEventWithStatus from a raw event map
func ExtractHealthEventFromRawEvent(event map[string]interface{}) (*datastore.HealthEventWithStatus, error) {
	// Handle different event formats (MongoDB change stream vs direct document)
	var document map[string]interface{}

	// Check if this is a MongoDB change stream event
	if fullDocument, exists := event["fullDocument"]; exists {
		if doc, ok := fullDocument.(map[string]interface{}); ok {
			document = doc
		} else {
			return nil, datastore.NewValidationError(
				"", // Provider unknown at this level
				"fullDocument is not a valid document",
				nil,
			)
		}
	} else {
		// Assume it's a direct document
		document = event
	}

	// Extract the health event
	healthEvent := &datastore.HealthEventWithStatus{
		RawEvent: event,
	}

	// Extract CreatedAt timestamp
	if createdAtVal, exists := document["createdAt"]; exists {
		if createdAt, err := parseTimestamp(createdAtVal); err == nil {
			healthEvent.CreatedAt = createdAt
		} else {
			return nil, datastore.NewValidationError(
				"",
				"invalid createdAt timestamp format",
				err,
			).WithMetadata("createdAt", createdAtVal)
		}
	} else {
		// Default to current time if not present
		healthEvent.CreatedAt = time.Now()
	}

	// Extract the actual health event data
	if healthEventData, exists := document["healthevent"]; exists {
		healthEvent.HealthEvent = healthEventData
	}

	// Extract health event status
	if statusData, exists := document["healtheventstatus"]; exists {
		status, err := parseHealthEventStatus(statusData)
		if err != nil {
			return nil, datastore.NewValidationError(
				"",
				"failed to parse health event status",
				err,
			).WithMetadata("status", statusData)
		}

		healthEvent.HealthEventStatus = status
	}

	return healthEvent, nil
}

// parseTimestamp handles various timestamp formats
func parseTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try parsing common timestamp formats
		for _, layout := range []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				return t, nil
			}
		}

		return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", v)
	case int64:
		// Unix timestamp
		return time.Unix(v, 0), nil
	case float64:
		// Unix timestamp as float
		return time.Unix(int64(v), 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

// parseHealthEventStatus converts a map to HealthEventStatus
func parseHealthEventStatus(value interface{}) (datastore.HealthEventStatus, error) {
	var status datastore.HealthEventStatus

	// Convert to JSON and back to handle various map types
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return status, fmt.Errorf("failed to marshal status: %w", err)
	}

	if err := json.Unmarshal(jsonBytes, &status); err != nil {
		return status, fmt.Errorf("failed to unmarshal status: %w", err)
	}

	return status, nil
}

// ExtractDocumentID extracts the document ID from a raw event
func ExtractDocumentID(event map[string]interface{}) (string, error) {
	// Try MongoDB ObjectId format first
	if id, exists := event["_id"]; exists {
		return convertIDToString(id), nil
	}

	// Try PostgreSQL uuid format
	if id, exists := event["id"]; exists {
		return convertIDToString(id), nil
	}

	// Try in fullDocument for change stream events
	if fullDoc, exists := event["fullDocument"]; exists {
		if id, err := extractIDFromFullDocument(fullDoc); err == nil {
			return id, nil
		}
	}

	return "", datastore.NewValidationError(
		"",
		"no document ID found in event",
		nil,
	).WithMetadata("event", event)
}

// extractIDFromDocument extracts ID from a document interface, trying both MongoDB and PostgreSQL formats
func extractIDFromDocument(doc interface{}) interface{} {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		return nil
	}

	// Try MongoDB _id format first
	if id, exists := docMap["_id"]; exists {
		return id
	}

	// Try PostgreSQL id format
	if id, exists := docMap["id"]; exists {
		return id
	}

	return nil
}

// ExtractDocumentIDNative extracts the document ID from a raw event while preserving its native type.
// This is useful for database operations that require the native ID type (e.g., MongoDB ObjectID).
// Unlike ExtractDocumentID which converts to string, this preserves the original type for queries.
func ExtractDocumentIDNative(event map[string]interface{}) (interface{}, error) {
	// Try MongoDB _id format first (preserves ObjectID type)
	if id, exists := event["_id"]; exists {
		return id, nil
	}

	// Try PostgreSQL id/uuid format
	if id, exists := event["id"]; exists {
		return id, nil
	}

	// Try in fullDocument for change stream events
	if fullDoc, exists := event["fullDocument"]; exists {
		if id := extractIDFromDocument(fullDoc); id != nil {
			return id, nil
		}
	}

	return nil, datastore.NewValidationError(
		"",
		"no document ID found in event",
		nil,
	).WithMetadata("event", event)
}

// ExtractNodeName extracts the node name from a health event
func ExtractNodeName(healthEvent interface{}) (string, error) {
	// Convert to map for field access
	eventMap, err := convertToMap(healthEvent)
	if err != nil {
		return "", fmt.Errorf("failed to convert health event to map: %w", err)
	}

	// Try common node name fields
	nodeNameFields := []string{"nodename", "nodeName", "node_name", "node"}

	for _, field := range nodeNameFields {
		if nodeName, exists := eventMap[field]; exists {
			if nodeNameStr, ok := nodeName.(string); ok {
				return nodeNameStr, nil
			}
		}
	}

	return "", datastore.NewValidationError(
		"",
		"no node name found in health event",
		nil,
	).WithMetadata("healthEvent", healthEvent)
}

// convertToMap converts various types to map[string]interface{}
func convertToMap(value interface{}) (map[string]interface{}, error) {
	if m, ok := value.(map[string]interface{}); ok {
		return m, nil
	}

	// Use reflection for struct types
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return convertUsingJSON(value)
	}

	return convertStructToMap(v)
}

// convertUsingJSON converts a value to map using JSON marshaling/unmarshaling
func convertUsingJSON(value interface{}) (map[string]interface{}, error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return result, nil
}

// convertStructToMap converts a struct to map using reflection
func convertStructToMap(v reflect.Value) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		if !value.CanInterface() {
			continue
		}

		fieldName := getFieldName(field)
		result[fieldName] = value.Interface()
	}

	return result, nil
}

// getFieldName extracts the field name, preferring JSON tags
func getFieldName(field reflect.StructField) string {
	fieldName := field.Name

	if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
		if commaIdx := len(jsonTag); commaIdx > 0 {
			fieldName = jsonTag[:commaIdx]
		}
	}

	return fieldName
}

// MergeMetadata merges multiple metadata maps, with later maps taking precedence
func MergeMetadata(metadata ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for _, m := range metadata {
		for k, v := range m {
			result[k] = v
		}
	}

	return result
}

// ValidateHealthEvent validates that a health event has required fields
func ValidateHealthEvent(event *datastore.HealthEventWithStatus) error {
	if event == nil {
		return datastore.NewValidationError(
			"",
			"health event is nil",
			nil,
		)
	}

	// Validate that we have a health event
	if event.HealthEvent == nil {
		return datastore.NewValidationError(
			"",
			"health event data is missing",
			nil,
		)
	}

	// Validate that we can extract a node name
	if _, err := ExtractNodeName(event.HealthEvent); err != nil {
		return datastore.NewValidationError(
			"",
			"health event missing node name",
			err,
		).WithMetadata("healthEvent", event.HealthEvent)
	}

	return nil
}

// CreateHealthEventFilter creates a filter for querying health events by node
func CreateHealthEventFilter(nodeName string) datastore.Document {
	return datastore.D(
		datastore.E("healthevent.nodename", nodeName),
	)
}

// CreateStatusUpdateDocument creates a document for updating health event status
func CreateStatusUpdateDocument(status datastore.HealthEventStatus) datastore.Document {
	return datastore.D(
		datastore.E("$set", datastore.D(
			datastore.E("healtheventstatus", status),
			datastore.E("lastModified", time.Now()),
		)),
	)
}

// extractIDFromFullDocument extracts document ID from fullDocument field
// This helper function reduces complexity in ExtractDocumentID
func extractIDFromFullDocument(fullDoc interface{}) (string, error) {
	doc, ok := fullDoc.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("fullDocument is not a map")
	}

	// Try MongoDB _id format first
	if id, exists := doc["_id"]; exists {
		return convertIDToString(id), nil
	}

	// Try PostgreSQL id format
	if id, exists := doc["id"]; exists {
		return convertIDToString(id), nil
	}

	return "", fmt.Errorf("no ID found in fullDocument")
}

// convertIDToString converts various ID types to their string representation
// Handles MongoDB ObjectID, PostgreSQL UUID, and other formats
func convertIDToString(id interface{}) string {
	// Check if it's a MongoDB ObjectID (has Hex() method)
	if objectID, ok := id.(interface{ Hex() string }); ok {
		return objectID.Hex()
	}

	// Fall back to standard string conversion for other types
	return fmt.Sprintf("%v", id)
}

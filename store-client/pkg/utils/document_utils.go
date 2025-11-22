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
	"log/slog"
	"strings"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

// tryExtractIDFromEventID attempts to extract a valid document ID from event["_id"]
// Returns the ID and true if valid, or empty string and false if it's a resume token
func tryExtractIDFromEventID(id interface{}) (string, bool) {
	// Check if this is a changestream resume token (map with _data field)
	idMap, isMap := id.(map[string]interface{})
	if isMap {
		if _, hasData := idMap["_data"]; hasData {
			// This is a PostgreSQL changestream resume token, not a document ID
			slog.Warn("[EXTRACT-ID] Skipping _id with _data field (resume token)", "_id", id)
			return "", false
		}

		// This is a MongoDB map-style ID, convert it
		slog.Info("[EXTRACT-ID] Found MongoDB map ID in _id", "id", id)

		return convertIDToString(id), true
	}

	// Simple string or ObjectID
	slog.Info("[EXTRACT-ID] Found simple ID in _id", "id", id)

	return convertIDToString(id), true
}

// ExtractDocumentID extracts the document ID from a raw event
func ExtractDocumentID(event map[string]interface{}) (string, error) {
	// For changestream events, try fullDocument first
	// This is important because in PostgreSQL changestreams, event["_id"] contains
	// the resume token metadata ({"_data": "123"}), not the actual document ID.
	if fullDoc, exists := event["fullDocument"]; exists {
		if id, err := extractIDFromFullDocument(fullDoc); err == nil {
			slog.Info("[EXTRACT-ID] Found ID in fullDocument", "id", id)
			return id, nil
		}
	}

	// Try MongoDB ObjectId format (for direct MongoDB queries, not changestreams)
	if id, exists := event["_id"]; exists {
		if docID, ok := tryExtractIDFromEventID(id); ok {
			return docID, nil
		}
	}

	// Try PostgreSQL uuid format (for direct PostgreSQL queries)
	if id, exists := event["id"]; exists {
		slog.Info("[EXTRACT-ID] Found ID in top-level id field", "id", id)
		return convertIDToString(id), nil
	}

	return "", datastore.NewValidationError(
		"",
		"no document ID found in event",
		nil,
	).WithMetadata("event", event)
}

// extractIDFromDocument extracts ID from a document interface, trying both MongoDB and PostgreSQL formats
func extractIDFromDocument(doc interface{}) interface{} {
	fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] extractIDFromDocument called with doc type: %T\n", doc)

	docMap, ok := doc.(map[string]interface{})
	if !ok {
		fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] FAILED - doc is not map[string]interface{}\n")

		return nil
	}

	fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] doc is a map with keys: %v\n", getMapKeysFromEvent(docMap))

	// Try MongoDB _id format first
	if id, exists := docMap["_id"]; exists {
		fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] SUCCESS - found '_id': %v (type: %T)\n", id, id)

		return id
	}

	fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] No '_id' found, trying 'id'...\n")

	// Try PostgreSQL id format
	if id, exists := docMap["id"]; exists {
		fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] SUCCESS - found 'id': %v (type: %T)\n", id, id)

		return id
	}

	fmt.Printf("[EXTRACT-FROM-DOC-DEBUG-v4] FAILED - no 'id' found either\n")

	return nil
}

// ExtractDocumentIDNative extracts the document ID from a raw event while preserving its native type.
// This is useful for database operations that require the native ID type (e.g., MongoDB ObjectID).
// Unlike ExtractDocumentID which converts to string, this preserves the original type for queries.
func ExtractDocumentIDNative(event map[string]interface{}) (interface{}, error) {
	// [DEBUG v4] Log entry to function
	fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] ExtractDocumentIDNative called with event keys: %v\n",
		getMapKeysFromEvent(event))

	// Try MongoDB _id format first (preserves ObjectID type)
	if id, exists := event["_id"]; exists {
		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] Found '_id' at top level: %v (type: %T)\n", id, id)

		return id, nil
	}

	fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] No '_id' at top level\n")

	// Try PostgreSQL id/uuid format
	if id, exists := event["id"]; exists {
		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] Found 'id' at top level: %v (type: %T)\n",
			id, id)

		return id, nil
	}

	fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] No 'id' at top level\n")

	// [DEBUG v4] Check if RawEvent field exists and examine it
	if rawEvent, exists := event["RawEvent"]; exists {
		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] Found RawEvent field (type: %T)\n", rawEvent)

		if rawMap, ok := rawEvent.(map[string]interface{}); ok {
			fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] RawEvent is a map with keys: %v\n",
				getMapKeysFromEvent(rawMap))

			// Try to find ID in RawEvent
			if id := extractIDFromDocument(rawEvent); id != nil {
				fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] SUCCESS - found ID in RawEvent: %v (type: %T)\n",
					id, id)

				return id, nil
			}

			fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] No ID found in RawEvent map\n")
		} else {
			fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] RawEvent is not a map\n")
		}
	}

	// Try in fullDocument for change stream events
	if fullDoc, exists := event["fullDocument"]; exists {
		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] Found fullDocument, checking for ID inside...\n")

		if id := extractIDFromDocument(fullDoc); id != nil {
			fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] SUCCESS - extracted ID from fullDocument: %v (type: %T)\n",
				id, id)

			return id, nil
		}

		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] FAILED - fullDocument exists but no ID found inside\n")
	} else {
		fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] No fullDocument in event\n")
	}

	fmt.Printf("[EXTRACT-NATIVE-DEBUG-v4] FAILED - no document ID found anywhere in event\n")

	return nil, datastore.NewValidationError(
		"",
		"no document ID found in event",
		nil,
	).WithMetadata("event", event)
}

// getMapKeysFromEvent returns the keys of a map as a slice (helper for debugging)
func getMapKeysFromEvent(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
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

// NormalizeFieldNamesForMongoDB converts protobuf-generated JSON structures to use lowercase field names.
// This is necessary because:
// 1. Protobuf JSON marshaling uses camelCase (e.g., entityType, entityValue)
// 2. MongoDB stores and queries fields based on JSON tag names (camelCase)
// 3. MongoDB aggregation pipeline filters expect lowercase field references (e.g., $$this.entitytype)
// 4. When embedding resolved values in pipelines, field names must match MongoDB's lowercase convention
//
// This function ensures that protobuf values embedded in MongoDB aggregation pipelines
// have lowercase field names matching the pipeline filter expectations.
//
// Example:
//
//	Input:  []*protos.Entity marshals to [{"entityType": "GPU", "entityValue": "0"}]
//	Output: [{"entitytype": "GPU", "entityvalue": "0"}]
//
// Usage: Call this on any protobuf values before embedding them in MongoDB aggregation pipelines.
func NormalizeFieldNamesForMongoDB(value interface{}) interface{} {
	// First, marshal to JSON (converts protobuf to JSON with camelCase)
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		// If marshaling fails, return original value
		return value
	}

	// Unmarshal to map[string]interface{} or []interface{}
	var intermediate interface{}
	if err := json.Unmarshal(jsonBytes, &intermediate); err != nil {
		// If unmarshaling fails, return original value
		return value
	}

	// Recursively lowercase all field names
	return lowercaseKeys(intermediate)
}

// lowercaseKeys recursively converts all map keys to lowercase
func lowercaseKeys(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			// Convert key to lowercase and recursively process value
			result[strings.ToLower(key)] = lowercaseKeys(val)
		}

		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = lowercaseKeys(val)
		}

		return result
	default:
		// Primitive values (string, number, bool, null) remain unchanged
		return v
	}
}

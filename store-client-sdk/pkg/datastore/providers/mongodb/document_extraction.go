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
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/common"
)

// ExtractMongoDocumentID extracts MongoDB document ID from change stream event
// Returns a K8s-compliant ID using the generic common utilities
func ExtractMongoDocumentID(rawEvent map[string]interface{}) string {
	var rawID string

	// Try direct _id extraction
	if id, exists := rawEvent["_id"]; exists {
		rawID = fmt.Sprintf("%v", id)
	}

	// Fallback: try documentKey._id (MongoDB change stream format)
	if rawID == "" {
		rawID = ExtractFromDocumentKey(rawEvent)
	}

	// Fallback: try fullDocument._id
	if rawID == "" {
		rawID = ExtractFromFullDocument(rawEvent)
	}

	// Use generic utility to create K8s-compliant ID
	if rawID != "" {
		return common.GenerateKubernetesCompliantID(rawID)
	}

	// Last resort: timestamp-based ID
	return common.CreateHashedID(fmt.Sprintf("mongo-%d", time.Now().UnixNano()), 8)
}

// ExtractFromDocumentKey extracts ID from MongoDB change stream documentKey
func ExtractFromDocumentKey(rawEvent map[string]interface{}) string {
	docKey, exists := rawEvent["documentKey"]
	if !exists {
		return ""
	}

	// Handle both map[string]interface{} and primitive.M (BSON map)
	// The shallow copy in changestream.go preserves nested BSON types
	var docKeyMap map[string]interface{}

	switch dk := docKey.(type) {
	case map[string]interface{}:
		docKeyMap = dk
	case primitive.M:
		// Convert primitive.M to map[string]interface{}
		docKeyMap = make(map[string]interface{})
		for k, v := range dk {
			docKeyMap[k] = v
		}
	default:
		return ""
	}

	id, exists := docKeyMap["_id"]
	if !exists {
		return ""
	}

	return FormatObjectIDForLogging(id)
}

// ExtractFromFullDocument extracts ID from MongoDB fullDocument
func ExtractFromFullDocument(rawEvent map[string]interface{}) string {
	fullDoc, exists := rawEvent["fullDocument"]
	if !exists {
		return ""
	}

	// Handle both map[string]interface{} and primitive.M (BSON map)
	// The shallow copy in changestream.go preserves nested BSON types
	var fullDocMap map[string]interface{}

	switch fd := fullDoc.(type) {
	case map[string]interface{}:
		fullDocMap = fd
	case primitive.M:
		// Convert primitive.M to map[string]interface{}
		fullDocMap = make(map[string]interface{})
		for k, v := range fd {
			fullDocMap[k] = v
		}
	default:
		return ""
	}

	id, exists := fullDocMap["_id"]
	if !exists {
		return ""
	}

	return FormatObjectIDForLogging(id)
}

// ParseObjectIDString converts string representation to MongoDB ObjectID
func ParseObjectIDString(idStr string) (primitive.ObjectID, error) {
	return primitive.ObjectIDFromHex(idStr)
}

// FormatObjectIDForLogging safely formats any ObjectID type for logging
func FormatObjectIDForLogging(objID interface{}) string {
	switch id := objID.(type) {
	case primitive.ObjectID:
		return id.Hex()
	case string:
		return id
	default:
		return fmt.Sprintf("%v", id)
	}
}

// ExtractRawObjectID extracts the raw MongoDB ObjectID without K8s formatting
// Useful when you need the actual ObjectID for database operations
func ExtractRawObjectID(rawEvent map[string]interface{}) string {
	// For MongoDB change streams, the top-level _id is the resume token (not the document ID)
	// We need to check documentKey._id first for change stream events
	// Priority 1: try documentKey._id (most common for change streams)
	if rawID := ExtractFromDocumentKey(rawEvent); rawID != "" {
		return rawID
	}

	// Priority 2: try fullDocument._id
	if rawID := ExtractFromFullDocument(rawEvent); rawID != "" {
		return rawID
	}

	// Priority 3: try direct _id extraction (for non-change-stream events)
	// Only use this if the _id is a valid ObjectID (string or primitive.ObjectID)
	if id, exists := rawEvent["_id"]; exists {
		// Skip if it's a map (resume token) or other type
		switch id.(type) {
		case primitive.ObjectID, string:
			return FormatObjectIDForLogging(id)
		}
	}

	return ""
}

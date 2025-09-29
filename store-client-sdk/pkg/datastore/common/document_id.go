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

package common

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// GenerateKubernetesCompliantID creates a K8s-compliant resource name from any input
// Works with any datastore - MongoDB ObjectID, PostgreSQL UUID, etc.
func GenerateKubernetesCompliantID(rawID string) string {
	return CreateHashedID(rawID, 8)
}

// CreateHashedID generates a hashed ID of specified length
// Generic utility - not tied to any specific datastore
func CreateHashedID(input string, length int) string {
	if input == "" {
		// Fallback for empty input
		input = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	hash := sha256.Sum256([]byte(input))
	hexHash := fmt.Sprintf("%x", hash)

	if length > len(hexHash) {
		length = len(hexHash)
	}

	return hexHash[:length]
}

// GenerateTimestampBasedID creates a unique ID using current timestamp
// Useful as a fallback when no document ID is available
func GenerateTimestampBasedID() string {
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	return CreateHashedID(timestamp, 8)
}

// ExtractDocumentIDFromRawEvent extracts a document ID from raw event and makes it K8s-compliant
// This uses the provider-specific DocumentIDExtractor if available, otherwise uses fallback
func ExtractDocumentIDFromRawEvent(rawEvent map[string]interface{}, dataStore interface{}) string {
	// Try provider-specific extraction first (if available)
	if extractor, ok := dataStore.(interface {
		ExtractDocumentID(map[string]interface{}) string
	}); ok {
		rawID := extractor.ExtractDocumentID(rawEvent)
		if rawID != "" {
			return GenerateKubernetesCompliantID(rawID)
		}
	}

	// Fallback: timestamp-based ID
	return GenerateTimestampBasedID()
}

// ExtractDocumentIDForUpdate extracts the raw document ID for database update operations
// This preserves the original format needed by the datastore (e.g., MongoDB ObjectID)
// Uses provider-specific extraction through the DocumentIDExtractor interface
func ExtractDocumentIDForUpdate(rawEvent map[string]interface{}, dataStore interface{}) string {
	// Try provider-specific extraction (if available)
	if extractor, ok := dataStore.(interface {
		ExtractDocumentID(map[string]interface{}) string
	}); ok {
		return extractor.ExtractDocumentID(rawEvent)
	}

	// No provider-specific extraction available
	return ""
}

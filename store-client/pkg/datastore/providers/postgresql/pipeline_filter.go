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
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

// PipelineFilter filters events based on MongoDB-style aggregation pipeline
// This allows PostgreSQL to emulate MongoDB's pipeline filtering at the application level
type PipelineFilter struct {
	stages []filterStage
}

// filterStage represents a single stage in the pipeline (currently only $match is supported)
type filterStage struct {
	matchConditions map[string]interface{}
}

// NewPipelineFilter creates a new pipeline filter from a MongoDB-style pipeline
func NewPipelineFilter(pipeline interface{}) (*PipelineFilter, error) {
	if pipeline == nil {
		return nil, nil
	}

	filter := &PipelineFilter{
		stages: make([]filterStage, 0),
	}

	// Handle different pipeline types
	var stageList []interface{}

	switch p := pipeline.(type) {
	case []interface{}:
		// Native Go slice
		stageList = p
	case datastore.Pipeline:
		// datastore.Pipeline is []Document, convert to []interface{}
		stageList = make([]interface{}, len(p))
		for i, doc := range p {
			stageList[i] = doc
		}
	default:
		return nil, fmt.Errorf("unsupported pipeline type: %T", pipeline)
	}

	// Process each stage
	for _, stage := range stageList {
		if err := filter.parseStage(stage); err != nil {
			slog.Warn("Failed to parse pipeline stage, skipping",
				"error", err,
				"stage", fmt.Sprintf("%T", stage))

			continue
		}
	}

	if len(filter.stages) == 0 {
		return nil, nil // No valid stages, return nil filter
	}

	return filter, nil
}

// parseStage parses a single pipeline stage
func (f *PipelineFilter) parseStage(stage interface{}) error {
	// Convert stage to map
	stageMap, ok := stage.(map[string]interface{})
	if !ok {
		// Try datastore.Document type
		if stageD, ok := stage.(datastore.Document); ok {
			stageMap = make(map[string]interface{})
			for _, elem := range stageD {
				stageMap[elem.Key] = elem.Value
			}
		} else {
			return fmt.Errorf("stage is not a map or datastore.Document: %T", stage)
		}
	}

	// Currently only support $match stages
	if matchValue, ok := stageMap["$match"]; ok {
		conditions, err := f.parseMatchConditions(matchValue)
		if err != nil {
			return fmt.Errorf("failed to parse $match conditions: %w", err)
		}

		f.stages = append(f.stages, filterStage{
			matchConditions: conditions,
		})
	}

	return nil
}

// parseMatchConditions parses $match conditions from the pipeline
func (f *PipelineFilter) parseMatchConditions(matchValue interface{}) (map[string]interface{}, error) {
	// Convert to map
	matchMap, ok := matchValue.(map[string]interface{})
	if !ok {
		// Try datastore.Document type
		if matchD, ok := matchValue.(datastore.Document); ok {
			matchMap = make(map[string]interface{})
			for _, elem := range matchD {
				matchMap[elem.Key] = elem.Value
			}
		} else {
			return nil, fmt.Errorf("match value is not a map or datastore.Document: %T", matchValue)
		}
	}

	return matchMap, nil
}

// MatchesEvent checks if an event matches the pipeline filter
func (f *PipelineFilter) MatchesEvent(event datastore.EventWithToken) bool {
	if f == nil || len(f.stages) == 0 {
		return true // No filter, all events match
	}

	// All stages must match for the event to pass
	for i, stage := range f.stages {
		matches := f.matchesStage(event.Event, stage.matchConditions)

		// Extract node name from event for better debugging
		nodeName := "unknown"

		if fullDoc, ok := event.Event["fullDocument"].(map[string]interface{}); ok {
			if healthevent, ok := fullDoc["healthevent"].(map[string]interface{}); ok {
				// Try lowercase first (MongoDB compatibility)
				if name, ok := healthevent["nodename"].(string); ok {
					nodeName = name
				} else if name, ok := healthevent["nodeName"].(string); ok {
					// Try camelCase (PostgreSQL JSON)
					nodeName = name
				}
			}
		}

		slog.Info("[PIPELINE-FILTER-DEBUG] Evaluating filter stage",
			"stageIndex", i,
			"matches", matches,
			"conditions", stage.matchConditions,
			"operationType", event.Event["operationType"],
			"nodeName", nodeName,
			"token", string(event.ResumeToken))

		if !matches {
			slog.Warn("[PIPELINE-FILTER-DEBUG] Event filtered out - stage did not match",
				"stageIndex", i,
				"conditions", stage.matchConditions,
				"operationType", event.Event["operationType"],
				"nodeName", nodeName,
				"token", string(event.ResumeToken))

			return false
		}
	}

	slog.Info("[PIPELINE-FILTER-DEBUG] Event passed all filter stages",
		"totalStages", len(f.stages),
		"token", string(event.ResumeToken))

	return true
}

// matchesStage checks if an event matches a single filter stage
func (f *PipelineFilter) matchesStage(event map[string]interface{}, conditions map[string]interface{}) bool {
	for key, value := range conditions {
		if !f.matchesCondition(event, key, value) {
			return false
		}
	}

	return true
}

// matchesCondition checks if an event matches a specific condition
func (f *PipelineFilter) matchesCondition(event map[string]interface{}, key string, expectedValue interface{}) bool {
	slog.Info("[PIPELINE-MATCH-DEBUG] Checking condition",
		"key", key,
		"expectedValueType", fmt.Sprintf("%T", expectedValue))

	// Handle MongoDB operators
	switch key {
	case "$or":
		result := f.matchesOr(event, expectedValue)
		slog.Info("[PIPELINE-MATCH-DEBUG] $or result", "matches", result)

		return result
	case "$and":
		result := f.matchesAnd(event, expectedValue)
		slog.Info("[PIPELINE-MATCH-DEBUG] $and result", "matches", result)

		return result
	default:
		// Handle field path matching (e.g., "operationType", "fullDocument.healthevent.isfatal")
		actualValue := f.getFieldValue(event, key)

		actualJSON, _ := json.Marshal(actualValue)
		expectedJSON, _ := json.Marshal(expectedValue)

		slog.Info("[PIPELINE-MATCH-DEBUG] Field value comparison",
			"key", key,
			"actualValue", string(actualJSON),
			"actualType", fmt.Sprintf("%T", actualValue),
			"expectedValue", string(expectedJSON),
			"expectedType", fmt.Sprintf("%T", expectedValue))

		result := f.matchesValue(actualValue, expectedValue)
		slog.Info("[PIPELINE-MATCH-DEBUG] Field match result",
			"key", key,
			"matches", result)

		return result
	}
}

// matchesOr handles $or conditions
func (f *PipelineFilter) matchesOr(event map[string]interface{}, orConditions interface{}) bool {
	// Convert to array
	conditionsArray, ok := orConditions.([]interface{})
	if !ok {
		// Try datastore.Array type
		if conditionsA, ok := orConditions.(datastore.Array); ok {
			conditionsArray = []interface{}(conditionsA)
		} else {
			slog.Warn("$or conditions not an array", "type", fmt.Sprintf("%T", orConditions))
			return false
		}
	}

	// At least one condition must match
	for _, condition := range conditionsArray {
		condMap, ok := condition.(map[string]interface{})
		if !ok {
			// Try datastore.Document type
			if condD, ok := condition.(datastore.Document); ok {
				condMap = make(map[string]interface{})
				for _, elem := range condD {
					condMap[elem.Key] = elem.Value
				}
			} else {
				continue
			}
		}

		if f.matchesStage(event, condMap) {
			return true
		}
	}

	return false
}

// matchesAnd handles $and conditions
func (f *PipelineFilter) matchesAnd(event map[string]interface{}, andConditions interface{}) bool {
	// Convert to array
	conditionsArray, ok := andConditions.([]interface{})
	if !ok {
		// Try datastore.Array type
		if conditionsA, ok := andConditions.(datastore.Array); ok {
			conditionsArray = []interface{}(conditionsA)
		} else {
			slog.Warn("$and conditions not an array", "type", fmt.Sprintf("%T", andConditions))
			return false
		}
	}

	// All conditions must match
	for _, condition := range conditionsArray {
		condMap, ok := condition.(map[string]interface{})
		if !ok {
			// Try datastore.Document type
			if condD, ok := condition.(datastore.Document); ok {
				condMap = make(map[string]interface{})
				for _, elem := range condD {
					condMap[elem.Key] = elem.Value
				}
			} else {
				return false
			}
		}

		if !f.matchesStage(event, condMap) {
			return false
		}
	}

	return true
}

// matchesValue checks if an actual value matches an expected value (with operator support)
//
//nolint:cyclop // Value matching requires type-specific comparisons
func (f *PipelineFilter) matchesValue(actualValue interface{}, expectedValue interface{}) bool {
	slog.Info("[PIPELINE-VALUE-DEBUG] Comparing values",
		"actualType", fmt.Sprintf("%T", actualValue),
		"expectedType", fmt.Sprintf("%T", expectedValue))

	// Handle MongoDB operators and nested field matching in expectedValue
	if expectedMap, ok := expectedValue.(map[string]interface{}); ok {
		slog.Info("[PIPELINE-VALUE-DEBUG] expectedValue is map[string]interface{}")
		return f.matchesMapValue(actualValue, expectedMap)
	}

	// Try datastore.Document type
	if expectedD, ok := expectedValue.(datastore.Document); ok {
		slog.Info("[PIPELINE-VALUE-DEBUG] expectedValue is datastore.Document, converting",
			"docLength", len(expectedD))

		expectedMap := make(map[string]interface{})

		for _, elem := range expectedD {
			slog.Info("[PIPELINE-VALUE-DEBUG] Document element",
				"key", elem.Key,
				"valueType", fmt.Sprintf("%T", elem.Value))
			expectedMap[elem.Key] = elem.Value
		}

		slog.Info("[PIPELINE-VALUE-DEBUG] Converted datastore.Document to map",
			"mapKeys", getMapKeysFromInterface(expectedMap))

		return f.matchesValue(actualValue, expectedMap)
	}

	// Direct value comparison
	slog.Info("[PIPELINE-VALUE-DEBUG] Using direct comparison")

	return f.matchesEqual(actualValue, expectedValue)
}

// matchesMapValue handles matching when expectedValue is a map
// (either operators or nested field matching)
func (f *PipelineFilter) matchesMapValue(actualValue interface{}, expectedMap map[string]interface{}) bool {
	slog.Info("[PIPELINE-MAP-DEBUG] Entered matchesMapValue",
		"actualValueType", fmt.Sprintf("%T", actualValue),
		"expectedMapType", fmt.Sprintf("%T", expectedMap),
		"expectedMapKeys", getMapKeysFromInterface(expectedMap))

	// Check if this is an operator map (all keys start with $) or a nested field match
	hasOperators := false
	hasNonOperators := false

	for key := range expectedMap {
		if strings.HasPrefix(key, "$") {
			hasOperators = true
		} else {
			hasNonOperators = true
		}
	}

	slog.Info("[PIPELINE-MAP-DEBUG] Key analysis",
		"hasOperators", hasOperators,
		"hasNonOperators", hasNonOperators)

	// If we have operators, process them
	if hasOperators {
		slog.Info("[PIPELINE-MAP-DEBUG] Processing operators")
		return f.matchesOperators(actualValue, expectedMap)
	}

	// If we have non-operators, this is a nested field match
	// e.g., {"healtheventstatus.nodequarantined": "Quarantined"}
	if hasNonOperators {
		slog.Info("[PIPELINE-MAP-DEBUG] Processing nested fields")
		return f.matchesNestedFields(actualValue, expectedMap)
	}

	slog.Info("[PIPELINE-MAP-DEBUG] No operators or non-operators, returning true")

	return true
}

// matchesOperators processes MongoDB operator expressions
func (f *PipelineFilter) matchesOperators(actualValue interface{}, operators map[string]interface{}) bool {
	//nolint:goconst // MongoDB operator strings are clear as literals
	for op, opValue := range operators {
		switch op {
		case "$in":
			return f.matchesIn(actualValue, opValue)
		case "$ne":
			return !f.matchesEqual(actualValue, opValue)
		case "$eq":
			return f.matchesEqual(actualValue, opValue)
		case "$gt":
			return f.matchesGreaterThan(actualValue, opValue)
		case "$gte":
			return f.matchesGreaterThanOrEqual(actualValue, opValue)
		case "$lt":
			return f.matchesLessThan(actualValue, opValue)
		case "$lte":
			return f.matchesLessThanOrEqual(actualValue, opValue)
		default:
			slog.Warn("Unsupported operator", "operator", op)
			return false
		}
	}

	return true
}

// matchesNestedFields checks if actualValue (as a map) contains expected fields
func (f *PipelineFilter) matchesNestedFields(actualValue interface{}, expectedFields map[string]interface{}) bool {
	actualMap, ok := actualValue.(map[string]interface{})
	if !ok {
		slog.Warn("[PIPELINE-NESTED-DEBUG] actualValue is not a map",
			"actualType", fmt.Sprintf("%T", actualValue))

		return false
	}

	slog.Info("[PIPELINE-NESTED-DEBUG] Matching nested fields",
		"actualMapKeys", getMapKeysFromInterface(actualMap),
		"expectedFieldsKeys", getMapKeysFromInterface(expectedFields))

	// All expected fields must match
	for fieldPath, expectedFieldValue := range expectedFields {
		// Try direct key match first (for flat maps with dot-notation keys)
		// This is important for PostgreSQL updatedFields which uses flat keys like
		// "healtheventstatus.nodequarantined" instead of nested maps
		var actualFieldValue interface{}

		if directValue, exists := actualMap[fieldPath]; exists {
			slog.Info("[PIPELINE-NESTED-DEBUG] Found direct key match",
				"fieldPath", fieldPath,
				"value", directValue)
			actualFieldValue = directValue
		} else {
			// Fall back to dot-notation path navigation (for nested maps)
			slog.Info("[PIPELINE-NESTED-DEBUG] No direct key, trying path navigation",
				"fieldPath", fieldPath)
			actualFieldValue = f.getFieldValue(actualMap, fieldPath)
			slog.Info("[PIPELINE-NESTED-DEBUG] Path navigation result",
				"fieldPath", fieldPath,
				"value", actualFieldValue)
		}

		match := f.matchesValue(actualFieldValue, expectedFieldValue)
		slog.Info("[PIPELINE-NESTED-DEBUG] Field match result",
			"fieldPath", fieldPath,
			"matches", match)

		if !match {
			return false
		}
	}

	return true
}

// Helper function to get map keys from interface{}
func getMapKeysFromInterface(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

// matchesIn checks if value is in array
func (f *PipelineFilter) matchesIn(actualValue interface{}, inArray interface{}) bool {
	// Convert to array
	array, ok := inArray.([]interface{})
	if !ok {
		// Try datastore.Array type
		if arrayA, ok := inArray.(datastore.Array); ok {
			array = []interface{}(arrayA)
		} else {
			slog.Warn("$in value not an array", "type", fmt.Sprintf("%T", inArray))
			return false
		}
	}

	for _, item := range array {
		if f.matchesEqual(actualValue, item) {
			return true
		}
	}

	return false
}

// matchesEqual checks if two values are equal
func (f *PipelineFilter) matchesEqual(actual, expected interface{}) bool {
	// Handle type conversions
	actualStr, actualIsStr := actual.(string)
	expectedStr, expectedIsStr := expected.(string)

	if actualIsStr && expectedIsStr {
		return actualStr == expectedStr
	}

	// Handle boolean
	actualBool, actualIsBool := actual.(bool)
	expectedBool, expectedIsBool := expected.(bool)

	if actualIsBool && expectedIsBool {
		return actualBool == expectedBool
	}

	// CRITICAL FIX: Handle missing boolean fields
	// When a boolean field is missing (nil) and we're comparing to false,
	// treat the missing field as false (protobuf/JSON default value).
	// This matches the behavior where absent boolean fields are implicitly false.
	if actual == nil && expectedIsBool && !expectedBool {
		slog.Info("[PIPELINE-MATCH-DEBUG] Treating missing boolean field as false")
		return true
	}

	// Handle numbers (int, float64, etc.)
	actualNum, actualIsNum := toFloat64(actual)
	expectedNum, expectedIsNum := toFloat64(expected)

	if actualIsNum && expectedIsNum {
		return actualNum == expectedNum
	}

	// Direct comparison
	return actual == expected
}

// matchesGreaterThan checks if actual > expected
func (f *PipelineFilter) matchesGreaterThan(actual, expected interface{}) bool {
	actualNum, actualIsNum := toFloat64(actual)
	expectedNum, expectedIsNum := toFloat64(expected)

	if actualIsNum && expectedIsNum {
		return actualNum > expectedNum
	}

	return false
}

// matchesGreaterThanOrEqual checks if actual >= expected
func (f *PipelineFilter) matchesGreaterThanOrEqual(actual, expected interface{}) bool {
	actualNum, actualIsNum := toFloat64(actual)
	expectedNum, expectedIsNum := toFloat64(expected)

	if actualIsNum && expectedIsNum {
		return actualNum >= expectedNum
	}

	return false
}

// matchesLessThan checks if actual < expected
func (f *PipelineFilter) matchesLessThan(actual, expected interface{}) bool {
	actualNum, actualIsNum := toFloat64(actual)
	expectedNum, expectedIsNum := toFloat64(expected)

	if actualIsNum && expectedIsNum {
		return actualNum < expectedNum
	}

	return false
}

// matchesLessThanOrEqual checks if actual <= expected
func (f *PipelineFilter) matchesLessThanOrEqual(actual, expected interface{}) bool {
	actualNum, actualIsNum := toFloat64(actual)
	expectedNum, expectedIsNum := toFloat64(expected)

	if actualIsNum && expectedIsNum {
		return actualNum <= expectedNum
	}

	return false
}

// getFieldValue extracts a field value from an event using a dot-separated path
// e.g., "operationType" or "fullDocument.healthevent.isfatal"
// Performs case-insensitive key matching to handle MongoDB (lowercase) vs PostgreSQL (camelCase) differences
func (f *PipelineFilter) getFieldValue(event map[string]interface{}, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	current := interface{}(event)

	for _, part := range parts {
		if currentMap, ok := current.(map[string]interface{}); ok {
			// Try exact match first (fast path)
			if value, exists := currentMap[part]; exists {
				current = value
				continue
			}

			// Fall back to case-insensitive match for MongoDB/PostgreSQL compatibility
			// MongoDB uses lowercase bson tags (ishealthy, isfatal)
			// PostgreSQL uses camelCase json tags (isHealthy, isFatal)
			found := false
			lowerPart := strings.ToLower(part)

			for key, value := range currentMap {
				if strings.ToLower(key) == lowerPart {
					current = value
					found = true

					break
				}
			}

			if !found {
				return nil // Path doesn't exist
			}
		} else {
			return nil // Path doesn't exist
		}
	}

	return current
}

// toFloat64 converts various numeric types to float64
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

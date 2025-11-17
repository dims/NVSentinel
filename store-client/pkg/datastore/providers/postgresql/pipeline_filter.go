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
				if name, ok := healthevent["nodename"].(string); ok {
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
	// Handle MongoDB operators
	switch key {
	case "$or":
		return f.matchesOr(event, expectedValue)
	case "$and":
		return f.matchesAnd(event, expectedValue)
	default:
		// Handle field path matching (e.g., "operationType", "fullDocument.healthevent.isfatal")
		actualValue := f.getFieldValue(event, key)
		return f.matchesValue(actualValue, expectedValue)
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
	// Handle MongoDB operators and nested field matching in expectedValue
	if expectedMap, ok := expectedValue.(map[string]interface{}); ok {
		return f.matchesMapValue(actualValue, expectedMap)
	}

	// Try datastore.Document type
	if expectedD, ok := expectedValue.(datastore.Document); ok {
		expectedMap := make(map[string]interface{})
		for _, elem := range expectedD {
			expectedMap[elem.Key] = elem.Value
		}

		return f.matchesValue(actualValue, expectedMap)
	}

	// Direct value comparison
	return f.matchesEqual(actualValue, expectedValue)
}

// matchesMapValue handles matching when expectedValue is a map
// (either operators or nested field matching)
func (f *PipelineFilter) matchesMapValue(actualValue interface{}, expectedMap map[string]interface{}) bool {
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

	// If we have operators, process them
	if hasOperators {
		return f.matchesOperators(actualValue, expectedMap)
	}

	// If we have non-operators, this is a nested field match
	// e.g., {"healtheventstatus.nodequarantined": "Quarantined"}
	if hasNonOperators {
		return f.matchesNestedFields(actualValue, expectedMap)
	}

	return true
}

// matchesOperators processes MongoDB operator expressions
func (f *PipelineFilter) matchesOperators(actualValue interface{}, operators map[string]interface{}) bool {
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
		return false
	}

	// All expected fields must match
	for fieldPath, expectedFieldValue := range expectedFields {
		actualFieldValue := f.getFieldValue(actualMap, fieldPath)
		if !f.matchesValue(actualFieldValue, expectedFieldValue) {
			return false
		}
	}

	return true
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
func (f *PipelineFilter) getFieldValue(event map[string]interface{}, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	current := interface{}(event)

	for _, part := range parts {
		if currentMap, ok := current.(map[string]interface{}); ok {
			current = currentMap[part]
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

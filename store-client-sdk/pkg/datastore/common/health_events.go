/*
Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.

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
	"encoding/json"
	"fmt"
	"time"

	platformconnectorprotos "github.com/nvidia/nvsentinel/platform-connectors/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
)

// Common error types for change stream processing
var (
	ErrInvalidChangeEvent     = fmt.Errorf("invalid change stream event")
	ErrMissingDocument        = fmt.Errorf("missing document in change event")
	ErrMissingFullDocument    = fmt.Errorf("missing fullDocument in change event")
	ErrNilDocument            = fmt.Errorf("document is nil - may be a delete operation")
	ErrInvalidDocumentType    = fmt.Errorf("document has invalid type")
	ErrMarshalFailed          = fmt.Errorf("failed to marshal document data")
	ErrUnmarshalFailed        = fmt.Errorf("failed to unmarshal health event")
	ErrInvalidHealthEventType = fmt.Errorf("invalid health event type")
)

// ExtractHealthEventFromRawEvent extracts a HealthEventWithStatus from a raw change stream event
// This handles the legacy map[string]interface{} format used by some modules
func ExtractHealthEventFromRawEvent(event map[string]interface{}) (*datastore.HealthEventWithStatus, error) {
	if event == nil {
		return nil, fmt.Errorf("%w: event is nil", ErrInvalidChangeEvent)
	}

	// Check for fullDocument (correct MongoDB change stream format)
	fullDocRaw, exists := event["fullDocument"]
	if !exists {
		return nil, fmt.Errorf("%w: no fullDocument field found", ErrMissingFullDocument)
	}

	// Handle different types that fullDocument might be
	var fullDoc map[string]interface{}
	switch v := fullDocRaw.(type) {
	case map[string]interface{}:
		fullDoc = v
	case nil:
		return nil, fmt.Errorf("%w: fullDocument is nil", ErrNilDocument)
	default:
		// Try to convert from other types (e.g., bson.M)
		fullDoc = make(map[string]interface{})

		jsonBytes, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			return nil, fmt.Errorf("%w: failed to marshal fullDocument for conversion: %w", ErrMarshalFailed, marshalErr)
		}

		if unmarshalErr := json.Unmarshal(jsonBytes, &fullDoc); unmarshalErr != nil {
			return nil, fmt.Errorf("%w: failed to convert fullDocument type %T: %w", ErrInvalidDocumentType, v, unmarshalErr)
		}
	}

	return extractHealthEventFromDocument(fullDoc)
}

// ExtractHealthEvent is a unified function that can handle both raw and structured events
// This provides a single entry point for all modules
func ExtractHealthEvent(event interface{}) (*datastore.HealthEventWithStatus, error) {
	switch e := event.(type) {
	case map[string]interface{}:
		return ExtractHealthEventFromRawEvent(e)
	default:
		return nil, fmt.Errorf("%w: unsupported event type %T", ErrInvalidChangeEvent, event)
	}
}

// extractHealthEventFromDocument handles the common document extraction logic
func extractHealthEventFromDocument(fullDoc map[string]interface{}) (*datastore.HealthEventWithStatus, error) {
	tempStruct, err := parseDocumentToTempStruct(fullDoc)
	if err != nil {
		return nil, err
	}

	// Create the result structure
	healthEventWithStatus := &datastore.HealthEventWithStatus{
		CreatedAt:         tempStruct.CreatedAt,
		HealthEventStatus: tempStruct.HealthEventStatus,
	}

	// Merge flat fields into HealthEventStatus if they're populated
	// These flat fields are set by UpdateHealthEventStatus at the root level
	if tempStruct.NodeQuarantinedFlat != nil {
		healthEventWithStatus.HealthEventStatus.NodeQuarantined = tempStruct.NodeQuarantinedFlat
	}

	if tempStruct.UserPodsEvictionStatusFlat != "" {
		healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status = tempStruct.UserPodsEvictionStatusFlat
	}

	if tempStruct.UserPodsEvictionMessageFlat != "" {
		healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Message = tempStruct.UserPodsEvictionMessageFlat
	}

	if tempStruct.FaultRemediatedFlat != nil {
		healthEventWithStatus.HealthEventStatus.FaultRemediated = tempStruct.FaultRemediatedFlat
	}

	if tempStruct.LastRemediationTimestampFlat != nil {
		healthEventWithStatus.HealthEventStatus.LastRemediationTimestamp = tempStruct.LastRemediationTimestampFlat
	}

	// Find the HealthEvent data in various possible locations
	healthEventMap := findHealthEventMap(tempStruct)

	// Create protobuf from the map if found
	protoHealthEvent, err := createProtobufHealthEvent(healthEventMap)
	if err != nil {
		return nil, err
	}

	healthEventWithStatus.HealthEvent = protoHealthEvent

	return healthEventWithStatus, nil
}

// tempStructForParsing defines the structure used for parsing MongoDB documents
type tempStructForParsing struct {
	CreatedAt         time.Time                   `json:"createdAt"`
	HealthEvent       map[string]interface{}      `json:"healthevent"`
	HealthEventStatus datastore.HealthEventStatus `json:"healtheventstatus"`
	Document          map[string]interface{}      `json:"document"`
	NestedHE          map[string]interface{}      `json:"document.healthevent"`
	// Flat fields at root level (set by UpdateHealthEventStatus)
	NodeQuarantinedFlat          *datastore.Status `json:"nodeQuarantined"`
	UserPodsEvictionStatusFlat   datastore.Status  `json:"userPodsEvictionStatus"`
	UserPodsEvictionMessageFlat  string            `json:"userPodsEvictionMessage"`
	FaultRemediatedFlat          *bool             `json:"faultRemediated"`
	LastRemediationTimestampFlat *time.Time        `json:"lastRemediationTimestamp"`
}

// parseDocumentToTempStruct converts MongoDB document to temporary structure
func parseDocumentToTempStruct(fullDoc map[string]interface{}) (*tempStructForParsing, error) {
	docJSON, err := json.Marshal(fullDoc)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMarshalFailed, err)
	}

	var tempStruct tempStructForParsing
	if err := json.Unmarshal(docJSON, &tempStruct); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal to temp struct: %w", ErrUnmarshalFailed, err)
	}

	return &tempStruct, nil
}

// findHealthEventMap locates health event data in various document locations
func findHealthEventMap(tempStruct *tempStructForParsing) map[string]interface{} {
	// Try direct health event field first
	if len(tempStruct.HealthEvent) > 0 {
		return tempStruct.HealthEvent
	}

	// Try nested document locations
	return findHealthEventInDocument(tempStruct.Document)
}

// findHealthEventInDocument searches for health event in nested document structure
func findHealthEventInDocument(document map[string]interface{}) map[string]interface{} {
	if document == nil {
		return nil
	}

	// Try "healthevent" field first
	if he, exists := document["healthevent"]; exists {
		if heMap, ok := he.(map[string]interface{}); ok {
			return heMap
		}
	}

	// Try "HealthEvent" field (capitalized)
	if he, exists := document["HealthEvent"]; exists {
		if heMap, ok := he.(map[string]interface{}); ok {
			return heMap
		}
	}

	return nil
}

// createProtobufHealthEvent creates protobuf from health event map
func createProtobufHealthEvent(healthEventMap map[string]interface{}) (*platformconnectorprotos.HealthEvent, error) {
	if len(healthEventMap) == 0 {
		return nil, nil
	}

	healthEventBytes, err := json.Marshal(healthEventMap)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to marshal health event map: %w", ErrMarshalFailed, err)
	}

	var protoHealthEvent platformconnectorprotos.HealthEvent
	if err := json.Unmarshal(healthEventBytes, &protoHealthEvent); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal to protobuf HealthEvent: %w", ErrUnmarshalFailed, err)
	}

	return &protoHealthEvent, nil
}

// Protobuf Type Assertion Utilities

// ExtractPlatformConnectorHealthEvent safely extracts a platformconnectorprotos.HealthEvent
func ExtractPlatformConnectorHealthEvent(
	hews *datastore.HealthEventWithStatus,
) (*platformconnectorprotos.HealthEvent, error) {
	if hews == nil {
		return nil, fmt.Errorf("HealthEventWithStatus is nil")
	}

	if hews.HealthEvent == nil {
		return nil, fmt.Errorf("HealthEvent field is nil")
	}

	healthEvent, ok := hews.HealthEvent.(*platformconnectorprotos.HealthEvent)
	if !ok {
		return nil, fmt.Errorf("%w: expected *platformconnectorprotos.HealthEvent, got %T",
			ErrInvalidHealthEventType, hews.HealthEvent)
	}

	return healthEvent, nil
}

// ExtractPlatformConnectorsHealthEvent safely extracts a platformconnectorprotos.HealthEvent
// NOTE: This is the same as ExtractPlatformConnectorHealthEvent, kept for backward compatibility
func ExtractPlatformConnectorsHealthEvent(
	hews *datastore.HealthEventWithStatus,
) (*platformconnectorprotos.HealthEvent, error) {
	return ExtractPlatformConnectorHealthEvent(hews)
}

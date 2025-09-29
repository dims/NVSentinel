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

package reconciler

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	platform_connectors "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"

	// Datastore abstraction
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/common"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/watcher"
	"k8s.io/klog"
)

const (
	maxRetries int           = 5
	delay      time.Duration = 10 * time.Second
)

type HealthEventsAnalyzerReconcilerConfig struct {
	HealthEventsAnalyzerRules *config.TomlConfig
	Publisher                 *publisher.PublisherConfig
	DataStore                 datastore.DataStore
	CollectionName            string
	Pipeline                  interface{} // Custom pipeline for change stream filtering (e.g., mongo.Pipeline)
}

type Reconciler struct {
	config HealthEventsAnalyzerReconcilerConfig
}

func NewReconciler(cfg HealthEventsAnalyzerReconcilerConfig) *Reconciler {
	return &Reconciler{config: cfg}
}

//nolint:cyclop // Matches main branch pattern
func (r *Reconciler) Start(ctx context.Context) {
	// Create change stream watcher directly (like main branch)
	watcherConfig := watcher.Config{
		ClientName: "health-events-analyzer",
		TableName:  r.config.CollectionName,
		Pipeline:   r.config.Pipeline, // Pass pipeline from config (watches for non-fatal unhealthy events)
	}

	changeStreamWatcher, err := watcher.CreateChangeStreamWatcher(ctx, r.config.DataStore, watcherConfig)
	if err != nil {
		klog.Fatalf("failed to create change stream watcher: %+v", err)
	}

	defer func() {
		if err := changeStreamWatcher.Close(ctx); err != nil {
			klog.Errorf("failed to close watcher: %+v", err)
		}
	}()

	changeStreamWatcher.Start(ctx)

	klog.Info("Listening for events on the channel...")

	// Simple direct loop (matches main branch pattern)
	for eventWithToken := range changeStreamWatcher.Events() {
		startTime := time.Now()

		// Extract health event from the event
		healthEventWithStatus, err := common.ExtractHealthEvent(eventWithToken.Event)
		if err != nil {
			klog.Errorf("Failed to extract health event from event: %+v", err)
			totalEventProcessingError.WithLabelValues("extract_health_event_error").Inc()

			if err := changeStreamWatcher.MarkProcessed(ctx, eventWithToken.ResumeToken); err != nil {
				klog.Errorf("Error marking event as processed: %+v", err)
			}

			continue
		}

		klog.V(2).Infof("Received event: %+v", healthEventWithStatus)

		// Extract the HealthEvent using common utility
		healthEvent, err := common.ExtractPlatformConnectorsHealthEvent(healthEventWithStatus)
		if err != nil {
			klog.Errorf("Failed to extract HealthEvent: %v", err)
			continue
		}

		var entityValue string
		if len(healthEvent.EntitiesImpacted) > 0 {
			entityValue = healthEvent.EntitiesImpacted[0].EntityValue
		} else {
			entityValue = "unknown"
		}

		totalEventsReceived.WithLabelValues(entityValue).Inc()

		var processErr error

		var publishedNewEvent bool

		for i := 1; i <= maxRetries; i++ {
			klog.V(2).Infof("Attempt %d, handling event", i)

			publishedNewEvent, processErr = r.handleEvent(ctx, healthEventWithStatus)
			if processErr == nil {
				totalEventsSuccessfullyProcessed.Inc()

				if publishedNewEvent {
					klog.Info("New fatal event published.")
					fatalEventsPublishedTotal.WithLabelValues(entityValue).Inc()
				} else {
					klog.Info("Fatal event is not published, rule set criteria didn't match.")
				}

				break
			}

			klog.Errorf("Error in handling the event: %+v", processErr)
			totalEventProcessingError.WithLabelValues("handle_event_error").Inc()
			time.Sleep(delay)
		}

		if processErr != nil {
			klog.Errorf("Max attempt reached, error in handling the event: %+v", processErr)
		}

		// Mark the event as processed using the token captured WITH this event
		if err := changeStreamWatcher.MarkProcessed(ctx, eventWithToken.ResumeToken); err != nil {
			klog.Errorf("Error marking event as processed: %+v", err)
		}

		duration := time.Since(startTime).Seconds()
		eventHandlingDuration.Observe(duration)
	}
}

func (r *Reconciler) handleEvent(ctx context.Context, event *datastore.HealthEventWithStatus) (bool, error) {
	for _, rule := range r.config.HealthEventsAnalyzerRules.Rules {
		// Check if current event matches any sequence criteria in the rule
		if matchesAnySequenceCriteria(rule, *event) && r.evaluateRule(ctx, rule, *event) {
			klog.Infof("Rule '%s' matched for event: %+v", rule.Name, event)

			actionVal, ok := platform_connectors.RecommenedAction_value[rule.RecommendedAction]
			if !ok {
				actionVal = int32(platform_connectors.RecommenedAction_NONE)

				klog.Warningf("Invalid recommended_action '%s' in rule '%s'; defaulting to NONE", rule.RecommendedAction, rule.Name)
			}

			// Type assert the HealthEvent for the Publisher
			publishHealthEvent, ok := event.HealthEvent.(*platform_connectors.HealthEvent)
			if !ok {
				return false, fmt.Errorf("expected *platform_connectors.HealthEvent for publishing, got %T", event.HealthEvent)
			}

			err := r.config.Publisher.Publish(ctx, publishHealthEvent, platform_connectors.RecommenedAction(actionVal))
			if err != nil {
				klog.Errorf("Error in publishing the new fatal event: %+v", err)
				publisher.FatalEventPublishingError.WithLabelValues("event_publishing_to_UDS_error").Inc()

				return false, err
			}

			return true, nil
		}

		klog.V(2).Infof("Rule '%s' didn't meet criteria", rule.Name)
	}

	klog.Infof("No rule matched for event: %+v", event)

	return false, nil
}

// matchesAnySequenceCriteria checks if the current event matches any sequence criteria in the rule
func matchesAnySequenceCriteria(
	rule config.HealthEventsAnalyzerRule,
	healthEventWithStatus datastore.HealthEventWithStatus,
) bool {
	// Extract the HealthEvent using common utility
	healthEvent, err := common.ExtractPlatformConnectorsHealthEvent(&healthEventWithStatus)
	if err != nil {
		klog.Errorf("Failed to extract HealthEvent: %v", err)
		return false
	}

	for _, seq := range rule.Sequence {
		if matchesSequenceCriteria(seq.Criteria, healthEvent) {
			return true
		}
	}

	return false
}

// matchesSequenceCriteria checks if the current event matches a specific sequence criteria
func matchesSequenceCriteria(criteria map[string]interface{}, event *platform_connectors.HealthEvent) bool {
	for key, value := range criteria {
		strValue, ok := value.(string)
		if ok && len(strValue) > 5 && strValue[:5] == "this." {
			continue
		}

		actualValue := getValueFromPath(key, event)
		if actualValue == nil || actualValue != value {
			return false
		}
	}

	return true
}

// getValueFromPath extracts a value from the event using a dot-notation path
//
//nolint:cyclop,gocognit // TODO: refactor to reduce complexity
func getValueFromPath(path string, event *platform_connectors.HealthEvent) interface{} {
	parts := strings.Split(path, ".")

	if len(parts) > 0 && parts[0] == "healthevent" {
		parts = parts[1:]
	}

	if len(parts) == 0 {
		return nil
	}

	rootField := strings.ToLower(parts[0])

	if len(parts) == 1 {
		val := reflect.ValueOf(event).Elem()

		// Find the field by name case-insensitive
		for i := 0; i < val.NumField(); i++ {
			field := val.Type().Field(i)
			if strings.EqualFold(field.Name, rootField) {
				return val.Field(i).Interface()
			}
		}
	}

	if strings.EqualFold(rootField, "errorcode") && len(parts) > 1 {
		if idx, err := strconv.Atoi(parts[1]); err == nil && idx < len(event.ErrorCode) {
			return event.ErrorCode[idx]
		}

		return nil
	}

	if strings.EqualFold(rootField, "entitiesimpacted") && len(parts) > 2 {
		if idx, err := strconv.Atoi(parts[1]); err == nil && idx < len(event.EntitiesImpacted) {
			entity := event.EntitiesImpacted[idx]
			subField := strings.ToLower(parts[2])

			entityVal := reflect.ValueOf(entity).Elem()
			for i := 0; i < entityVal.NumField(); i++ {
				field := entityVal.Type().Field(i)
				if strings.EqualFold(field.Name, subField) {
					return entityVal.Field(i).Interface()
				}
			}
		}

		return nil
	}

	// Handle metadata map
	if strings.EqualFold(rootField, "metadata") && len(parts) > 1 {
		metadataKey := parts[1]
		if value, exists := event.Metadata[metadataKey]; exists {
			return value
		}
	}

	generatedTimestamp := strings.EqualFold(rootField, "generatedtimestamp")
	if generatedTimestamp && len(parts) > 1 && event.GeneratedTimestamp != nil {
		subField := strings.ToLower(parts[1])
		timestampVal := reflect.ValueOf(event.GeneratedTimestamp).Elem()

		for i := 0; i < timestampVal.NumField(); i++ {
			field := timestampVal.Type().Field(i)
			if strings.EqualFold(field.Name, subField) {
				return timestampVal.Field(i).Interface()
			}
		}
	}

	return nil
}

func (r *Reconciler) evaluateRule(
	ctx context.Context,
	rule config.HealthEventsAnalyzerRule,
	healthEventWithStatus datastore.HealthEventWithStatus,
) bool {
	klog.V(2).Infof("Starting rule evaluation for rule: %s", rule.Name)

	timeWindow, err := time.ParseDuration(rule.TimeWindow)
	if err != nil {
		klog.Errorf("Failed to parse time window: %v", err)
		totalEventProcessingError.WithLabelValues("parse_time_window_error").Inc()

		return false
	}

	// Extract the HealthEvent using common utility (once, outside the loop)
	healthEvent, err := common.ExtractPlatformConnectorsHealthEvent(&healthEventWithStatus)
	if err != nil {
		klog.Errorf("Failed to extract HealthEvent: %v", err)
		return false
	}

	// For each sequence in the rule, check if we have enough matching events
	for i, seq := range rule.Sequence {
		klog.V(2).Infof("Evaluating sequence %d: %+v", i, seq)

		// Convert sequence criteria to a generic filter for the datastore
		filter, err := r.buildFilterFromSequenceCriteria(seq.Criteria, timeWindow, healthEvent)
		if err != nil {
			klog.Errorf("Failed to build filter from sequence criteria: %v", err)
			totalEventProcessingError.WithLabelValues("parse_criteria_error").Inc()

			return false
		}

		// Query the datastore using the generic interface
		events, err := r.config.DataStore.HealthEventStore().FindHealthEventsByFilter(ctx, filter)
		if err != nil {
			klog.Errorf("Failed to query health events: %v", err)
			totalEventProcessingError.WithLabelValues("execute_query_error").Inc()

			return false
		}

		// Check if we have enough events matching this sequence
		if len(events) < seq.ErrorCount {
			klog.V(2).Infof("Sequence %d failed: found %d events, required %d", i, len(events), seq.ErrorCount)
			return false
		}

		klog.V(2).Infof("Sequence %d passed: found %d events (>= %d required)", i, len(events), seq.ErrorCount)
	}

	klog.V(2).Infof("All sequences passed for rule: %s", rule.Name)

	return true
}

// buildFilterFromSequenceCriteria converts sequence criteria to a generic datastore filter
func (r *Reconciler) buildFilterFromSequenceCriteria(
	criteria map[string]interface{},
	timeWindow time.Duration,
	healthEvent *platform_connectors.HealthEvent,
) (map[string]interface{}, error) {
	filter := make(map[string]interface{})

	// Add time window filter
	cutoffTime := time.Now().UTC().Add(-timeWindow)
	filter["created_at"] = map[string]interface{}{
		"$gte": cutoffTime,
	}

	// Parse the criteria and convert to generic filter format
	for key, value := range criteria {
		if strValue, ok := value.(string); ok && len(strValue) > 5 && strValue[:5] == "this." {
			// Handle dynamic values from current event (e.g., "this.NodeName")
			fieldPath := strValue[5:] // Skip "this."
			actualValue := getValueFromPath(fieldPath, healthEvent)
			filter[key] = actualValue
		} else {
			// Static value
			filter[key] = value
		}
	}

	return filter, nil
}

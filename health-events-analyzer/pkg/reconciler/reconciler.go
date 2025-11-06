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
	"log/slog"
	"reflect"
	"strings"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	datamodels "github.com/nvidia/nvsentinel/data-models/pkg/model"
	protos "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/helper"
)

const (
	maxRetries int           = 5
	delay      time.Duration = 10 * time.Second
)

type HealthEventsAnalyzerReconcilerConfig struct {
	DataStoreConfig           *datastore.DataStoreConfig
	Pipeline                  interface{}
	HealthEventsAnalyzerRules *config.TomlConfig
	Publisher                 *publisher.PublisherConfig
}

type Reconciler struct {
	config         HealthEventsAnalyzerReconcilerConfig
	databaseClient client.DatabaseClient
	eventProcessor client.EventProcessor
}

func NewReconciler(cfg HealthEventsAnalyzerReconcilerConfig) *Reconciler {
	return &Reconciler{
		config: cfg,
	}
}

// Start begins the reconciliation process by listening to change stream events
// and processing them accordingly.
func (r *Reconciler) Start(ctx context.Context) error {
	// Use standardized datastore client initialization
	bundle, err := helper.NewDatastoreClientFromConfig(
		ctx, "health-events-analyzer", *r.config.DataStoreConfig, r.config.Pipeline,
	)
	if err != nil {
		return fmt.Errorf("failed to create datastore client bundle: %w", err)
	}
	defer bundle.Close(ctx)

	r.databaseClient = bundle.DatabaseClient

	// Create and configure the unified EventProcessor
	processorConfig := client.EventProcessorConfig{
		MaxRetries:    maxRetries,
		RetryDelay:    delay,
		EnableMetrics: true,
		MetricsLabels: map[string]string{"module": "health-events-analyzer"},
	}

	r.eventProcessor = client.NewEventProcessor(bundle.ChangeStreamWatcher, bundle.DatabaseClient, processorConfig)

	// Set the event handler for processing health events
	r.eventProcessor.SetEventHandler(client.EventHandlerFunc(r.processHealthEvent))

	slog.Info("Starting health events analyzer with unified event processor...")

	// Start the event processor
	return r.eventProcessor.Start(ctx)
}

// processHealthEvent handles individual health events and implements the EventHandler interface
func (r *Reconciler) processHealthEvent(ctx context.Context, event *datamodels.HealthEventWithStatus) error {
	startTime := time.Now()

	slog.Debug("Received event", "event", event)
	slog.Info("Health events analyzer processing event",
		"nodeName", event.HealthEvent.NodeName,
		"checkName", event.HealthEvent.CheckName,
		"isFatal", event.HealthEvent.IsFatal,
		"isHealthy", event.HealthEvent.IsHealthy,
		"errorCode", event.HealthEvent.ErrorCode,
		"createdAt", event.CreatedAt)

	// Track event reception metrics
	totalEventsReceived.WithLabelValues(event.HealthEvent.EntitiesImpacted[0].EntityValue).Inc()

	// Process the event using existing business logic
	publishedNewEvent, err := r.handleEvent(ctx, event)
	if err != nil {
		// Log error but let the EventProcessor handle retry logic
		totalEventProcessingError.WithLabelValues("handle_event_error").Inc()
		return fmt.Errorf("failed to handle event: %w", err)
	}

	// Track success metrics
	totalEventsSuccessfullyProcessed.Inc()

	if publishedNewEvent {
		slog.Info("New fatal event published.")
		fatalEventsPublishedTotal.WithLabelValues(event.HealthEvent.EntitiesImpacted[0].EntityValue).Inc()
	} else {
		slog.Info("Fatal event is not published, rule set criteria didn't match.")
	}

	// Track processing duration
	duration := time.Since(startTime).Seconds()
	eventHandlingDuration.Observe(duration)

	return nil
}

func (r *Reconciler) handleEvent(ctx context.Context, event *datamodels.HealthEventWithStatus) (bool, error) {
	var multiErr *multierror.Error

	publishedNewEvent := false

	for _, rule := range r.config.HealthEventsAnalyzerRules.Rules {
		published, err := r.processRule(ctx, rule, event)
		if err != nil {
			multiErr = multierror.Append(multiErr, err)
			continue
		}

		if published {
			publishedNewEvent = true
		}
	}

	if multiErr.ErrorOrNil() != nil {
		slog.Error("Error in handling the event", "error", multiErr)
		return publishedNewEvent, fmt.Errorf("error in handling the event: %w", multiErr)
	}

	return publishedNewEvent, nil
}

// processRule handles the processing of a single rule against an event
func (r *Reconciler) processRule(ctx context.Context,
	rule config.HealthEventsAnalyzerRule,
	event *datamodels.HealthEventWithStatus) (bool, error) {
	// Validate all sequences from DB docs
	matchedSequences, err := r.validateAllSequenceCriteria(ctx, rule, *event)
	if err != nil {
		slog.Error("Error in validating all sequence criteria", "error", err)
		return false, fmt.Errorf("error in validating all sequence criteria: %w", err)
	}

	if !matchedSequences {
		return false, nil
	}

	err = r.publishMatchedEvent(ctx, rule, event)
	if err != nil {
		slog.Error("Error in publishing the matched event", "error", err)
		return false, fmt.Errorf("error in publishing the matched event: %w", err)
	}

	return true, nil
}

// publishMatchedEvent publishes an event when a rule matches
func (r *Reconciler) publishMatchedEvent(ctx context.Context,
	rule config.HealthEventsAnalyzerRule,
	event *datamodels.HealthEventWithStatus) error {
	slog.Info("Rule matched for event", "rule_name", rule.Name, "event", event)
	ruleMatchedTotal.WithLabelValues(rule.Name, event.HealthEvent.NodeName).Inc()

	actionVal := r.getRecommendedActionValue(rule.RecommendedAction, rule.Name)

	err := r.config.Publisher.Publish(ctx, event.HealthEvent, protos.RecommendedAction(actionVal), rule.Name)
	if err != nil {
		slog.Error("Error in publishing the new fatal event", "error", err)
		return fmt.Errorf("error in publishing the new fatal event: %w", err)
	}

	slog.Info("New event successfully published for matching rule", "rule_name", rule.Name)

	return nil
}

// getRecommendedActionValue returns the action value, with fallback to RecommendedAction_CONTACT_SUPPORT if invalid
func (r *Reconciler) getRecommendedActionValue(recommendedAction, ruleName string) int32 {
	actionVal, ok := protos.RecommendedAction_value[recommendedAction]
	if !ok {
		defaultAction := int32(protos.RecommendedAction_CONTACT_SUPPORT)
		slog.Warn("Invalid recommended_action in rule; defaulting to CONTACT_SUPPORT",
			"recommended_action", recommendedAction,
			"rule_name", ruleName,
			"default_action", protos.RecommendedAction_name[defaultAction])

		return defaultAction
	}

	return actionVal
}

func (r *Reconciler) validateAllSequenceCriteria(ctx context.Context, rule config.HealthEventsAnalyzerRule,
	healthEventWithStatus datamodels.HealthEventWithStatus) (bool, error) {
	slog.Debug("Evaluating rule for event", "rule_name", rule.Name, "event", healthEventWithStatus)

	timeWindow, err := time.ParseDuration(rule.TimeWindow)
	if err != nil {
		slog.Error("Failed to parse time window", "error", err)
		totalEventProcessingError.WithLabelValues("parse_time_window_error").Inc()

		return false, fmt.Errorf("failed to parse time window: %w", err)
	}

	// Check each sequence condition individually
	for i, seq := range rule.Sequence {
		// Build the filter using database-agnostic filter builder
		timeThreshold := time.Now().UTC().Add(-timeWindow).Unix()
		builder := client.NewFilterBuilder()
		builder.Gte("healthevent.generatedtimestamp.seconds", timeThreshold)

		// Exclude events published by health-events-analyzer itself (matches main branch behavior)
		builder.Ne("healthevent.agent", "health-events-analyzer")

		// Add sequence-specific criteria
		for key, value := range seq.Criteria {
			resolvedValue := r.resolveCriteriaValue(value, healthEventWithStatus)
			builder.Eq(key, resolvedValue)
		}

		// Build the final filter
		filter := builder.Build()

		// Count documents matching this sequence
		count, err := r.databaseClient.CountDocuments(ctx, filter, nil)
		if err != nil {
			slog.Error("Failed to count documents for sequence", "sequence", i, "error", err)
			totalEventProcessingError.WithLabelValues("count_documents_error").Inc()

			return false, err
		}

		// Check if count meets the required threshold
		if count < int64(seq.ErrorCount) {
			return false, nil
		}
	}

	return true, nil
}

// resolveCriteriaValue resolves a criteria value, handling "this." references
func (r *Reconciler) resolveCriteriaValue(
	value interface{},
	healthEventWithStatus datamodels.HealthEventWithStatus,
) interface{} {
	strValue, ok := value.(string)
	if !ok {
		return value
	}

	if !r.isThisReference(strValue) {
		return value
	}

	fieldPath := strValue[5:] // Skip "this."

	resolvedValue, err := getValueFromPath(fieldPath, healthEventWithStatus)
	if err != nil {
		slog.Error("Failed to resolve field path", "path", fieldPath, "error", err)
		return value // fallback to original value
	}

	return resolvedValue
}

// isThisReference checks if a string value is a "this." reference
func (r *Reconciler) isThisReference(strValue string) bool {
	return len(strValue) > 5 && strValue[:5] == "this."
}

// getValueFromPath extracts a value from a struct using dot-separated field path
func getValueFromPath(path string, event datamodels.HealthEventWithStatus) (interface{}, error) {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	// Start with the full event
	value := reflect.ValueOf(event)

	// Navigate through the path
	for _, part := range parts {
		if value.Kind() == reflect.Ptr {
			value = value.Elem()
		}

		// Handle array/slice indices
		if isNumericIndex(part) {
			var err error

			value, err = handleArrayIndex(value, part, path)
			if err != nil {
				return nil, err
			}

			continue
		}

		var err error

		value, err = navigateStructField(value, part, path)
		if err != nil {
			return nil, err
		}
	}

	// Return the interface{} value
	if value.CanInterface() {
		return value.Interface(), nil
	}

	return nil, fmt.Errorf("cannot access value at path %s", path)
}

// handleArrayIndex handles array/slice index navigation
func handleArrayIndex(value reflect.Value, part, path string) (reflect.Value, error) {
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return reflect.Value{}, fmt.Errorf("cannot navigate path %s: %s is not a slice/array", path, part)
	}

	index := parseIndex(part)
	if index < 0 || index >= value.Len() {
		return reflect.Value{}, fmt.Errorf("index %s out of bounds in path %s", part, path)
	}

	return value.Index(index), nil
}

// navigateStructField navigates to a field within a struct
func navigateStructField(value reflect.Value, part, path string) (reflect.Value, error) {
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("cannot navigate path %s: %s is not a struct", path, part)
	}

	// Find field by name (case insensitive)
	structType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := structType.Field(i)
		if strings.EqualFold(field.Name, part) {
			return value.Field(i), nil
		}
	}

	return reflect.Value{}, fmt.Errorf("field %s not found in path %s", part, path)
}

// isNumericIndex checks if a string represents a numeric array index
func isNumericIndex(s string) bool {
	if len(s) == 0 {
		return false
	}

	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// parseIndex converts a string to an integer index
func parseIndex(s string) int {
	result := 0
	for _, r := range s {
		result = result*10 + int(r-'0')
	}

	return result
}

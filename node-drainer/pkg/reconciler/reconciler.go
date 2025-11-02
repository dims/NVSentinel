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
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/statemanager"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/config"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/evaluator"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/informers"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/metrics"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/queue"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/utils"

	"k8s.io/client-go/kubernetes"
)

type Reconciler struct {
	Config              config.ReconcilerConfig
	NodeEvictionContext sync.Map
	DryRun              bool
	queueManager        queue.EventQueueManager
	informers           *informers.Informers
	evaluator           evaluator.DrainEvaluator
	kubernetesClient    kubernetes.Interface
	databaseClient      queue.DataStore
}

func NewReconciler(cfg config.ReconcilerConfig,
	dryRunEnabled bool, kubeClient kubernetes.Interface, informersInstance *informers.Informers,
	databaseClient queue.DataStore) *Reconciler {
	queueManager := queue.NewEventQueueManager()
	drainEvaluator := evaluator.NewNodeDrainEvaluator(cfg.TomlConfig, informersInstance)

	reconciler := &Reconciler{
		Config:              cfg,
		NodeEvictionContext: sync.Map{},
		DryRun:              dryRunEnabled,
		queueManager:        queueManager,
		informers:           informersInstance,
		evaluator:           drainEvaluator,
		kubernetesClient:    kubeClient,
		databaseClient:      databaseClient,
	}

	queueManager.SetDataStoreEventProcessor(reconciler)

	return reconciler
}

func (r *Reconciler) GetQueueManager() queue.EventQueueManager {
	return r.queueManager
}

func (r *Reconciler) Shutdown() {
	r.queueManager.Shutdown()
}

// PreprocessAndEnqueueEvent preprocesses an event from the change stream before enqueueing it.
// This function:
// 1. Extracts and unmarshals the health event
// 2. Skips events already in terminal status
// 3. Sets the initial status to InProgress (idempotent - only updates if not already set)
// 4. Enqueues the event to the processing queue
//
// This matches the behavior of the main branch's mongodb/event_watcher.go:preprocessAndEnqueueEvent
func (r *Reconciler) PreprocessAndEnqueueEvent(ctx context.Context, event client.Event) error {
	// Unmarshal the full document
	var document map[string]interface{}
	if err := event.UnmarshalDocument(&document); err != nil {
		return fmt.Errorf("failed to unmarshal event document: %w", err)
	}

	// Extract health event with status
	healthEventWithStatus := model.HealthEventWithStatus{}
	if err := unmarshalGenericEvent(document, &healthEventWithStatus); err != nil {
		return fmt.Errorf("failed to extract health event with status: %w", err)
	}

	// Skip if already in terminal state
	if isTerminalStatus(healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status) {
		slog.Debug("Skipping event - already in terminal state",
			"node", healthEventWithStatus.HealthEvent.NodeName,
			"status", healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status)

		return nil
	}

	nodeName := healthEventWithStatus.HealthEvent.NodeName

	// Extract the actual _id from the document (preserves its native type - ObjectID for MongoDB)
	documentID, err := utils.ExtractDocumentIDNative(document)
	if err != nil {
		return fmt.Errorf("failed to extract document ID: %w", err)
	}

	// Set initial status to StatusInProgress (idempotent - only updates if not already set)
	// Use the actual _id value (preserving its type) for the filter
	filter := map[string]interface{}{
		"_id": documentID,
		"healtheventstatus.userpodsevictionstatus.status": map[string]interface{}{
			"$ne": string(model.StatusInProgress),
		},
	}

	update := map[string]interface{}{
		"$set": map[string]interface{}{
			"healtheventstatus.userpodsevictionstatus.status": string(model.StatusInProgress),
		},
	}

	result, err := r.databaseClient.UpdateDocument(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update initial status: %w", err)
	}

	switch {
	case result.ModifiedCount > 0:
		slog.Info("Set initial eviction status to InProgress", "node", nodeName)
	case result.MatchedCount == 0:
		slog.Warn("No document matched for status update", "node", nodeName, "documentID", fmt.Sprintf("%v", documentID))
	default:
		slog.Debug("Status already set to InProgress", "node", nodeName, "documentID", fmt.Sprintf("%v", documentID))
	}

	// Enqueue to the queue manager
	return r.queueManager.EnqueueEventGeneric(ctx, nodeName, document, r.databaseClient)
}

// isTerminalStatus checks if a status is terminal (processing should not continue)
func isTerminalStatus(status model.Status) bool {
	return status == model.StatusSucceeded ||
		status == model.StatusFailed ||
		status == model.AlreadyDrained
}

// unmarshalGenericEvent converts a generic map to a specific struct using JSON marshaling
func unmarshalGenericEvent(event map[string]interface{}, target interface{}) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	if err := json.Unmarshal(jsonBytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to target type: %w", err)
	}

	return nil
}

func (r *Reconciler) ProcessEventGeneric(ctx context.Context,
	event datastore.Event, database queue.DataStore, nodeName string) error {
	start := time.Now()

	defer func() {
		metrics.EventHandlingDuration.Observe(time.Since(start).Seconds())
	}()

	healthEventWithStatus, err := r.parseHealthEventFromEvent(event, nodeName)
	if err != nil {
		return err
	}

	metrics.TotalEventsReceived.Inc()

	actionResult, err := r.evaluator.EvaluateEventWithDatabase(ctx, healthEventWithStatus, database)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("evaluate_event_error", nodeName).Inc()
		return fmt.Errorf("failed to evaluate event: %w", err)
	}

	slog.Info("Evaluated action for node",
		"node", nodeName,
		"action", actionResult.Action.String())

	return r.executeAction(ctx, actionResult, healthEventWithStatus, event, database)
}

func (r *Reconciler) executeAction(ctx context.Context, action *evaluator.DrainActionResult,
	healthEvent model.HealthEventWithStatus, event datastore.Event, database queue.DataStore) error {
	nodeName := healthEvent.HealthEvent.NodeName

	switch action.Action {
	case evaluator.ActionSkip:
		return r.executeSkip(ctx, nodeName, healthEvent, event, database)

	case evaluator.ActionWait:
		slog.Info("Waiting for node",
			"node", nodeName,
			"delay", action.WaitDelay)

		return fmt.Errorf("waiting for retry delay: %v", action.WaitDelay)

	case evaluator.ActionEvictImmediate:
		r.updateNodeDrainStatus(ctx, nodeName, &healthEvent, true)
		return r.executeImmediateEviction(ctx, action, healthEvent)

	case evaluator.ActionEvictWithTimeout:
		r.updateNodeDrainStatus(ctx, nodeName, &healthEvent, true)
		return r.executeTimeoutEviction(ctx, action, healthEvent)

	case evaluator.ActionCheckCompletion:
		slog.Debug("Executing ActionCheckCompletion", "node", nodeName)
		r.updateNodeDrainStatus(ctx, nodeName, &healthEvent, true)

		return r.executeCheckCompletion(ctx, action, healthEvent)

	case evaluator.ActionMarkAlreadyDrained:
		return r.executeMarkAlreadyDrained(ctx, healthEvent, event, database)

	case evaluator.ActionUpdateStatus:
		return r.executeUpdateStatus(ctx, healthEvent, event, database)

	default:
		return fmt.Errorf("unknown action: %s", action.Action.String())
	}
}

func (r *Reconciler) executeSkip(ctx context.Context,
	nodeName string, healthEvent model.HealthEventWithStatus,
	event datastore.Event, database queue.DataStore) error {
	slog.Info("Skipping event for node", "node", nodeName)

	// Update MongoDB status to StatusSucceeded for healthy events that cancel draining
	if healthEvent.HealthEventStatus.NodeQuarantined != nil &&
		*healthEvent.HealthEventStatus.NodeQuarantined == model.UnQuarantined {
		// Update MongoDB status to StatusSucceeded for healthy events that cancel draining
		podsEvictionStatus := &healthEvent.HealthEventStatus.UserPodsEvictionStatus
		podsEvictionStatus.Status = model.StatusSucceeded

		if err := r.updateNodeUserPodsEvictedStatus(ctx, database, event, podsEvictionStatus, nodeName,
			metrics.DrainStatusCancelled); err != nil {
			slog.Error("Failed to update MongoDB status for node",
				"node", nodeName,
				"error", err)

			return fmt.Errorf("failed to update MongoDB status for node %s: %w", nodeName, err)
		}

		slog.Info("Updated MongoDB status for node",
			"node", nodeName,
			"status", "succeeded")
	}

	r.updateNodeDrainStatus(ctx, nodeName, &healthEvent, false)

	return nil
}

func (r *Reconciler) executeImmediateEviction(ctx context.Context,
	action *evaluator.DrainActionResult, healthEvent model.HealthEventWithStatus) error {
	nodeName := healthEvent.HealthEvent.NodeName
	for _, namespace := range action.Namespaces {
		if err := r.informers.EvictAllPodsInImmediateMode(ctx, namespace, nodeName, action.Timeout); err != nil {
			metrics.ProcessingErrors.WithLabelValues("immediate_eviction_error", nodeName).Inc()
			return fmt.Errorf("failed immediate eviction for namespace %s on node %s: %w", namespace, nodeName, err)
		}
	}

	return fmt.Errorf("immediate eviction completed, requeuing for status verification")
}

func (r *Reconciler) executeTimeoutEviction(ctx context.Context,
	action *evaluator.DrainActionResult, healthEvent model.HealthEventWithStatus) error {
	nodeName := healthEvent.HealthEvent.NodeName
	timeoutMinutes := int(action.Timeout.Minutes())

	if err := r.informers.DeletePodsAfterTimeout(ctx,
		nodeName, action.Namespaces, timeoutMinutes, &healthEvent); err != nil {
		metrics.ProcessingErrors.WithLabelValues("timeout_eviction_error", nodeName).Inc()
		return fmt.Errorf("failed timeout eviction for node %s: %w", nodeName, err)
	}

	return fmt.Errorf("timeout eviction initiated, requeuing for status verification")
}

func (r *Reconciler) executeCheckCompletion(ctx context.Context,
	action *evaluator.DrainActionResult, healthEvent model.HealthEventWithStatus) error {
	nodeName := healthEvent.HealthEvent.NodeName

	allPodsComplete := true

	var remainingPods []string

	for _, namespace := range action.Namespaces {
		pods, err := r.informers.FindEvictablePodsInNamespaceAndNode(namespace, nodeName)
		if err != nil {
			return fmt.Errorf("failed to check pods in namespace %s on node %s: %w", namespace, nodeName, err)
		}

		if len(pods) > 0 {
			allPodsComplete = false

			for _, pod := range pods {
				remainingPods = append(remainingPods, fmt.Sprintf("%s/%s", namespace, pod.Name))
			}
		}
	}

	if !allPodsComplete {
		message := fmt.Sprintf("Waiting for following pods to finish: %v", remainingPods)
		reason := "AwaitingPodCompletion"

		if err := r.informers.UpdateNodeEvent(ctx, nodeName, reason, message); err != nil {
			// Don't fail the whole operation just because event update failed
			slog.Error("Failed to update node event",
				"node", nodeName,
				"error", err)
		}

		slog.Info("Pods still running on node, requeueing for later check",
			"node", nodeName,
			"remainingPods", remainingPods)

		return fmt.Errorf("waiting for pods to complete: %d pods remaining", len(remainingPods))
	}

	slog.Info("All pods completed on node", "node", nodeName)

	return fmt.Errorf("pod completion verified, requeuing for status update")
}

func (r *Reconciler) executeMarkAlreadyDrained(ctx context.Context,
	healthEvent model.HealthEventWithStatus, event datastore.Event, database queue.DataStore) error {
	nodeName := healthEvent.HealthEvent.NodeName
	podsEvictionStatus := &healthEvent.HealthEventStatus.UserPodsEvictionStatus
	podsEvictionStatus.Status = model.AlreadyDrained

	return r.updateNodeUserPodsEvictedStatus(ctx, database, event, podsEvictionStatus,
		nodeName, metrics.DrainStatusSkipped)
}

func (r *Reconciler) executeUpdateStatus(ctx context.Context,
	healthEvent model.HealthEventWithStatus, event datastore.Event, database queue.DataStore) error {
	nodeName := healthEvent.HealthEvent.NodeName
	podsEvictionStatus := &healthEvent.HealthEventStatus.UserPodsEvictionStatus
	podsEvictionStatus.Status = model.StatusSucceeded

	if _, err := r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx,
		nodeName, statemanager.DrainSucceededLabelValue, false); err != nil {
		slog.Error("Failed to update node label to drain-succeeded",
			"node", nodeName,
			"error", err)
		metrics.ProcessingErrors.WithLabelValues("label_update_error", nodeName).Inc()
	}

	err := r.updateNodeUserPodsEvictedStatus(ctx, database, event, podsEvictionStatus,
		nodeName, metrics.DrainStatusDrained)
	if err != nil {
		return fmt.Errorf("failed to update user pod eviction status: %w", err)
	}

	return nil
}

func (r *Reconciler) updateNodeDrainStatus(ctx context.Context,
	nodeName string, healthEvent *model.HealthEventWithStatus, isDraining bool) {
	if healthEvent.HealthEventStatus.NodeQuarantined == nil {
		return
	}

	// Handle UnQuarantined events - remove draining label
	if *healthEvent.HealthEventStatus.NodeQuarantined == model.UnQuarantined {
		if _, err := r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx,
			nodeName, statemanager.DrainingLabelValue, true); err != nil {
			slog.Error("Failed to remove draining label for node",
				"node", nodeName,
				"error", err)
		}

		return
	}

	// Handle Quarantined/AlreadyQuarantined events
	if isDraining {
		if _, err := r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx,
			nodeName, statemanager.DrainingLabelValue, false); err != nil {
			slog.Error("Failed to update node label to draining",
				"node", nodeName,
				"error", err)
			metrics.ProcessingErrors.WithLabelValues("label_update_error", nodeName).Inc()
		}
	}
}

func (r *Reconciler) updateNodeUserPodsEvictedStatus(ctx context.Context, database queue.DataStore,
	event datastore.Event, userPodsEvictionStatus *model.OperationStatus,
	nodeName string, drainStatus string) error {
	// Extract the document ID (preserving native type for MongoDB)
	documentID, err := utils.ExtractDocumentIDNative(event)
	if err != nil {
		return fmt.Errorf("failed to extract document ID: %w", err)
	}

	filter := map[string]interface{}{"_id": documentID}
	update := map[string]interface{}{
		"$set": map[string]interface{}{
			"healtheventstatus.userpodsevictionstatus": *userPodsEvictionStatus,
		},
	}

	_, err = database.UpdateDocument(ctx, filter, update)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("update_status_error", nodeName).Inc()
		return fmt.Errorf("error updating document with ID: %v, error: %w", documentID, err)
	}

	slog.Info("Health event status has been updated",
		"documentID", documentID,
		"evictionStatus", userPodsEvictionStatus.Status)
	metrics.EventsProcessed.WithLabelValues(drainStatus, nodeName).Inc()

	return nil
}

// parseHealthEventFromEvent extracts and parses health event from a document
// The event parameter is already the fullDocument extracted from the change stream
func (r *Reconciler) parseHealthEventFromEvent(event datastore.Event,
	nodeName string) (model.HealthEventWithStatus, error) {
	var healthEventWithStatus model.HealthEventWithStatus

	// The event is already the fullDocument (extracted in main.go)
	// Convert to JSON and then marshal/unmarshal to get proper structure
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("marshal_error", nodeName).Inc()
		return healthEventWithStatus, fmt.Errorf("failed to marshal document: %w", err)
	}

	if err := json.Unmarshal(jsonBytes, &healthEventWithStatus); err != nil {
		metrics.ProcessingErrors.WithLabelValues("unmarshal_error", nodeName).Inc()
		return healthEventWithStatus, fmt.Errorf("failed to unmarshal health event: %w", err)
	}

	// Safety check - ensure HealthEvent is not nil
	if healthEventWithStatus.HealthEvent == nil {
		metrics.ProcessingErrors.WithLabelValues("unmarshal_error", nodeName).Inc()
		return healthEventWithStatus, fmt.Errorf("health event is nil after unmarshaling")
	}

	return healthEventWithStatus, nil
}

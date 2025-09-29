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
	"log"
	"sync"
	"time"

	platformconnectorprotos "github.com/nvidia/nvsentinel/platform-connectors/pkg/protos"
	"github.com/nvidia/nvsentinel/statemanager"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/common"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/watcher"
	"k8s.io/klog"
)

const (
	maxRetries = 5
	retryDelay = 10 * time.Second
)

type ReconcilerConfig struct {
	DataStore          datastore.DataStore
	Pipeline           interface{} // Custom pipeline for change stream filtering (e.g., mongo.Pipeline)
	K8sClient          FaultRemediationClientInterface
	StateManager       statemanager.StateManager
	EnableLogCollector bool
}

type Reconciler struct {
	Config              ReconcilerConfig
	NodeEvictionContext sync.Map
	DryRun              bool
}

type HealthEventDoc struct {
	ID string `json:"id,omitempty"`
	datastore.HealthEventWithStatus
}

func NewReconciler(cfg ReconcilerConfig, dryRunEnabled bool) *Reconciler {
	return &Reconciler{Config: cfg, NodeEvictionContext: sync.Map{}, DryRun: dryRunEnabled}
}

func (r *Reconciler) shouldSkipEvent(healthEventWithStatus datastore.HealthEventWithStatus) bool {
	// Extract the HealthEvent using common utility
	healthEvent, err := common.ExtractPlatformConnectorHealthEvent(&healthEventWithStatus)
	if err != nil {
		klog.Errorf("Failed to extract HealthEvent: %v", err)
		return true // Skip event if extraction fails
	}

	action := healthEvent.RecommendedAction
	nodeName := healthEvent.NodeName

	switch action { // nolint:exhaustive  // we need to trim down the number of recommended actions
	case platformconnectorprotos.RecommenedAction_NONE:
		// NONE means no remediation needed
		klog.Infof("Skipping event for node: %s, recommended action is NONE (no remediation needed)", nodeName)
		return true
	case platformconnectorprotos.RecommenedAction_COMPONENT_RESET,
		platformconnectorprotos.RecommenedAction_RESTART_VM,
		platformconnectorprotos.RecommenedAction_RESTART_BM:
		// need to reboot the node, hence process this event
		return false
	default:
		// All other actions are currently unsupported
		klog.Infof("Unsupported recommended action %s for node %s. Only COMPONENT_RESET, RESTART_VM,"+
			" and RESTART_BM are supported",
			action.String(), nodeName)
		totalUnsupportedRemediationActions.WithLabelValues(action.String(), nodeName).Inc()

		return true
	}
}

//nolint:cyclop // Matches main branch pattern
func (r *Reconciler) Start(ctx context.Context) {
	// Create change stream watcher directly (like main branch)
	watcherConfig := watcher.Config{
		ClientName: "fault-remediation-module",
		TableName:  "HealthEvents",
		Pipeline:   r.Config.Pipeline, // Pass pipeline from config (watches for drain completion)
	}

	changeStreamWatcher, err := watcher.CreateChangeStreamWatcher(ctx, r.Config.DataStore, watcherConfig)
	if err != nil {
		log.Fatalf("failed to create change stream watcher: %+v", err)
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
		klog.Info("Event received....")

		totalEventsReceived.Inc()

		// Extract health event from change stream to HealthEventDoc with ID
		healthEventDoc := HealthEventDoc{}

		extractedEvent, err := common.ExtractHealthEventFromRawEvent(eventWithToken.Event)
		if err != nil {
			totalEventProcessingError.WithLabelValues("unmarshal_doc_error", "unknown").Inc()
			klog.Errorf("Failed to extract health event: %+v", err)

			if err := changeStreamWatcher.MarkProcessed(ctx, eventWithToken.ResumeToken); err != nil {
				totalEventProcessingError.WithLabelValues("mark_processed_error", "unknown").Inc()
				klog.Errorf("Error updating resume token: %+v", err)
			}

			continue
		}

		healthEventDoc.HealthEventWithStatus = *extractedEvent

		// Extract K8s-compliant document ID for CR naming
		healthEventDoc.ID = common.ExtractDocumentIDFromRawEvent(eventWithToken.Event, r.Config.DataStore)

		// Extract node name for logging
		healthEvent, err := common.ExtractPlatformConnectorHealthEvent(&healthEventDoc.HealthEventWithStatus)
		if err != nil {
			klog.Errorf("Failed to extract HealthEvent: %v", err)
			totalEventProcessingError.WithLabelValues("extract_error", "unknown").Inc()

			continue
		}

		// Run log collector for all non-NONE actions if enabled (matching main branch)
		if healthEvent.RecommendedAction != platformconnectorprotos.RecommenedAction_NONE &&
			r.Config.EnableLogCollector {
			klog.Infof("Log collector feature enabled; running log collector for node %s", healthEvent.NodeName)

			if err := r.Config.K8sClient.RunLogCollectorJob(ctx, healthEvent.NodeName); err != nil {
				klog.Errorf("Log collector job failed for node %s: %v", healthEvent.NodeName, err)
			}
		}

		eventSkipped, nodeRemediatedStatus := r.executeRemediation(ctx, healthEventDoc)
		if !eventSkipped {
			updateErr := r.updateNodeRemediatedStatus(
				ctx, healthEvent.NodeName, nodeRemediatedStatus, eventWithToken.Event,
			)
			if updateErr != nil {
				totalEventProcessingError.WithLabelValues("update_status_error", healthEvent.NodeName).Inc()
				log.Printf("\nError updating remediation status for node: %+v\n", updateErr)
			} else {
				totalEventsSuccessfullyProcessed.Inc()
			}
		}

		if err := changeStreamWatcher.MarkProcessed(ctx, eventWithToken.ResumeToken); err != nil {
			totalEventProcessingError.WithLabelValues("mark_processed_error", healthEvent.NodeName).Inc()
			klog.Errorf("Error updating resume token: %+v", err)
		}
	}
}

func (r *Reconciler) executeRemediation(ctx context.Context, healthEventWithStatus HealthEventDoc) (bool, bool) {
	healthEvent, err := common.ExtractPlatformConnectorHealthEvent(&healthEventWithStatus.HealthEventWithStatus)
	if err != nil {
		klog.Errorf("Failed to extract HealthEvent: %v", err)
		return true, false // Skip event
	}

	_, err = r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx, healthEvent.NodeName,
		statemanager.RemediatingLabelValue, false)
	if err != nil {
		klog.Errorf("Error updating node label: %+v", err)
		totalEventProcessingError.WithLabelValues("label_update_error", healthEvent.NodeName).Inc()
	}

	shouldSkipEvent := r.shouldSkipEvent(healthEventWithStatus.HealthEventWithStatus)

	nodeRemediatedStatus := false

	remediationLabelValue := statemanager.RemediationFailedLabelValue

	if !shouldSkipEvent {
		for i := 1; i <= maxRetries; i++ {
			klog.Infof("Attempt %d, handle event for node: %s", i, healthEvent.NodeName)

			if r.Config.K8sClient.CreateMaintenanceResource(ctx, &healthEventWithStatus) {
				nodeRemediatedStatus = true
				remediationLabelValue = statemanager.RemediationSucceededLabelValue

				break
			}

			if i < maxRetries {
				time.Sleep(retryDelay)
			}
		}
	}
	// If shouldSkipEvent is true or if the nodeRemediatedStatus is false, we will update the state to remediation-failed,
	// else we will update the state to remediation-succeeded.
	_, err = r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx, healthEvent.NodeName,
		remediationLabelValue, false)
	if err != nil {
		klog.Errorf("Error updating node label: %+v", err)
		totalEventProcessingError.WithLabelValues("label_update_error", healthEvent.NodeName).Inc()
	}

	return shouldSkipEvent, nodeRemediatedStatus
}

func (r *Reconciler) updateNodeRemediatedStatus(
	ctx context.Context, nodeName string, nodeRemediatedStatus bool, rawEvent map[string]interface{},
) error {
	var err error

	rawObjectID := common.ExtractDocumentIDForUpdate(rawEvent, r.Config.DataStore)
	if rawObjectID == "" {
		klog.V(2).Infof("Could not extract ObjectID from raw event for node %s, skipping database status update", nodeName)
		return nil
	}

	status := datastore.HealthEventStatus{
		FaultRemediated: &nodeRemediatedStatus,
	}

	for i := 1; i <= maxRetries; i++ {
		klog.Infof("Attempt %d, updating health event for node %s with ID %s", i, nodeName, rawObjectID)

		err = r.Config.DataStore.HealthEventStore().UpdateHealthEventStatus(ctx, rawObjectID, status)
		if err == nil {
			break
		}

		time.Sleep(retryDelay)
	}

	if err != nil {
		return fmt.Errorf("error updating document with ID: %v, error: %w", rawObjectID, err)
	}

	klog.Infof("Health event with ID %v has been updated with status %+v", rawObjectID, nodeRemediatedStatus)

	return nil
}

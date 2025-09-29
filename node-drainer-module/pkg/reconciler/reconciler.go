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
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/nvidia/nvsentinel/node-drainer-module/pkg/config"
	"github.com/nvidia/nvsentinel/node-drainer-module/pkg/metrics"
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

// EvictionContext holds cancellation context for pod evictions
type EvictionContext struct {
	cancel context.CancelFunc
}

type ReconcilerConfig struct {
	TomlConfig   config.TomlConfig
	DataStore    datastore.DataStore
	Pipeline     interface{} // Custom pipeline for change stream filtering (e.g., mongo.Pipeline)
	K8sClient    NodeDrainerClientInterface
	StateManager statemanager.StateManager
}

type Reconciler struct {
	Config              ReconcilerConfig
	NodeEvictionContext sync.Map
	DryRun              bool
}

func NewReconciler(cfg ReconcilerConfig, dryRunEnabled bool) *Reconciler {
	return &Reconciler{Config: cfg, NodeEvictionContext: sync.Map{}, DryRun: dryRunEnabled}
}

func (r *Reconciler) Start(ctx context.Context) {
	watcherConfig := watcher.Config{
		ClientName: "node-drainer-module",
		TableName:  "HealthEvents",
		Pipeline:   r.Config.Pipeline, // Pass pipeline from config (watches for nodequarantined updates)
	}

	watcher, err := watcher.CreateChangeStreamWatcher(ctx, r.Config.DataStore, watcherConfig)
	if err != nil {
		klog.Fatalf("failed to create change stream watcher: %+v", err)
	}
	defer watcher.Close(ctx)

	healthEventStore := r.Config.DataStore.HealthEventStore()

	oldEvents, err := r.getInProgressEvents(ctx, healthEventStore)
	if err != nil {
		klog.Errorf("Error getting in-progress events: %+v", err)
	} else {
		for _, event := range oldEvents {
			r.startEventProcessing(ctx, event, healthEventStore)
		}
	}

	watcher.Start(ctx)

	klog.Infoln("Listening for events on the channel...")

	for eventWithToken := range watcher.Events() {
		metrics.TotalEventsReceived.Inc()

		healthEventWithStatus, err := common.ExtractHealthEventFromRawEvent(eventWithToken.Event)
		if err != nil {
			metrics.TotalEventProcessingError.WithLabelValues("extract_error").Inc()
			klog.Errorf("Failed to extract health event: %+v", err)

			continue
		}

		// Store the raw event for later use in updates
		healthEventWithStatus.RawEvent = eventWithToken.Event

		r.startEventProcessing(ctx, *healthEventWithStatus, healthEventStore)

		if err := watcher.MarkProcessed(ctx, eventWithToken.ResumeToken); err != nil {
			metrics.TotalEventProcessingError.WithLabelValues("mark_processed_error").Inc()
			klog.Errorf("Error updating resume token: %+v", err)
		}
	}
}

func (r *Reconciler) startEventProcessing(ctx context.Context, healthEventWithStatus datastore.HealthEventWithStatus,
	healthEventStore datastore.HealthEventStore) {
	currentStatus := healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status
	if currentStatus == datastore.StatusSucceeded || currentStatus == datastore.StatusFailed {
		klog.Infof("Skipping health event as its already in terminal state"+
			"\nHealth event: %+v", healthEventWithStatus.HealthEvent)
		return
	}

	healthEvent, err := common.ExtractPlatformConnectorHealthEvent(&healthEventWithStatus)
	if err != nil {
		metrics.TotalEventProcessingError.WithLabelValues("extract_error").Inc()
		klog.Errorf("Failed to extract HealthEvent: %v", err)

		return
	}

	klog.Infof("Received event: \n%+v", healthEvent)
	// set the user pod eviction status to in-progress
	podsEvictionStatus := &healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus
	podsEvictionStatus.Status = datastore.StatusInProgress

	if healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil {
		//nolint:exhaustive // todo
		switch *healthEventWithStatus.HealthEventStatus.NodeQuarantined {
		case datastore.Quarantined:
			metrics.UnhealthyEvent.WithLabelValues(healthEvent.NodeName, healthEvent.CheckName).Inc()
		case datastore.UnQuarantined:
			metrics.HealthyEvent.WithLabelValues(healthEvent.NodeName, healthEvent.CheckName).Inc()
		}
	}

	err = r.updateNodeUserPodsEvictedStatus(ctx, healthEventStore, &healthEventWithStatus, podsEvictionStatus)
	if err != nil {
		metrics.TotalEventProcessingError.WithLabelValues("update_status_error").Inc()
		klog.Errorf("Error in updating health event: \n%+v:, \nerror: %+v", healthEvent.NodeName, err)
	}

	go func(event datastore.HealthEventWithStatus) {
		r.processEvents(ctx, healthEventStore, event)
	}(healthEventWithStatus)
}

func (r *Reconciler) processEvents(ctx context.Context, healthEventStore datastore.HealthEventStore,
	healthEventWithStatus datastore.HealthEventWithStatus) {
	startTime := time.Now()

	var err error

	podsEvictionStatus := &healthEventWithStatus.HealthEventStatus.UserPodsEvictionStatus

	healthEvent, err := common.ExtractPlatformConnectorHealthEvent(&healthEventWithStatus)
	if err != nil {
		klog.Errorf("Failed to extract HealthEvent: %v", err)
		return
	}

	for i := 1; i <= maxRetries; i++ {
		klog.Infof("Attempt %d, Processing health event: %+v", i, healthEventWithStatus)

		err = r.handleEvent(ctx, healthEvent.NodeName, &healthEventWithStatus)
		if err == nil {
			metrics.TotalEventsSuccessfullyProcessed.Inc()

			podsEvictionStatus.Status = datastore.StatusSucceeded

			break
		}

		klog.Errorf("Error in processing the event:\n%+v, error is : \n%+v", healthEvent, err)
		metrics.TotalEventProcessingError.WithLabelValues("handle_event_error").Inc()
		time.Sleep(retryDelay)
	}

	if err != nil {
		klog.Errorf("Max attempt reached, error in handling health event: "+
			"%+v:, \nerror: %+v", healthEvent, err)

		podsEvictionStatus.Status = datastore.StatusFailed
		podsEvictionStatus.Message = err.Error()
	}

	updateErr := r.updateNodeUserPodsEvictedStatus(ctx, healthEventStore, &healthEventWithStatus, podsEvictionStatus)
	if updateErr != nil {
		metrics.TotalEventProcessingError.WithLabelValues("update_status_error").Inc()
		klog.Errorf("Error in updating the user pods eviction status for node: %+v", err)
	}

	duration := time.Since(startTime).Seconds()
	klog.V(2).Infof("Event handling took %.2f seconds", duration)
}

//nolint:cyclop,gocognit //todo
func (r *Reconciler) handleEvent(ctx context.Context, nodeName string,
	healthEventWithStatus *datastore.HealthEventWithStatus) error {
	namespaceMap := r.getMatchingNamespace(ctx)
	deleteAfterTimeout := r.Config.TomlConfig.DeleteAfterTimeoutMinutes
	getTimeoutNamespaces := r.getTimeoutNamespaces(ctx)

	healthEvent, err := common.ExtractPlatformConnectorHealthEvent(healthEventWithStatus)
	if err != nil {
		return fmt.Errorf("failed to extract HealthEvent: %w", err)
	}

	// If DrainOverrides.Force is true, override all namespaces to use immediate eviction
	if healthEvent.DrainOverrides != nil && healthEvent.DrainOverrides.Force {
		klog.Infof("DrainOverrides.Force is true, forcing immediate eviction for all namespaces")

		for ns := range namespaceMap {
			namespaceMap[ns] = config.ModeImmediateEvict
		}
	}

	var mu sync.Mutex

	nsWithImmediateMode := []string{}

	var wg sync.WaitGroup

	errChan := make(chan error, len(r.Config.TomlConfig.UserNamespaces))

	// Set metric based on node quarantine status
	if healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil &&
		*healthEventWithStatus.HealthEventStatus.NodeQuarantined == datastore.UnQuarantined {
		// Node is healthy/unquarantined - set metric to 0
		metrics.NodeDrainStatus.WithLabelValues(nodeName).Set(0)
	} else {
		_, err := r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx, nodeName, statemanager.DrainingLabelValue, false)
		if err != nil {
			klog.Errorf("Error updating node label: %+v", err)
			metrics.TotalEventProcessingError.WithLabelValues("label_update_error").Inc()
		}
		// Node is quarantined - set metric to 1 to indicate draining started
		metrics.NodeDrainStatus.WithLabelValues(nodeName).Set(1)
	}

	if len(getTimeoutNamespaces) > 0 &&
		healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil &&
		*healthEventWithStatus.HealthEventStatus.NodeQuarantined == datastore.Quarantined {
		ctxTimeout, cancelTimeout := context.WithCancel(ctx)
		timeoutKey := fmt.Sprintf("%s-timeout", nodeName)

		r.NodeEvictionContext.Store(timeoutKey, &EvictionContext{cancel: cancelTimeout})

		wg.Add(1)

		f := func(ctx context.Context, cancelFn context.CancelFunc, timeoutKey string, nodeName string, namespaces []string) {
			defer func() {
				// ensure the derived context is cancelled to release resources
				cancelFn()
				// remove the key so that future healthy events don't try to cancel again
				r.NodeEvictionContext.Delete(timeoutKey)
				metrics.NodeDrainTimeout.WithLabelValues(nodeName).Set(0)
				wg.Done()
			}()

			metrics.NodeDrainTimeout.WithLabelValues(nodeName).Set(1)

			if err := r.Config.K8sClient.DeletePodsAfterTimeout(ctx, nodeName, namespaces,
				deleteAfterTimeout, healthEventWithStatus); err != nil {
				klog.Errorf("Error in deleting pod if not finished: %+v", err)
				metrics.NodeDrainError.WithLabelValues("delete_pods_after_timeout_error", nodeName).Inc()
			}
		}
		go f(ctxTimeout, cancelTimeout, timeoutKey, nodeName, getTimeoutNamespaces)
	}

	for ns, mode := range namespaceMap {
		nsWithNode := fmt.Sprintf("%s-%s", nodeName, ns)
		//nolint:nestif // TODO
		if healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil &&
			*healthEventWithStatus.HealthEventStatus.NodeQuarantined == datastore.UnQuarantined {
			if _, ok := r.NodeEvictionContext.Load(nsWithNode); ok {
				if mode == config.ModeAllowCompletion {
					metrics.HealthyEventWithContextCancellation.Inc()
					klog.Infof("Cancelling the eviction of pods in namespace %s on node %s", ns, nodeName)

					context, _ := r.NodeEvictionContext.Load(nsWithNode)
					evictionContext := context.(*EvictionContext)

					evictionContext.cancel()
				}
			}

			if context, ok := r.NodeEvictionContext.Load(fmt.Sprintf("%s-timeout", nodeName)); ok {
				klog.Infof("Cancelling the eviction of pods on node %s", nodeName)

				evictionContext := context.(*EvictionContext)

				evictionContext.cancel()
			}
		} else {
			wg.Add(1)

			f := func(ctx context.Context, mode config.EvictMode, nodeName string, ns string, nsWithNode string) {
				ctx1, cancel := context.WithCancel(ctx)
				defer func() {
					cancel()
					wg.Done()
				}()

				//nolint:exhaustive // todo
				switch mode {
				case config.ModeImmediateEvict:
					klog.Infof("Evicting pods from namespace %s in %s mode", ns, mode)
					mu.Lock()
					nsWithImmediateMode = append(nsWithImmediateMode, ns)
					mu.Unlock()

					if err := r.Config.K8sClient.EvictAllPodsInImmediateMode(ctx, ns, nodeName,
						r.Config.TomlConfig.EvictionTimeoutInSeconds.Duration); err != nil {
						klog.Infof("error while evicting pods in namespace %s on node %s: %+v\n", ns, nodeName, err)
						errChan <- err
					}
				case config.ModeAllowCompletion:
					r.NodeEvictionContext.Store(nsWithNode, &EvictionContext{cancel: cancel})
					klog.Infof("Monitoring pods for completion in namespace %s in %s mode", ns, mode)

					if err := r.Config.K8sClient.MonitorPodCompletion(ctx1, ns, nodeName); err != nil {
						klog.Infof("error while monitoring pods to complete in namespace %s on node %s: %+v\n", ns, nodeName, err)
						errChan <- err
					}

				default:
					klog.Errorf("Invalid mode of eviction; ignoring pods in namespace %s in %s mode", ns, mode)
				}

				r.NodeEvictionContext.Delete(nsWithNode)

				select {
				case <-ctx1.Done():
					klog.Infof("Context cancelled for health event: %+v", healthEventWithStatus)
				default:
				}
			}
			go f(ctx, mode, nodeName, ns, nsWithNode)
		}
	}

	wg.Wait()
	close(errChan)

	var drainError error
	// errChan is only written to when draining is occurring for Quarantined HealthEvents
	if len(errChan) > 0 {
		var mErr *multierror.Error
		for err := range errChan {
			mErr = multierror.Append(mErr, err)
		}

		drainError = mErr
	} else {
		// verifyEvictionCompleted ensures that eviction verification only occurs on Quarantined HealthEvents
		drainError = r.verifyEvictionCompleted(ctx, healthEventWithStatus, nodeName, nsWithImmediateMode)
	}

	// We will update the state label from draining to drain-succeeded or drain-failed on Quarantined HealthEvents
	if healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil &&
		*healthEventWithStatus.HealthEventStatus.NodeQuarantined == datastore.Quarantined {
		drainLabelValue := statemanager.DrainSucceededLabelValue

		if drainError != nil {
			metrics.NodeDrainStatus.WithLabelValues(nodeName).Set(0)

			drainLabelValue = statemanager.DrainFailedLabelValue
		}

		_, err := r.Config.StateManager.UpdateNVSentinelStateNodeLabel(ctx, nodeName, drainLabelValue, false)
		if err != nil {
			klog.Errorf("Error updating node label: %+v", err)
			metrics.TotalEventProcessingError.WithLabelValues("label_update_error").Inc()
		}
	}
	// drainError can only be non-nil on Quarantined HealthEvents
	return drainError
}

func (r *Reconciler) getMatchingNamespace(ctx context.Context) map[string]config.EvictMode {
	namespaceMap := make(map[string]config.EvictMode)
	systemNamespaces := r.Config.TomlConfig.SystemNamespaces

	for _, userNamespace := range r.Config.TomlConfig.UserNamespaces {
		if userNamespace.Mode == config.ModeDeleteAfterTimeout {
			continue
		}

		matchedNamespaces, err := r.Config.K8sClient.GetNamespacesMatchingPattern(ctx, userNamespace.Name, systemNamespaces)

		if err != nil {
			klog.Errorf("Error while matching namespaces with pattern %s: %+v", userNamespace.Name, err)
			continue
		}

		for _, ns := range matchedNamespaces {
			// Add only if not present in the map
			if _, ok := namespaceMap[ns]; !ok {
				namespaceMap[ns] = userNamespace.Mode
			}
		}
	}

	return namespaceMap
}

func (r *Reconciler) getTimeoutNamespaces(ctx context.Context) []string {
	timeoutNamespaces := []string{}
	systemNamespaces := r.Config.TomlConfig.SystemNamespaces

	for _, userNamespace := range r.Config.TomlConfig.UserNamespaces {
		if userNamespace.Mode == config.ModeDeleteAfterTimeout {
			matchedNamespaces, err := r.Config.K8sClient.GetNamespacesMatchingPattern(ctx, userNamespace.Name, systemNamespaces)
			if err != nil {
				klog.Errorf("Error while matching namespaces with pattern %s: %+v", userNamespace.Name, err)
				continue
			}

			timeoutNamespaces = append(timeoutNamespaces, matchedNamespaces...)
		}
	}

	return timeoutNamespaces
}

// getInProgressEvents to get the events for which draining was already started
func (r *Reconciler) getInProgressEvents(ctx context.Context,
	healthEventStore datastore.HealthEventStore) ([]datastore.HealthEventWithStatus, error) {
	klog.Info("Querying for in-progress drain events from datastore")

	// Query for events with StatusInProgress in userPodsEvictionStatus
	events, err := healthEventStore.FindHealthEventsByStatus(ctx, datastore.StatusInProgress)
	if err != nil {
		return nil, fmt.Errorf("failed to query in-progress events: %w", err)
	}

	klog.Infof("Found %d in-progress drain events to resume", len(events))

	return events, nil
}

func (r *Reconciler) verifyEvictionCompleted(ctx context.Context,
	healthEventWithStatus *datastore.HealthEventWithStatus, nodeName string, nsWithImmediateMode []string) error {
	if healthEventWithStatus.HealthEventStatus.NodeQuarantined != nil &&
		*healthEventWithStatus.HealthEventStatus.NodeQuarantined == datastore.Quarantined && !r.DryRun {
		klog.Infof("Verifying if all pods have been successfully evicted, if not, forcefully deleting them")

		allEvicted := r.Config.K8sClient.CheckIfAllPodsAreEvictedInImmediateMode(ctx, nsWithImmediateMode, nodeName,
			r.Config.TomlConfig.EvictionTimeoutInSeconds.Duration)
		if !allEvicted {
			return fmt.Errorf("error in evicting all pods in namespace %v on node %s", nsWithImmediateMode, nodeName)
		}

		metrics.NodeDrainSuccess.WithLabelValues(nodeName).Inc()
	}

	return nil
}

func (r *Reconciler) updateNodeUserPodsEvictedStatus(ctx context.Context, healthEventStore datastore.HealthEventStore,
	healthEventWithStatus *datastore.HealthEventWithStatus, userPodsEvictionStatus *datastore.OperationStatus) error {
	var err error

	// Extract document ID from the raw event (matches main branch pattern)
	rawObjectID := common.ExtractDocumentIDForUpdate(healthEventWithStatus.RawEvent, r.Config.DataStore)
	if rawObjectID == "" {
		klog.V(2).Infof("Could not extract ObjectID from raw event, skipping database status update")
		return nil
	}

	status := datastore.HealthEventStatus{
		UserPodsEvictionStatus: *userPodsEvictionStatus,
	}

	for i := 1; i <= maxRetries; i++ {
		klog.Infof("Attempt %d, updating health event with ID %s", i, rawObjectID)

		err = healthEventStore.UpdateHealthEventStatus(ctx, rawObjectID, status)
		if err == nil {
			break
		}

		time.Sleep(retryDelay)
	}

	if err != nil {
		return fmt.Errorf("error updating document with ID: %v, error: %w", rawObjectID, err)
	}

	klog.Infof("Health event status has been updated , status: %+v", userPodsEvictionStatus)

	return nil
}

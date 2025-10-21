// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package event

import (
	"context"
	"fmt"
	"sync"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/metrics"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
	klog "k8s.io/klog/v2"
)

// Processor persists normalized maintenance events to the backing datastore.
// Any CSP-specific node-mapping must already have been resolved by the caller.
type Processor struct {
	store  datastore.DataStore
	config *config.Config
	mu     sync.Mutex
}

// NewProcessor returns an initialised Processor. k8sMapper parameter is
// removed.
func NewProcessor(cfg *config.Config, store datastore.DataStore) *Processor {
	if cfg == nil || store == nil {
		klog.Fatalf("Cannot create Event Processor with nil dependencies (config or store)")
		return nil // Should not be reached
	}

	return &Processor{
		config: cfg,
		store:  store,
	}
}

// ensureClusterName sets the ClusterName on the event from config if missing.
func (p *Processor) ensureClusterName(event *model.MaintenanceEvent) {
	if event.ClusterName == "" && p.config.ClusterName != "" {
		event.ClusterName = p.config.ClusterName
		klog.V(2).Infof("Event %s missing cluster name; set from config: %s",
			event.EventID, event.ClusterName)
	} else if event.ClusterName == "" {
		klog.Warningf("Event %s missing cluster name and no global config available.", event.EventID)
	}
}

// defaultStatus ensures event.Status has a default value.
func defaultStatus(event *model.MaintenanceEvent) {
	if event.Status == "" {
		klog.Warningf("Event %s has empty status; defaulting to %s.",
			event.EventID, model.StatusDetected)

		event.Status = model.StatusDetected
	}
}

// inheritState applies stateful inheritance based on event.Status.
func (p *Processor) inheritState(ctx context.Context, event *model.MaintenanceEvent) {
	// We only need NodeName to attempt a lookup.
	// MaintenanceType might be empty on the incoming event (e.g. sparse COMPLETED log)
	// and the specific inheritance functions will handle trying to find a match.
	if event.NodeName == "" {
		klog.V(2).Infof("Event %s missing NodeName; skipping inheritance", event.EventID)
		return
	}

	switch event.Status {
	case model.StatusMaintenanceOngoing:
		p.inheritPendingToOngoing(
			ctx,
			event,
		) // This function expects event.MaintenanceType to be non-empty from normalizer if possible
	case model.StatusMaintenanceComplete:
		p.inheritOngoingToCompleted(ctx, event) // This function handles if event.MaintenanceType is initially empty
	case model.StatusDetected,
		model.StatusQuarantineTriggered,
		model.StatusHealthyTriggered,
		model.StatusNodeReadinessTimeout,
		model.StatusCancelled,
		model.StatusError:
		klog.V(3).
			Infof("Event %s has status %s; no specific state inheritance rules apply.", event.EventID, event.Status)
	default:
		// This case handles any unexpected or future InternalStatus values.
		klog.Warningf(
			"Event %s has unhandled status '%s' in inheritState; no action taken.",
			event.EventID,
			event.Status,
		)
	}
}

// inheritPendingToOngoing inherits fields from a prior PENDING event.
//
//nolint:cyclop
func (p *Processor) inheritPendingToOngoing(ctx context.Context, event *model.MaintenanceEvent) {
	prior, found, err := p.store.MaintenanceEventStore().FindLatestActiveEventByNodeAndType(
		ctx, event.NodeName, event.MaintenanceType, []model.InternalStatus{model.StatusDetected})
	if err != nil {
		klog.Warningf("Error finding prior PENDING for event %s: %v", event.EventID, err)
		return
	}

	if !found || prior == nil {
		return
	}

	if prior.ScheduledStartTime != nil {
		event.ScheduledStartTime = prior.ScheduledStartTime
	}

	if prior.ScheduledEndTime != nil {
		event.ScheduledEndTime = prior.ScheduledEndTime
	}

	event.MaintenanceType = prior.MaintenanceType
	if (event.ResourceID == "" || event.ResourceID == defaultUnknown) && prior.ResourceID != "" &&
		prior.ResourceID != defaultUnknown {
		event.ResourceID = prior.ResourceID
	}

	if (event.ResourceType == "" || event.ResourceType == defaultUnknown) && prior.ResourceType != "" &&
		prior.ResourceType != defaultUnknown {
		event.ResourceType = prior.ResourceType
	}
}

// inheritOngoingToCompleted inherits fields from a prior ONGOING event.
//
//nolint:cyclop
func (p *Processor) inheritOngoingToCompleted(ctx context.Context, event *model.MaintenanceEvent) {
	klog.InfoS(
		"[Processor.inheritOngoingToCompleted] Processing COMPLETED event, attempting to find latest ONGOING for node",
		"completedEventID",
		event.EventID,
		"nodeName",
		event.NodeName,
	)

	priorEvent, found, err := p.store.MaintenanceEventStore().FindLatestOngoingEventByNode(ctx, event.NodeName)
	if err != nil {
		klog.Warningf(
			"[inheritOngoingToCompleted] Error finding prior ONGOING event for COMPLETED event %s "+
				"(Node: %s): %v. No inheritance applied.",
			event.EventID,
			event.NodeName,
			err,
		)

		return
	}

	if !found || priorEvent == nil {
		klog.Warningf(
			"[inheritOngoingToCompleted] No prior ONGOING event found for COMPLETED event %s "+
				"(Node: %s). No inheritance applied.",
			event.EventID,
			event.NodeName,
		)

		return
	}

	klog.V(2).InfoS("COMPLETED event before inheritance",
		"eventID", event.EventID,
		"maintenanceType", event.MaintenanceType,
		"scheduledStart", safeFormatTime(event.ScheduledStartTime),
		"actualStart", safeFormatTime(event.ActualStartTime),
	)
	klog.V(3).InfoS("Prior ONGOING event details",
		"priorEventID", priorEvent.EventID,
		"maintenanceType", priorEvent.MaintenanceType,
		"scheduledStart", safeFormatTime(priorEvent.ScheduledStartTime),
		"actualStart", safeFormatTime(priorEvent.ActualStartTime),
	)

	// Inherit from the found prior ONGOING event
	event.MaintenanceType = priorEvent.MaintenanceType // Take the type from the ONGOING event

	if priorEvent.ScheduledStartTime != nil {
		event.ScheduledStartTime = priorEvent.ScheduledStartTime
	}

	if priorEvent.ScheduledEndTime != nil {
		event.ScheduledEndTime = priorEvent.ScheduledEndTime
	}

	if priorEvent.ActualStartTime != nil {
		event.ActualStartTime = priorEvent.ActualStartTime
	}

	if (event.ResourceID == "" || event.ResourceID == defaultUnknown) &&
		(priorEvent.ResourceID != "" && priorEvent.ResourceID != defaultUnknown) {
		event.ResourceID = priorEvent.ResourceID
	}

	if (event.ResourceType == "" || event.ResourceType == defaultUnknown) &&
		(priorEvent.ResourceType != "" && priorEvent.ResourceType != defaultUnknown) {
		event.ResourceType = priorEvent.ResourceType
	}

	klog.V(2).InfoS("COMPLETED event after inheritance",
		"eventID", event.EventID,
		"maintenanceType", event.MaintenanceType,
		"scheduledStart", safeFormatTime(event.ScheduledStartTime),
		"actualStart", safeFormatTime(event.ActualStartTime),
	)
}

// logEventDetails logs key event fields before upsert.
func (p *Processor) logEventDetails(event *model.MaintenanceEvent) {
	klog.InfoS("Event details before upsert",
		"EventID", event.EventID,
		"Cluster", event.ClusterName,
		"Node", event.NodeName,
		"MaintenanceType", event.MaintenanceType,
		"InternalStatus", event.Status,
		"CSPStatus", event.CSPStatus,
		"ScheduledStart", safeFormatTime(event.ScheduledStartTime),
		"ScheduledEnd", safeFormatTime(event.ScheduledEndTime),
		"ActualStart", safeFormatTime(event.ActualStartTime),
		"ActualEnd", safeFormatTime(event.ActualEndTime),
		"ResourceType", event.ResourceType,
		"ResourceID", event.ResourceID)
}

// logMissingNode warns if NodeName is missing but ResourceID is present.
func (p *Processor) logMissingNode(event *model.MaintenanceEvent) {
	if event.NodeName == "" && event.ResourceID != "" && event.ResourceID != defaultUnknown {
		klog.Warningf(
			"Event %s has ResourceID %s but no NodeName; mapping may have failed",
			event.EventID,
			event.ResourceID,
		)
	}
}

// ProcessEvent takes a normalized MaintenanceEvent and upserts it to the
// datastore. NodeName is expected to be pre-populated by the CSP client if
// mapping was successful.
func (p *Processor) ProcessEvent(ctx context.Context, event *model.MaintenanceEvent) error {
	if event == nil {
		metrics.MainProcessingErrors.WithLabelValues("unknown", "nil_event").Inc()
		return fmt.Errorf("cannot process nil event")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.ensureClusterName(event)
	defaultStatus(event)
	p.inheritState(ctx, event)
	p.logEventDetails(event)
	p.logMissingNode(event)

	metrics.MainDatastoreUpsertAttempts.WithLabelValues(string(event.CSP)).Inc()

	if err := p.store.MaintenanceEventStore().UpsertMaintenanceEvent(ctx, event); err != nil {
		metrics.MainDatastoreUpsertErrors.WithLabelValues(string(event.CSP)).Inc()
		metrics.MainProcessingErrors.WithLabelValues(string(event.CSP), "datastore_upsert").Inc()
		klog.Errorf("Failed to upsert event %s: %v", event.EventID, err)

		return fmt.Errorf("failed to upsert event %s: %w", event.EventID, err)
	}

	metrics.MainDatastoreUpsertSuccess.WithLabelValues(string(event.CSP)).Inc()

	klog.V(1).Infof("Processed event %s (Node:%s)", event.EventID, event.NodeName)

	return nil
}

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

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nvidia/nvsentinel/node-drainer/pkg/metrics"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/client-go/util/workqueue"
)

func NewEventQueueManager() EventQueueManager {
	mgr := &eventQueueManager{
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[NodeEvent](10*time.Second, 2*time.Minute),
		),
		shutdown: make(chan struct{}),
	}

	return mgr
}

// SetEventProcessor method has been removed - use SetDataStoreEventProcessor instead

func (m *eventQueueManager) SetDataStoreEventProcessor(processor DataStoreEventProcessor) {
	m.dataStoreEventProcessor = processor
}

// EnqueueEventGeneric enqueues an event using the new database-agnostic interface
func (m *eventQueueManager) EnqueueEventGeneric(ctx context.Context,
	nodeName string, event datastore.Event, database DataStore) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context cancelled while enqueueing event for node %s: %w", nodeName, ctx.Err())
	}

	select {
	case <-m.shutdown:
		return fmt.Errorf("queue is shutting down")
	default:
	}

	// Extract eventID from the event map
	eventID := extractEventID(event)

	nodeEvent := NodeEvent{
		NodeName: nodeName,
		EventID:  eventID,
		Event:    &event,
		Database: database,
	}

	slog.Debug("Enqueueing event", "nodeName", nodeName, "eventID", eventID)

	m.queue.Add(nodeEvent)
	metrics.QueueDepth.Set(float64(m.queue.Len()))

	return nil
}

// extractEventID extracts the document ID from an event map
// Tries both MongoDB (_id) and PostgreSQL (id) formats
func extractEventID(event datastore.Event) string {
	// Try MongoDB _id format first
	if id, exists := event["_id"]; exists {
		return fmt.Sprintf("%v", id)
	}

	// Try PostgreSQL id format
	if id, exists := event["id"]; exists {
		return fmt.Sprintf("%v", id)
	}

	// Fallback: use empty string (shouldn't happen in practice)
	slog.Debug("No _id or id found in event", "eventKeys", getEventKeys(event))

	return ""
}

// getEventKeys returns the keys of an event map for debugging
func getEventKeys(event datastore.Event) []string {
	keys := make([]string, 0, len(event))
	for k := range event {
		keys = append(keys, k)
	}

	return keys
}

// EnqueueEvent method has been removed - use EnqueueEventGeneric instead

func (m *eventQueueManager) Shutdown() {
	slog.Info("Shutting down workqueue")
	m.queue.ShutDown()
	close(m.shutdown)
	slog.Info("Workqueue shutdown complete")
}

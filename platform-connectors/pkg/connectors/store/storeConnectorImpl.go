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

package store

import (
	"context"
	"fmt"
	"os"
	"time"

	platformconnector "github.com/nvidia/nvsentinel/platform-connectors/pkg/protos"
	"github.com/nvidia/nvsentinel/platform-connectors/pkg/ringbuffer"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"k8s.io/klog"
)

type StoreConnector struct {
	// dataStore is the abstracted data store client
	dataStore datastore.DataStore
	// ringBuffer for pushing data to the resource count sink
	ringBuffer *ringbuffer.RingBuffer
	nodeName   string
}

func newStoreConnector(
	dataStore datastore.DataStore,
	ringBuffer *ringbuffer.RingBuffer,
	nodeName string,
) *StoreConnector {
	return &StoreConnector{
		dataStore:  dataStore,
		ringBuffer: ringBuffer,
		nodeName:   nodeName,
	}
}

func InitializeStoreConnector(ctx context.Context, ringbuffer *ringbuffer.RingBuffer) *StoreConnector {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatalf("Failed to fetch nodename")
	}

	// Load datastore configuration
	datastoreConfig, err := config.LoadDatastoreConfig()
	if err != nil {
		klog.Fatalf("Failed to load datastore configuration: %v", err)
	}

	// Initialize datastore using the abstraction layer
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		klog.Fatalf("Failed to initialize data store: %v", err)
	}

	klog.Info("Successfully initialized store connector with datastore abstraction.")

	return newStoreConnector(dataStore, ringbuffer, nodeName)
}

func (r *StoreConnector) FetchAndProcessHealthMetric(ctx context.Context) {
	defer func() {
		err := r.dataStore.Close(ctx)
		if err != nil {
			klog.Errorf("failed to close datastore connection with error: %+v ", err)
		}
	}()

	klog.V(2).Info("Starting health events processing loop for MongoDB storage")

	for {
		select {
		case <-ctx.Done():
			klog.Info("Context canceled. Exiting health metric processing loop.")
			return
		default:
			healthEvents := r.ringBuffer.Dequeue()
			if healthEvents == nil || len(healthEvents.GetEvents()) == 0 {
				// Add slight delay to prevent busy waiting
				time.Sleep(100 * time.Millisecond)
				continue
			}

			klog.V(3).Infof("Dequeued health events batch with %d events", len(healthEvents.GetEvents()))

			err := r.insertHealthEvents(ctx, healthEvents)
			if err != nil {
				klog.Errorf("Error inserting health events: %v", err)
				r.ringBuffer.HealthMetricEleProcessingFailed(healthEvents)
			} else {
				klog.V(2).Infof("Successfully stored %d health events in MongoDB", len(healthEvents.GetEvents()))
				r.ringBuffer.HealthMetricEleProcessingCompleted(healthEvents)
			}
		}
	}
}

func (r *StoreConnector) insertHealthEvents(
	ctx context.Context,
	healthEvents *platformconnector.HealthEvents,
) error {
	// Get the health event store interface
	healthEventStore := r.dataStore.HealthEventStore()
	if healthEventStore == nil {
		return fmt.Errorf("health event store is not available")
	}

	klog.V(3).Infof("Processing %d health events for storage", len(healthEvents.GetEvents()))

	// Process each health event individually
	for i, healthEvent := range healthEvents.GetEvents() {
		klog.V(4).Infof("Processing health event %d/%d: Node=%s, Agent=%s, CheckName=%s, IsFatal=%v",
			i+1, len(healthEvents.GetEvents()),
			healthEvent.GetNodeName(),
			healthEvent.GetAgent(),
			healthEvent.GetCheckName(),
			healthEvent.GetIsFatal())

		// Initialize health event status with default values
		unquarantined := datastore.UnQuarantined
		healthEventStatus := datastore.HealthEventStatus{
			NodeQuarantined: &unquarantined,
			UserPodsEvictionStatus: datastore.OperationStatus{
				Status:  "pending",
				Message: "Health event received, processing pending",
			},
			FaultRemediated: new(bool), // Initialize to false
		}

		// Create health event with status
		healthEventWithStatus := &datastore.HealthEventWithStatus{
			CreatedAt:         time.Now().UTC(),
			HealthEvent:       healthEvent,
			HealthEventStatus: healthEventStatus,
		}

		// Insert individual health event using the correct method
		err := healthEventStore.InsertHealthEvents(ctx, healthEventWithStatus)
		if err != nil {
			return fmt.Errorf("failed to insert health event for node %s: %w",
				healthEvent.GetNodeName(), err)
		}

		klog.V(2).Infof("Inserted health event: Node=%s, Agent=%s, CheckName=%s, Fatal=%v",
			healthEvent.GetNodeName(), healthEvent.GetAgent(), healthEvent.GetCheckName(), healthEvent.GetIsFatal())
	}

	klog.V(2).Infof("Successfully processed and stored %d health events", len(healthEvents.GetEvents()))

	return nil
}

func GenerateRandomObjectID() string {
	return datastore.GenerateID()
}

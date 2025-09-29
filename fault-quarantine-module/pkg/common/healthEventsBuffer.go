// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/buffer"
	sdkcommon "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/common"
)

// HealthEventInfo represents information about a health event
// This is now an alias to the generic buffer's EventInfo
type HealthEventInfo = buffer.EventInfo[*datastore.HealthEventWithStatus]

// HealthEventBuffer is a wrapper around the generic EventBuffer
// that maintains backward compatibility with fault-quarantine's existing API
type HealthEventBuffer struct {
	buffer *buffer.EventBuffer[*datastore.HealthEventWithStatus]
	ctx    context.Context // Kept for backward compatibility, though not used
}

// healthEventKeyExtractor extracts the node name from a health event for deduplication
func healthEventKeyExtractor(event *datastore.HealthEventWithStatus) (string, error) {
	healthEvent, err := sdkcommon.ExtractPlatformConnectorsHealthEvent(event)
	if err != nil {
		return "", err
	}

	return healthEvent.NodeName, nil
}

// NewHealthEventBuffer creates a new health event buffer.
func NewHealthEventBuffer(ctx context.Context) *HealthEventBuffer {
	return &HealthEventBuffer{
		buffer: buffer.NewEventBuffer(0, healthEventKeyExtractor), // 0 = use default size (1000)
		ctx:    ctx,
	}
}

// Add adds a health event at the end of the buffer.
// De-duplicates: if node already in buffer, updates event instead of adding duplicate
// Returns true if added/updated, false if buffer is full
func (b *HealthEventBuffer) Add(
	event *datastore.HealthEventWithStatus,
	eventData map[string]interface{},
	resumeToken []byte,
) bool {
	return b.buffer.Add(event, eventData, resumeToken)
}

// RemoveAt removes an element at the specified index.
func (b *HealthEventBuffer) RemoveAt(index int) error {
	return b.buffer.RemoveAt(index)
}

// Length returns the current number of elements in the buffer.
func (b *HealthEventBuffer) Length() int {
	return b.buffer.Length()
}

// Get returns the element at the specified index without removing it.
func (b *HealthEventBuffer) Get(index int) (*HealthEventInfo, error) {
	info := b.buffer.Get(index)
	if info == nil {
		return nil, nil
	}

	return info, nil
}

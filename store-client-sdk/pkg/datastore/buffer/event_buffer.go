// Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
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

package buffer

import (
	"fmt"
	"sync"
)

// EventInfo wraps an event with associated metadata for change stream processing
type EventInfo[T any] struct {
	// Event is the main event data
	Event T

	// RawEvent is the raw change stream event data
	RawEvent map[string]interface{}

	// ResumeToken is the change stream resume token captured with this event
	ResumeToken []byte

	// Key is the cached deduplication key (e.g., node name)
	Key string

	// HasProcessed indicates if this event has been processed (for retry logic)
	HasProcessed bool
}

// KeyExtractor is a function that extracts a deduplication key from an event
type KeyExtractor[T any] func(event T) (string, error)

// EventBuffer is a generic thread-safe buffer for change stream events
// with node-based deduplication and overflow protection
type EventBuffer[T any] struct {
	events       []EventInfo[T]
	mu           sync.RWMutex
	maxSize      int
	dropped      int
	dedupIndex   map[string]int // Maps dedup key to array index
	keyExtractor KeyExtractor[T]
}

const (
	// DefaultMaxBufferSize prevents unbounded memory growth
	// Set to 1000 to handle scale scenarios with multiple change stream events per health event
	DefaultMaxBufferSize = 1000
)

// NewEventBuffer creates a new event buffer with the specified configuration
// maxSize: Maximum number of events to buffer (0 = use DefaultMaxBufferSize)
// keyExtractor: Function to extract deduplication key from event
func NewEventBuffer[T any](maxSize int, keyExtractor KeyExtractor[T]) *EventBuffer[T] {
	if maxSize <= 0 {
		maxSize = DefaultMaxBufferSize
	}

	return &EventBuffer[T]{
		events:       make([]EventInfo[T], 0, maxSize),
		maxSize:      maxSize,
		dropped:      0,
		dedupIndex:   make(map[string]int, maxSize),
		keyExtractor: keyExtractor,
	}
}

// Add adds an event to the buffer with deduplication
// If an event with the same key exists, it updates the existing event
// Returns true if added/updated successfully, false if buffer is full
func (b *EventBuffer[T]) Add(event T, rawEvent map[string]interface{}, resumeToken []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Extract deduplication key
	key, err := b.keyExtractor(event)
	if err != nil {
		// If key extraction fails, treat as unique event
		key = fmt.Sprintf("unknown-%d", len(b.events))
	}

	// Check if event with this key already exists (deduplication)
	if existingIdx, exists := b.dedupIndex[key]; exists {
		// Update existing event with latest data
		b.events[existingIdx] = EventInfo[T]{
			Event:        event,
			RawEvent:     rawEvent,
			ResumeToken:  resumeToken,
			Key:          key,
			HasProcessed: false,
		}

		return true
	}

	// Check if buffer is full
	if len(b.events) >= b.maxSize {
		b.dropped++
		return false
	}

	// Add new event
	b.dedupIndex[key] = len(b.events)
	b.events = append(b.events, EventInfo[T]{
		Event:        event,
		RawEvent:     rawEvent,
		ResumeToken:  resumeToken,
		Key:          key,
		HasProcessed: false,
	})

	return true
}

// Get returns the event at the specified index without removing it
// Returns nil if index is out of bounds
func (b *EventBuffer[T]) Get(index int) *EventInfo[T] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if index < 0 || index >= len(b.events) {
		return nil
	}

	return &b.events[index]
}

// RemoveAt removes the event at the specified index
// This rebuilds the deduplication index since indices shift
func (b *EventBuffer[T]) RemoveAt(index int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= len(b.events) {
		return fmt.Errorf("index out of bounds: %d (buffer size: %d)", index, len(b.events))
	}

	// Remove from deduplication map
	delete(b.dedupIndex, b.events[index].Key)

	// Remove the element at index
	b.events = append(b.events[:index], b.events[index+1:]...)

	// Rebuild deduplication index with updated indices
	b.dedupIndex = make(map[string]int, len(b.events))
	for i, event := range b.events {
		b.dedupIndex[event.Key] = i
	}

	return nil
}

// GetAll retrieves all events and clears the buffer atomically
// This is useful for batch processing where you want to drain the buffer
func (b *EventBuffer[T]) GetAll() []EventInfo[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.events) == 0 {
		return nil
	}

	// Copy events and clear buffer
	events := make([]EventInfo[T], len(b.events))
	copy(events, b.events)

	// Clear buffer
	b.events = b.events[:0]
	b.dedupIndex = make(map[string]int, b.maxSize)

	return events
}

// Length returns the current number of events in the buffer
func (b *EventBuffer[T]) Length() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.events)
}

// Clear removes all events from the buffer
func (b *EventBuffer[T]) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.events = b.events[:0]
	b.dedupIndex = make(map[string]int, b.maxSize)
}

// GetDroppedCount returns the number of events dropped due to buffer full
func (b *EventBuffer[T]) GetDroppedCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.dropped
}

// ResetDroppedCount resets the dropped events counter
func (b *EventBuffer[T]) ResetDroppedCount() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dropped = 0
}

// MarkProcessed marks the event at the specified index as processed
// This is useful for retry logic where you want to track which events have been attempted
func (b *EventBuffer[T]) MarkProcessed(index int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= len(b.events) {
		return fmt.Errorf("index out of bounds: %d (buffer size: %d)", index, len(b.events))
	}

	b.events[index].HasProcessed = true

	return nil
}

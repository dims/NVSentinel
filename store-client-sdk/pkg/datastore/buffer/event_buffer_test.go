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
	"testing"
)

// testEvent is a simple event type for testing
type testEvent struct {
	NodeName string
	Value    int
}

func testKeyExtractor(event testEvent) (string, error) {
	return event.NodeName, nil
}

func TestNewEventBuffer(t *testing.T) {
	t.Run("default size", func(t *testing.T) {
		buf := NewEventBuffer(0, testKeyExtractor)
		if buf.maxSize != DefaultMaxBufferSize {
			t.Errorf("Expected maxSize=%d, got %d", DefaultMaxBufferSize, buf.maxSize)
		}
	})

	t.Run("custom size", func(t *testing.T) {
		buf := NewEventBuffer(500, testKeyExtractor)
		if buf.maxSize != 500 {
			t.Errorf("Expected maxSize=500, got %d", buf.maxSize)
		}
	})
}

func TestEventBuffer_Add(t *testing.T) {
	t.Run("add single event", func(t *testing.T) {
		buf := NewEventBuffer(10, testKeyExtractor)
		event := testEvent{NodeName: "node1", Value: 1}

		ok := buf.Add(event, map[string]interface{}{"test": "data"}, []byte("token1"))
		if !ok {
			t.Error("Expected Add to succeed")
		}

		if buf.Length() != 1 {
			t.Errorf("Expected length=1, got %d", buf.Length())
		}
	})

	t.Run("add multiple unique events", func(t *testing.T) {
		buf := NewEventBuffer(10, testKeyExtractor)

		for i := 0; i < 5; i++ {
			event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
			ok := buf.Add(event, nil, nil)
			if !ok {
				t.Errorf("Expected Add to succeed for event %d", i)
			}
		}

		if buf.Length() != 5 {
			t.Errorf("Expected length=5, got %d", buf.Length())
		}
	})
}

func TestEventBuffer_Deduplication(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add initial event
	event1 := testEvent{NodeName: "node1", Value: 1}
	buf.Add(event1, map[string]interface{}{"version": 1}, []byte("token1"))

	if buf.Length() != 1 {
		t.Errorf("Expected length=1 after first add, got %d", buf.Length())
	}

	// Add event with same key (should update, not add)
	event2 := testEvent{NodeName: "node1", Value: 2}
	buf.Add(event2, map[string]interface{}{"version": 2}, []byte("token2"))

	if buf.Length() != 1 {
		t.Errorf("Expected length=1 after dedup, got %d", buf.Length())
	}

	// Verify the event was updated
	info := buf.Get(0)
	if info.Event.Value != 2 {
		t.Errorf("Expected updated value=2, got %d", info.Event.Value)
	}
	if string(info.ResumeToken) != "token2" {
		t.Errorf("Expected updated token=token2, got %s", string(info.ResumeToken))
	}
}

func TestEventBuffer_BufferFull(t *testing.T) {
	buf := NewEventBuffer(3, testKeyExtractor)

	// Fill buffer
	for i := 0; i < 3; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		ok := buf.Add(event, nil, nil)
		if !ok {
			t.Errorf("Expected Add to succeed for event %d", i)
		}
	}

	// Try to add when full
	event := testEvent{NodeName: "node99", Value: 99}
	ok := buf.Add(event, nil, nil)
	if ok {
		t.Error("Expected Add to fail when buffer is full")
	}

	// Verify dropped count
	if buf.GetDroppedCount() != 1 {
		t.Errorf("Expected dropped count=1, got %d", buf.GetDroppedCount())
	}

	// Verify buffer size didn't change
	if buf.Length() != 3 {
		t.Errorf("Expected length=3, got %d", buf.Length())
	}
}

func TestEventBuffer_Get(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add some events
	for i := 0; i < 3; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		buf.Add(event, map[string]interface{}{"index": i}, []byte(fmt.Sprintf("token%d", i)))
	}

	// Get valid index
	info := buf.Get(1)
	if info == nil {
		t.Fatal("Expected Get(1) to return event")
	}
	if info.Event.NodeName != "node1" {
		t.Errorf("Expected NodeName=node1, got %s", info.Event.NodeName)
	}

	// Get out of bounds
	info = buf.Get(99)
	if info != nil {
		t.Error("Expected Get(99) to return nil for out of bounds")
	}

	// Get negative index
	info = buf.Get(-1)
	if info != nil {
		t.Error("Expected Get(-1) to return nil for negative index")
	}
}

func TestEventBuffer_RemoveAt(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add some events
	for i := 0; i < 5; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		buf.Add(event, nil, nil)
	}

	// Remove middle element
	err := buf.RemoveAt(2)
	if err != nil {
		t.Errorf("Expected RemoveAt(2) to succeed, got error: %v", err)
	}

	if buf.Length() != 4 {
		t.Errorf("Expected length=4 after removal, got %d", buf.Length())
	}

	// Verify node2 is gone
	info := buf.Get(2)
	if info.Event.NodeName == "node2" {
		t.Error("Expected node2 to be removed")
	}

	// Verify dedup index is rebuilt (try to add node3 again)
	event := testEvent{NodeName: "node3", Value: 999}
	buf.Add(event, nil, nil)

	// Should update existing node3, not add new one
	if buf.Length() != 4 {
		t.Errorf("Expected length=4 after dedup update, got %d", buf.Length())
	}
}

func TestEventBuffer_GetAll(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add some events
	for i := 0; i < 5; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		buf.Add(event, nil, []byte(fmt.Sprintf("token%d", i)))
	}

	// GetAll should return all events and clear buffer
	events := buf.GetAll()

	if len(events) != 5 {
		t.Errorf("Expected 5 events, got %d", len(events))
	}

	if buf.Length() != 0 {
		t.Errorf("Expected buffer to be empty after GetAll, got length=%d", buf.Length())
	}

	// Verify we can add new events after GetAll
	event := testEvent{NodeName: "new-node", Value: 100}
	ok := buf.Add(event, nil, nil)
	if !ok {
		t.Error("Expected Add to succeed after GetAll")
	}
}

func TestEventBuffer_Clear(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add some events
	for i := 0; i < 5; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		buf.Add(event, nil, nil)
	}

	buf.Clear()

	if buf.Length() != 0 {
		t.Errorf("Expected length=0 after Clear, got %d", buf.Length())
	}

	// Verify we can add events after Clear
	event := testEvent{NodeName: "node0", Value: 999}
	ok := buf.Add(event, nil, nil)
	if !ok {
		t.Error("Expected Add to succeed after Clear")
	}
}

func TestEventBuffer_DroppedCount(t *testing.T) {
	buf := NewEventBuffer(2, testKeyExtractor)

	// Fill buffer
	buf.Add(testEvent{NodeName: "node1", Value: 1}, nil, nil)
	buf.Add(testEvent{NodeName: "node2", Value: 2}, nil, nil)

	// Try to add more (should be dropped)
	buf.Add(testEvent{NodeName: "node3", Value: 3}, nil, nil)
	buf.Add(testEvent{NodeName: "node4", Value: 4}, nil, nil)

	if buf.GetDroppedCount() != 2 {
		t.Errorf("Expected dropped count=2, got %d", buf.GetDroppedCount())
	}

	// Reset counter
	buf.ResetDroppedCount()

	if buf.GetDroppedCount() != 0 {
		t.Errorf("Expected dropped count=0 after reset, got %d", buf.GetDroppedCount())
	}
}

func TestEventBuffer_MarkProcessed(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add some events
	for i := 0; i < 3; i++ {
		event := testEvent{NodeName: fmt.Sprintf("node%d", i), Value: i}
		buf.Add(event, nil, nil)
	}

	// Mark first event as processed
	err := buf.MarkProcessed(0)
	if err != nil {
		t.Errorf("Expected MarkProcessed(0) to succeed, got error: %v", err)
	}

	// Verify it was marked
	info := buf.Get(0)
	if !info.HasProcessed {
		t.Error("Expected HasProcessed=true after marking")
	}

	// Try to mark out of bounds
	err = buf.MarkProcessed(99)
	if err == nil {
		t.Error("Expected MarkProcessed(99) to fail")
	}
}

func TestEventBuffer_ThreadSafety(t *testing.T) {
	buf := NewEventBuffer(1000, testKeyExtractor)

	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				event := testEvent{
					NodeName: fmt.Sprintf("node%d-%d", idx, j),
					Value:    j,
				}
				buf.Add(event, nil, nil)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = buf.Length()
				_ = buf.Get(0)
			}
		}()
	}

	wg.Wait()

	// Verify final state is consistent
	length := buf.Length()
	if length <= 0 || length > 1000 {
		t.Errorf("Expected length between 1 and 1000, got %d", length)
	}
}

func TestEventBuffer_GetAllWithDeduplication(t *testing.T) {
	buf := NewEventBuffer(10, testKeyExtractor)

	// Add events, some with duplicate keys
	buf.Add(testEvent{NodeName: "node1", Value: 1}, nil, nil)
	buf.Add(testEvent{NodeName: "node2", Value: 2}, nil, nil)
	buf.Add(testEvent{NodeName: "node1", Value: 3}, nil, nil) // Dedup: updates node1
	buf.Add(testEvent{NodeName: "node3", Value: 4}, nil, nil)

	// Should have 3 unique events
	if buf.Length() != 3 {
		t.Errorf("Expected length=3, got %d", buf.Length())
	}

	events := buf.GetAll()

	// Verify GetAll returns correct number
	if len(events) != 3 {
		t.Errorf("Expected 3 events from GetAll, got %d", len(events))
	}

	// Verify node1 has updated value
	foundNode1 := false
	for _, e := range events {
		if e.Event.NodeName == "node1" {
			foundNode1 = true
			if e.Event.Value != 3 {
				t.Errorf("Expected node1 value=3 (updated), got %d", e.Event.Value)
			}
		}
	}

	if !foundNode1 {
		t.Error("Expected to find node1 in GetAll results")
	}
}

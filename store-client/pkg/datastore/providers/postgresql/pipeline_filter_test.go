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

package postgresql

import (
	"testing"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPipelineFilter_NilPipeline(t *testing.T) {
	filter, err := NewPipelineFilter(nil)
	assert.NoError(t, err)
	assert.Nil(t, filter)
}

func TestNewPipelineFilter_EmptyPipeline(t *testing.T) {
	pipeline := []interface{}{}
	filter, err := NewPipelineFilter(pipeline)
	assert.NoError(t, err)
	assert.Nil(t, filter)
}

func TestNewPipelineFilter_SimpleMatchStage(t *testing.T) {
	// Create pipeline with simple $match stage
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "insert",
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)
	require.NotNil(t, filter)
	assert.Equal(t, 1, len(filter.stages))
}

func TestPipelineFilter_MatchesEvent_OperationType(t *testing.T) {
	// Create filter that matches insert operations
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "insert",
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		event    datastore.EventWithToken
		expected bool
	}{
		{
			name: "matches insert operation",
			event: datastore.EventWithToken{
				Event: map[string]interface{}{
					"operationType": "insert",
				},
			},
			expected: true,
		},
		{
			name: "doesn't match update operation",
			event: datastore.EventWithToken{
				Event: map[string]interface{}{
					"operationType": "update",
				},
			},
			expected: false,
		},
		{
			name: "doesn't match delete operation",
			event: datastore.EventWithToken{
				Event: map[string]interface{}{
					"operationType": "delete",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filter.MatchesEvent(tt.event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_MatchesEvent_NestedField(t *testing.T) {
	// Create filter that matches nested field path
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"fullDocument.healthevent.isfatal": true,
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		event    datastore.EventWithToken
		expected bool
	}{
		{
			name: "matches fatal event",
			event: datastore.EventWithToken{
				Event: map[string]interface{}{
					"fullDocument": map[string]interface{}{
						"healthevent": map[string]interface{}{
							"isfatal": true,
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "doesn't match non-fatal event",
			event: datastore.EventWithToken{
				Event: map[string]interface{}{
					"fullDocument": map[string]interface{}{
						"healthevent": map[string]interface{}{
							"isfatal": false,
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filter.MatchesEvent(tt.event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_MatchesEvent_InOperator(t *testing.T) {
	// Create filter with $in operator
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": map[string]interface{}{
					"$in": []interface{}{"insert", "update"},
				},
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		opType   string
		expected bool
	}{
		{"matches insert", "insert", true},
		{"matches update", "update", true},
		{"doesn't match delete", "delete", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := datastore.EventWithToken{
				Event: map[string]interface{}{
					"operationType": tt.opType,
				},
			}
			result := filter.MatchesEvent(event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_MatchesEvent_NeOperator(t *testing.T) {
	// Create filter with $ne operator
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"fullDocument.healthevent.agent": map[string]interface{}{
					"$ne": "health-events-analyzer",
				},
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		agent    string
		expected bool
	}{
		{"matches different agent", "fault-quarantine", true},
		{"doesn't match excluded agent", "health-events-analyzer", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := datastore.EventWithToken{
				Event: map[string]interface{}{
					"fullDocument": map[string]interface{}{
						"healthevent": map[string]interface{}{
							"agent": tt.agent,
						},
					},
				},
			}
			result := filter.MatchesEvent(event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_MatchesEvent_OrOperator(t *testing.T) {
	// Create filter with $or operator
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{
						"operationType": "insert",
					},
					map[string]interface{}{
						"operationType": "update",
					},
				},
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		opType   string
		expected bool
	}{
		{"matches insert via $or", "insert", true},
		{"matches update via $or", "update", true},
		{"doesn't match delete", "delete", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := datastore.EventWithToken{
				Event: map[string]interface{}{
					"operationType": tt.opType,
				},
			}
			result := filter.MatchesEvent(event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_MatchesEvent_ComplexOr(t *testing.T) {
	// Create filter similar to BuildQuarantinedAndDrainedNodesPipeline
	pipeline := []interface{}{
		map[string]interface{}{
			"$match": map[string]interface{}{
				"operationType": "update",
				"$or": []interface{}{
					map[string]interface{}{
						"fullDocument.healtheventstatus.userpodsevictionstatus.status": map[string]interface{}{
							"$in": []interface{}{"Succeeded", "AlreadyDrained"},
						},
						"fullDocument.healtheventstatus.nodequarantined": map[string]interface{}{
							"$in": []interface{}{"Quarantined", "AlreadyQuarantined"},
						},
					},
					map[string]interface{}{
						"fullDocument.healtheventstatus.nodequarantined": "UnQuarantined",
					},
				},
			},
		},
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)

	tests := []struct {
		name     string
		event    map[string]interface{}
		expected bool
	}{
		{
			name: "matches quarantined and drained",
			event: map[string]interface{}{
				"operationType": "update",
				"fullDocument": map[string]interface{}{
					"healtheventstatus": map[string]interface{}{
						"userpodsevictionstatus": map[string]interface{}{
							"status": "Succeeded",
						},
						"nodequarantined": "Quarantined",
					},
				},
			},
			expected: true,
		},
		{
			name: "matches unquarantined",
			event: map[string]interface{}{
				"operationType": "update",
				"fullDocument": map[string]interface{}{
					"healtheventstatus": map[string]interface{}{
						"nodequarantined": "UnQuarantined",
					},
				},
			},
			expected: true,
		},
		{
			name: "doesn't match insert operation",
			event: map[string]interface{}{
				"operationType": "insert",
				"fullDocument": map[string]interface{}{
					"healtheventstatus": map[string]interface{}{
						"nodequarantined": "Quarantined",
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventWithToken := datastore.EventWithToken{
				Event: tt.event,
			}
			result := filter.MatchesEvent(eventWithToken)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPipelineFilter_WithDatastoreTypes(t *testing.T) {
	// Create pipeline using datastore.D and datastore.A types
	pipeline := []interface{}{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", datastore.D(
					datastore.E("$in", datastore.A("insert")),
				)),
			)),
		),
	}

	filter, err := NewPipelineFilter(pipeline)
	require.NoError(t, err)
	require.NotNil(t, filter)

	event := datastore.EventWithToken{
		Event: map[string]interface{}{
			"operationType": "insert",
		},
	}

	result := filter.MatchesEvent(event)
	assert.True(t, result)
}

func TestPipelineFilter_NilFilter_AllEventsMatch(t *testing.T) {
	var filter *PipelineFilter

	event := datastore.EventWithToken{
		Event: map[string]interface{}{
			"operationType": "anything",
		},
	}

	// Nil filter should match all events
	result := filter.MatchesEvent(event)
	assert.True(t, result)
}

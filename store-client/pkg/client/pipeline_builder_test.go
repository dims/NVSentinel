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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMongoDBPipelineBuilder_NodeQuarantineStatusPipeline(t *testing.T) {
	builder := NewMongoDBPipelineBuilder()
	pipeline := builder.BuildNodeQuarantineStatusPipeline()

	// Verify pipeline structure exists
	require.NotNil(t, pipeline)
	require.NotEmpty(t, pipeline)

	// Verify it has a $match stage with operationType=update
	// The actual filter testing is done in integration tests to avoid import cycles
	assert.Len(t, pipeline, 1, "Pipeline should have 1 stage")
}

func TestAllHealthEventInsertsPipeline(t *testing.T) {
	builder := NewMongoDBPipelineBuilder()
	pipeline := builder.BuildAllHealthEventInsertsPipeline()

	require.NotNil(t, pipeline)
	require.NotEmpty(t, pipeline)
	assert.Len(t, pipeline, 1, "Pipeline should have 1 stage")
}

func TestNonFatalUnhealthyInsertsPipeline(t *testing.T) {
	builder := NewMongoDBPipelineBuilder()
	pipeline := builder.BuildNonFatalUnhealthyInsertsPipeline()

	require.NotNil(t, pipeline)
	require.NotEmpty(t, pipeline)
	assert.Len(t, pipeline, 1, "Pipeline should have 1 stage")
}

func TestQuarantinedAndDrainedNodesPipeline(t *testing.T) {
	builder := NewMongoDBPipelineBuilder()
	pipeline := builder.BuildQuarantinedAndDrainedNodesPipeline()

	require.NotNil(t, pipeline)
	require.NotEmpty(t, pipeline)
	assert.Len(t, pipeline, 1, "Pipeline should have 1 stage")
}

func TestGetPipelineBuilder(t *testing.T) {
	// Test that GetPipelineBuilder returns a MongoDB builder (default)
	builder := GetPipelineBuilder()
	require.NotNil(t, builder)

	// Verify it returns a MongoDBPipelineBuilder
	_, ok := builder.(*MongoDBPipelineBuilder)
	assert.True(t, ok, "Default builder should be MongoDBPipelineBuilder")
}

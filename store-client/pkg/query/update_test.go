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

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateBuilder_Set_SimpleField(t *testing.T) {
	update := NewUpdate().Set("status", "active")

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	expectedMongo := map[string]interface{}{
		"$set": map[string]interface{}{
			"status": "active",
		},
	}
	assert.Equal(t, expectedMongo, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "document = jsonb_set(document, '{status}', $1::jsonb)", sql)
	assert.Equal(t, []interface{}{"\"active\""}, args)
}

func TestUpdateBuilder_Set_NestedField(t *testing.T) {
	update := NewUpdate().Set("healtheventstatus.nodequarantined", "Quarantined")

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	expectedMongo := map[string]interface{}{
		"$set": map[string]interface{}{
			"healtheventstatus.nodequarantined": "Quarantined",
		},
	}
	assert.Equal(t, expectedMongo, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "document = jsonb_set(document, '{healtheventstatus,nodequarantined}', $1::jsonb)", sql)
	assert.Equal(t, []interface{}{"\"Quarantined\""}, args)
}

func TestUpdateBuilder_Set_ColumnField(t *testing.T) {
	update := NewUpdate().Set("updatedAt", "2025-01-15T10:00:00Z")

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	expectedMongo := map[string]interface{}{
		"$set": map[string]interface{}{
			"updatedAt": "2025-01-15T10:00:00Z",
		},
	}
	assert.Equal(t, expectedMongo, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "updatedAt = $1", sql)
	assert.Equal(t, []interface{}{"2025-01-15T10:00:00Z"}, args)
}

func TestUpdateBuilder_SetMultiple_Fields(t *testing.T) {
	update := NewUpdate().
		Set("status", "active").
		Set("type", "critical")

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	setMap := mongoUpdate["$set"].(map[string]interface{})
	assert.Len(t, setMap, 2)
	assert.Equal(t, "active", setMap["status"])
	assert.Equal(t, "critical", setMap["type"])

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Contains(t, sql, "document = jsonb_set(document, '{status}', $1::jsonb)")
	assert.Contains(t, sql, "document = jsonb_set(document, '{type}', $2::jsonb)")
	assert.Len(t, args, 2)
}

func TestUpdateBuilder_SetMultiple_Map(t *testing.T) {
	updates := map[string]interface{}{
		"status": "active",
		"count":  10,
	}
	update := NewUpdate().SetMultiple(updates)

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	setMap := mongoUpdate["$set"].(map[string]interface{})
	assert.Len(t, setMap, 2)
	assert.Equal(t, "active", setMap["status"])
	assert.Equal(t, 10, setMap["count"])

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Contains(t, sql, "document = jsonb_set(document")
	assert.Len(t, args, 2)
}

func TestUpdateBuilder_Set_DeeplyNestedField(t *testing.T) {
	update := NewUpdate().Set("healtheventstatus.userpodsevictionstatus.status", "InProgress")

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	expectedMongo := map[string]interface{}{
		"$set": map[string]interface{}{
			"healtheventstatus.userpodsevictionstatus.status": "InProgress",
		},
	}
	assert.Equal(t, expectedMongo, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "document = jsonb_set(document, '{healtheventstatus,userpodsevictionstatus,status}', $1::jsonb)", sql)
	assert.Equal(t, []interface{}{"\"InProgress\""}, args)
}

func TestUpdateBuilder_EmptyUpdate(t *testing.T) {
	update := NewUpdate()

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	assert.Equal(t, map[string]interface{}{}, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "", sql)
	assert.Nil(t, args)
}

func TestUpdateBuilder_NilUpdate(t *testing.T) {
	var update *UpdateBuilder

	// Test MongoDB output
	mongoUpdate := update.ToMongo()
	assert.Equal(t, map[string]interface{}{}, mongoUpdate)

	// Test SQL output
	sql, args := update.ToSQL()
	assert.Equal(t, "", sql)
	assert.Nil(t, args)
}

func TestToJSONBValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "string value",
			value:    "active",
			expected: "\"active\"",
		},
		{
			name:     "boolean true",
			value:    true,
			expected: "true",
		},
		{
			name:     "boolean false",
			value:    false,
			expected: "false",
		},
		{
			name:     "integer",
			value:    42,
			expected: "42",
		},
		{
			name:     "float",
			value:    3.14,
			expected: "3.140000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toJSONBValue(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMongoFieldToJSONBPath(t *testing.T) {
	tests := []struct {
		name         string
		mongoField   string
		expectedPath string
	}{
		{
			name:         "simple field",
			mongoField:   "status",
			expectedPath: "status",
		},
		{
			name:         "two levels",
			mongoField:   "healthevent.isfatal",
			expectedPath: "healthevent,isfatal",
		},
		{
			name:         "three levels",
			mongoField:   "healtheventstatus.userpodsevictionstatus.status",
			expectedPath: "healtheventstatus,userpodsevictionstatus,status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mongoFieldToJSONBPath(tt.mongoField)
			assert.Equal(t, tt.expectedPath, result)
		})
	}
}

func TestUpdateBuilder_RealWorldUpdates(t *testing.T) {
	t.Run("node-drainer status update", func(t *testing.T) {
		update := NewUpdate().Set("healtheventstatus.userpodsevictionstatus.status", "Succeeded")

		// MongoDB update should work
		mongoUpdate := update.ToMongo()
		require.NotNil(t, mongoUpdate)
		assert.Contains(t, mongoUpdate, "$set")

		// SQL should generate correctly
		sql, args := update.ToSQL()
		assert.Contains(t, sql, "jsonb_set")
		assert.Contains(t, sql, "healtheventstatus,userpodsevictionstatus,status")
		assert.Len(t, args, 1)
	})

	t.Run("fault-quarantine cancellation", func(t *testing.T) {
		update := NewUpdate().Set("healtheventstatus.nodequarantined", "Cancelled")

		// MongoDB update should work
		mongoUpdate := update.ToMongo()
		require.NotNil(t, mongoUpdate)
		assert.Contains(t, mongoUpdate, "$set")

		// SQL should generate correctly
		sql, args := update.ToSQL()
		assert.Contains(t, sql, "jsonb_set")
		assert.Contains(t, sql, "healtheventstatus,nodequarantined")
		assert.Equal(t, []interface{}{"\"Cancelled\""}, args)
	})

	t.Run("multiple field update", func(t *testing.T) {
		update := NewUpdate().
			Set("healtheventstatus.nodequarantined", "Quarantined").
			Set("healtheventstatus.userpodsevictionstatus.status", "InProgress")

		// MongoDB update should work
		mongoUpdate := update.ToMongo()
		require.NotNil(t, mongoUpdate)
		setMap := mongoUpdate["$set"].(map[string]interface{})
		assert.Len(t, setMap, 2)

		// SQL should generate correctly
		sql, args := update.ToSQL()
		assert.Contains(t, sql, "jsonb_set")
		assert.Len(t, args, 2)
	})
}

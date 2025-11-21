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
	"database/sql"
	"strconv"
	"testing"
	"time"
)

func TestPostgreSQLEventAdapter_GetDocumentID(t *testing.T) {
	tests := []struct {
		name        string
		eventData   map[string]interface{}
		want        string
		wantErr     bool
		description string
	}{
		{
			name: "returns changelog ID from _id._data",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
			},
			want:        "136",
			wantErr:     false,
			description: "Should return changelog ID as string",
		},
		{
			name: "returns int-parseable value",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "12345",
				},
			},
			want:        "12345",
			wantErr:     false,
			description: "Value should be parseable as integer",
		},
		{
			name: "does NOT return UUID from fullDocument",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
				"fullDocument": map[string]interface{}{
					"id": "6d4e36e4-b9d2-473b-a290-3ed7fb99073e",
				},
			},
			want:        "136",
			wantErr:     false,
			description: "Should return _id._data, not fullDocument.id",
		},
		{
			name: "handles numeric _data value",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": 999,
				},
			},
			want:        "999",
			wantErr:     false,
			description: "Should handle numeric values in _data",
		},
		{
			name: "returns error when _id not present",
			eventData: map[string]interface{}{
				"fullDocument": map[string]interface{}{
					"id": "6d4e36e4-b9d2-473b-a290-3ed7fb99073e",
				},
			},
			want:        "",
			wantErr:     true,
			description: "Should error when _id is missing",
		},
		{
			name: "uses _id directly if _data not present",
			eventData: map[string]interface{}{
				"_id": "789",
			},
			want:        "789",
			wantErr:     false,
			description: "Should fallback to _id directly if _data not present",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &PostgreSQLEventAdapter{
				eventData: tt.eventData,
			}

			got, err := adapter.GetDocumentID()

			if (err != nil) != tt.wantErr {
				t.Errorf("GetDocumentID() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("GetDocumentID() = %v, want %v", got, tt.want)
			}

			// Verify it's int-parseable for non-error cases
			if !tt.wantErr && got != "" {
				_, parseErr := strconv.ParseInt(got, 10, 64)
				if parseErr != nil {
					t.Errorf("GetDocumentID() returned non-int-parseable value: %v, error: %v", got, parseErr)
				}
			}
		})
	}
}

func TestPostgreSQLEventAdapter_GetRecordUUID(t *testing.T) {
	tests := []struct {
		name      string
		eventData map[string]interface{}
		want      string
		wantErr   bool
	}{
		{
			name: "returns UUID from fullDocument.id",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
				"fullDocument": map[string]interface{}{
					"id": "6d4e36e4-b9d2-473b-a290-3ed7fb99073e",
				},
			},
			want:    "6d4e36e4-b9d2-473b-a290-3ed7fb99073e",
			wantErr: false,
		},
		{
			name: "returns UUID from fullDocument._id (MongoDB compatibility)",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
				"fullDocument": map[string]interface{}{
					"_id": "507f1f77bcf86cd799439011",
				},
			},
			want:    "507f1f77bcf86cd799439011",
			wantErr: false,
		},
		{
			name: "errors when fullDocument not present",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "errors when id not in fullDocument",
			eventData: map[string]interface{}{
				"_id": map[string]interface{}{
					"_data": "136",
				},
				"fullDocument": map[string]interface{}{
					"name": "test",
				},
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &PostgreSQLEventAdapter{
				eventData: tt.eventData,
			}

			got, err := adapter.GetRecordUUID()

			if (err != nil) != tt.wantErr {
				t.Errorf("GetRecordUUID() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("GetRecordUUID() = %v, want %v", got, tt.want)
			}

			// Verify UUID format for successful cases
			if !tt.wantErr && len(got) > 0 {
				// UUID should be at least 32 chars (without hyphens) or 36 (with hyphens)
				if len(got) < 24 {
					t.Errorf("GetRecordUUID() returned suspiciously short ID: %v (length %d)", got, len(got))
				}
			}
		})
	}
}

func TestPostgreSQLEventAdapter_BothMethods(t *testing.T) {
	// Test that both methods return different values as expected
	adapter := &PostgreSQLEventAdapter{
		eventData: map[string]interface{}{
			"_id": map[string]interface{}{
				"_data": "136",
			},
			"fullDocument": map[string]interface{}{
				"id": "6d4e36e4-b9d2-473b-a290-3ed7fb99073e",
			},
		},
	}

	docID, err := adapter.GetDocumentID()
	if err != nil {
		t.Fatalf("GetDocumentID() unexpected error: %v", err)
	}

	uuid, err := adapter.GetRecordUUID()
	if err != nil {
		t.Fatalf("GetRecordUUID() unexpected error: %v", err)
	}

	// They should be different
	if docID == uuid {
		t.Errorf("GetDocumentID() and GetRecordUUID() returned same value: %v", docID)
	}

	// Document ID should be int-parseable
	_, err = strconv.ParseInt(docID, 10, 64)
	if err != nil {
		t.Errorf("GetDocumentID() not int-parseable: %v, error: %v", docID, err)
	}

	// UUID should be longer
	if len(uuid) <= len(docID) {
		t.Errorf("GetRecordUUID() should return longer UUID than GetDocumentID(), got UUID=%s (len %d) vs docID=%s (len %d)",
			uuid, len(uuid), docID, len(docID))
	}
}

func TestBuildEventDocument(t *testing.T) {
	watcher := &PostgreSQLChangeStreamWatcher{}

	var emptyOld, emptyNew sql.NullString

	event := watcher.buildEventDocument(
		136,                                      // changelog ID
		"6d4e36e4-b9d2-473b-a290-3ed7fb99073e", // record UUID
		"INSERT",
		emptyOld,
		emptyNew,
		time.Now(),
	)

	// Check that _id._data contains the changelog ID, not the UUID
	idMap, ok := event["_id"].(map[string]interface{})
	if !ok {
		t.Fatal("event[_id] is not a map")
	}

	data, ok := idMap["_data"]
	if !ok {
		t.Fatal("event[_id][_data] not found")
	}

	dataStr, ok := data.(string)
	if !ok {
		t.Fatalf("event[_id][_data] is not a string, got %T", data)
	}

	want := "136"
	if dataStr != want {
		t.Errorf("event[_id][_data] = %v, want %v", dataStr, want)
	}

	// Verify it's int-parseable
	_, err := strconv.ParseInt(dataStr, 10, 64)
	if err != nil {
		t.Errorf("event[_id][_data] not int-parseable: %v, error: %v", dataStr, err)
	}

	// Verify it's NOT the UUID
	if dataStr == "6d4e36e4-b9d2-473b-a290-3ed7fb99073e" {
		t.Error("event[_id][_data] contains UUID instead of changelog ID")
	}
}

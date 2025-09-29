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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	"k8s.io/klog/v2"
)

// PostgreSQLHealthEventStore implements HealthEventStore for PostgreSQL
type PostgreSQLHealthEventStore struct {
	db *sql.DB
}

// NewPostgreSQLHealthEventStore creates a new PostgreSQL health event store
func NewPostgreSQLHealthEventStore(db *sql.DB) *PostgreSQLHealthEventStore {
	return &PostgreSQLHealthEventStore{db: db}
}

// InsertHealthEvents inserts health events into the database
func (p *PostgreSQLHealthEventStore) InsertHealthEvents(
	ctx context.Context, eventWithStatus *datastore.HealthEventWithStatus,
) error {
	documentJSON, err := json.Marshal(eventWithStatus)
	if err != nil {
		return fmt.Errorf("failed to marshal health event: %w", err)
	}

	indexFields := p.extractIndexFields(eventWithStatus)
	nodeQuarantined := p.convertNodeQuarantinedStatus(eventWithStatus.HealthEventStatus.NodeQuarantined)

	return p.insertHealthEventRecord(ctx, indexFields, nodeQuarantined, eventWithStatus, documentJSON)
}

// healthEventIndexFields contains fields extracted for indexing
type healthEventIndexFields struct {
	nodeName          string
	eventType         string
	severity          string
	recommendedAction string
}

// extractIndexFields extracts key fields for indexing from the health event
func (p *PostgreSQLHealthEventStore) extractIndexFields(
	eventWithStatus *datastore.HealthEventWithStatus,
) healthEventIndexFields {
	fields := healthEventIndexFields{}

	healthEventMap, ok := eventWithStatus.HealthEvent.(map[string]interface{})
	if !ok {
		return fields
	}

	if nodeNameVal, exists := healthEventMap["nodeName"]; exists {
		if nodeNameStr, ok := nodeNameVal.(string); ok {
			fields.nodeName = nodeNameStr
		}
	}

	if eventTypeVal, exists := healthEventMap["checkName"]; exists {
		if eventTypeStr, ok := eventTypeVal.(string); ok {
			fields.eventType = eventTypeStr
		}
	}

	if severityVal, exists := healthEventMap["componentClass"]; exists {
		if severityStr, ok := severityVal.(string); ok {
			fields.severity = severityStr
		}
	}

	if actionVal, exists := healthEventMap["recommendedAction"]; exists {
		if actionStr, ok := actionVal.(string); ok {
			fields.recommendedAction = actionStr
		}
	}

	return fields
}

// convertNodeQuarantinedStatus converts node quarantined status to string pointer
func (p *PostgreSQLHealthEventStore) convertNodeQuarantinedStatus(status *datastore.Status) *string {
	if status == nil {
		return nil
	}

	statusStr := string(*status)

	return &statusStr
}

// insertHealthEventRecord inserts the health event record into the database
func (p *PostgreSQLHealthEventStore) insertHealthEventRecord(
	ctx context.Context,
	fields healthEventIndexFields,
	nodeQuarantined *string,
	eventWithStatus *datastore.HealthEventWithStatus,
	documentJSON []byte,
) error {
	query := `
		INSERT INTO health_events (
			node_name, event_type, severity, recommended_action,
			node_quarantined, user_pods_eviction_status, user_pods_eviction_message,
			fault_remediated, last_remediation_timestamp, document
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
	`

	_, err := p.db.ExecContext(ctx, query,
		fields.nodeName,
		fields.eventType,
		fields.severity,
		fields.recommendedAction,
		nodeQuarantined,
		string(eventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Status),
		eventWithStatus.HealthEventStatus.UserPodsEvictionStatus.Message,
		eventWithStatus.HealthEventStatus.FaultRemediated,
		eventWithStatus.HealthEventStatus.LastRemediationTimestamp,
		documentJSON,
	)

	if err != nil {
		return fmt.Errorf("failed to insert health event: %w", err)
	}

	klog.V(2).Infof("Successfully inserted health event for node: %s", fields.nodeName)

	return nil
}

// UpdateHealthEventStatus updates the status of a health event by ID
func (p *PostgreSQLHealthEventStore) UpdateHealthEventStatus(
	ctx context.Context, id string, status datastore.HealthEventStatus,
) error {
	var nodeQuarantined *string

	if status.NodeQuarantined != nil {
		statusStr := string(*status.NodeQuarantined)
		nodeQuarantined = &statusStr
	}

	query := `
		UPDATE health_events
		SET node_quarantined = $1,
		    user_pods_eviction_status = $2,
		    user_pods_eviction_message = $3,
		    fault_remediated = $4,
		    last_remediation_timestamp = $5,
		    updated_at = NOW()
		WHERE id = $6
	`

	result, err := p.db.ExecContext(ctx, query,
		nodeQuarantined,
		string(status.UserPodsEvictionStatus.Status),
		status.UserPodsEvictionStatus.Message,
		status.FaultRemediated,
		status.LastRemediationTimestamp,
		id,
	)

	if err != nil {
		return fmt.Errorf("failed to update health event status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("health event not found: %s", id)
	}

	klog.V(2).Infof("Successfully updated health event status: %s", id)

	return nil
}

// UpdateHealthEventStatusByNode updates the status of health events by node name
func (p *PostgreSQLHealthEventStore) UpdateHealthEventStatusByNode(
	ctx context.Context, nodeName string, status datastore.HealthEventStatus,
) error {
	var nodeQuarantined *string

	if status.NodeQuarantined != nil {
		statusStr := string(*status.NodeQuarantined)
		nodeQuarantined = &statusStr
	}

	query := `
		UPDATE health_events
		SET node_quarantined = $1,
		    user_pods_eviction_status = $2,
		    user_pods_eviction_message = $3,
		    fault_remediated = $4,
		    last_remediation_timestamp = $5,
		    updated_at = NOW()
		WHERE node_name = $6
	`

	result, err := p.db.ExecContext(ctx, query,
		nodeQuarantined,
		string(status.UserPodsEvictionStatus.Status),
		status.UserPodsEvictionStatus.Message,
		status.FaultRemediated,
		status.LastRemediationTimestamp,
		nodeName,
	)

	if err != nil {
		return fmt.Errorf("failed to update health event status by node: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	klog.V(2).Infof("Successfully updated %d health event statuses for node: %s", rowsAffected, nodeName)

	return nil
}

// FindHealthEventsByNode finds all health events for a specific node
func (p *PostgreSQLHealthEventStore) FindHealthEventsByNode(
	ctx context.Context, nodeName string,
) ([]datastore.HealthEventWithStatus, error) {
	query := `
		SELECT document FROM health_events
		WHERE node_name = $1
		ORDER BY created_at DESC
	`

	rows, err := p.db.QueryContext(ctx, query, nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to query health events by node: %w", err)
	}
	defer rows.Close()

	var events []datastore.HealthEventWithStatus

	for rows.Next() {
		var documentJSON []byte
		if err := rows.Scan(&documentJSON); err != nil {
			return nil, fmt.Errorf("failed to scan health event: %w", err)
		}

		var event datastore.HealthEventWithStatus
		if err := json.Unmarshal(documentJSON, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal health event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating health event rows: %w", err)
	}

	return events, nil
}

// FindHealthEventsByFilter finds health events based on filter criteria
func (p *PostgreSQLHealthEventStore) FindHealthEventsByFilter(
	ctx context.Context, filter map[string]interface{},
) ([]datastore.HealthEventWithStatus, error) {
	conditions, params := p.buildFilterConditions(filter)
	query := p.buildFilterQuery(conditions)

	return p.executeFilterQuery(ctx, query, params)
}

// buildFilterConditions builds WHERE conditions and parameters from filter map
func (p *PostgreSQLHealthEventStore) buildFilterConditions(
	filter map[string]interface{},
) ([]string, []interface{}) {
	var (
		conditions []string
		params     []interface{}
		paramIndex = 1
	)

	for key, value := range filter {
		condition, param := p.buildSingleCondition(key, value, paramIndex)
		conditions = append(conditions, condition)
		params = append(params, param)
		paramIndex++
	}

	return conditions, params
}

// buildSingleCondition builds a single WHERE condition for a filter key-value pair
func (p *PostgreSQLHealthEventStore) buildSingleCondition(
	key string,
	value interface{},
	paramIndex int,
) (string, interface{}) {
	switch key {
	case "node_name":
		return fmt.Sprintf("node_name = $%d", paramIndex), value
	case "event_type":
		return fmt.Sprintf("event_type = $%d", paramIndex), value
	case "node_quarantined":
		return fmt.Sprintf("node_quarantined = $%d", paramIndex), value
	case "user_pods_eviction_status":
		return fmt.Sprintf("user_pods_eviction_status = $%d", paramIndex), value
	default:
		// For complex JSON queries, use JSONB operators
		return fmt.Sprintf("document->>'%s' = $%d", key, paramIndex), value
	}
}

// buildFilterQuery builds the complete SQL query with WHERE clause
func (p *PostgreSQLHealthEventStore) buildFilterQuery(conditions []string) string {
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	return fmt.Sprintf(`
		SELECT document FROM health_events
		%s
		ORDER BY created_at DESC
	`, whereClause)
}

// executeFilterQuery executes the filter query and returns results
func (p *PostgreSQLHealthEventStore) executeFilterQuery(
	ctx context.Context,
	query string,
	params []interface{},
) ([]datastore.HealthEventWithStatus, error) {
	rows, err := p.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query health events by filter: %w", err)
	}
	defer rows.Close()

	var events []datastore.HealthEventWithStatus

	for rows.Next() {
		var documentJSON []byte
		if err := rows.Scan(&documentJSON); err != nil {
			return nil, fmt.Errorf("failed to scan health event: %w", err)
		}

		var event datastore.HealthEventWithStatus
		if err := json.Unmarshal(documentJSON, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal health event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating health event rows: %w", err)
	}

	return events, nil
}

// FindHealthEventsByStatus finds health events matching a specific status
func (p *PostgreSQLHealthEventStore) FindHealthEventsByStatus(
	ctx context.Context, status datastore.Status,
) ([]datastore.HealthEventWithStatus, error) {
	query := `
		SELECT document FROM health_events
		WHERE user_pods_eviction_status = $1
		ORDER BY created_at DESC
	`

	rows, err := p.db.QueryContext(ctx, query, string(status))
	if err != nil {
		return nil, fmt.Errorf("failed to query health events by status: %w", err)
	}
	defer rows.Close()

	var events []datastore.HealthEventWithStatus

	for rows.Next() {
		var documentJSON []byte
		if err := rows.Scan(&documentJSON); err != nil {
			return nil, fmt.Errorf("failed to scan health event: %w", err)
		}

		var event datastore.HealthEventWithStatus
		if err := json.Unmarshal(documentJSON, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal health event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating health event rows: %w", err)
	}

	return events, nil
}

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
	"log/slog"
	"reflect"
	"strings"
	"unicode"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/query"
)

// PostgreSQLDatabaseClient implements client.DatabaseClient for PostgreSQL
// This provides backward compatibility with services using the legacy DatabaseClient interface
type PostgreSQLDatabaseClient struct {
	db        *sql.DB
	tableName string
}

// toSnakeCase converts PascalCase strings to snake_case for PostgreSQL table names
// Examples: "HealthEvents" -> "health_events", "MaintenanceEvents" -> "maintenance_events"
func toSnakeCase(s string) string {
	if s == "" {
		return s
	}

	var result strings.Builder

	for i, r := range s {
		if unicode.IsUpper(r) {
			// Add underscore before uppercase letters (except first character)
			if i > 0 {
				result.WriteRune('_')
			}

			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// NewPostgreSQLDatabaseClient creates a new PostgreSQL database client
// Converts MongoDB-style PascalCase table names to PostgreSQL snake_case
func NewPostgreSQLDatabaseClient(db *sql.DB, tableName string) client.DatabaseClient {
	// Convert PascalCase to snake_case for PostgreSQL compatibility
	// MongoDB: "HealthEvents" -> PostgreSQL: "health_events"
	snakeCaseTableName := toSnakeCase(tableName)

	slog.Info("Creating PostgreSQL database client",
		"originalTableName", tableName,
		"postgresTableName", snakeCaseTableName)

	return &PostgreSQLDatabaseClient{
		db:        db,
		tableName: snakeCaseTableName,
	}
}

// InsertMany inserts multiple documents into the database
func (c *PostgreSQLDatabaseClient) InsertMany(
	ctx context.Context, documents []interface{},
) (*client.InsertManyResult, error) {
	slog.Info("=== [PostgreSQL-DEBUG] InsertMany ENTRY ===",
		"documentCount", len(documents),
		"tableName", c.tableName,
		"timestamp", "entry")

	if len(documents) == 0 {
		slog.Info("[PostgreSQL-DEBUG] No documents to insert, returning empty result")
		return &client.InsertManyResult{InsertedIDs: []interface{}{}}, nil
	}

	// Check if we're inserting health events - they need special handling for PostgreSQL
	if len(documents) > 0 {
		firstDocType := fmt.Sprintf("%T", documents[0])
		slog.Info("=== [PostgreSQL-DEBUG] Document type check ===",
			"documentType", firstDocType,
			"documentCount", len(documents),
			"expectedType", "model.HealthEventWithStatus")

		if _, ok := documents[0].(model.HealthEventWithStatus); ok {
			slog.Info("=== [PostgreSQL-DEBUG] TYPE ASSERTION SUCCESS ===",
				"detectedType", "model.HealthEventWithStatus",
				"routing", "insertHealthEvents")

			return c.insertHealthEvents(ctx, documents)
		}

		slog.Warn("=== [PostgreSQL-DEBUG] TYPE ASSERTION FAILED ===",
			"actualType", firstDocType,
			"expectedType", "model.HealthEventWithStatus",
			"fallback", "generic insertion")
	}

	// Generic document insertion for non-health-event documents
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (data) VALUES ($1) RETURNING id", c.tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	insertedIDs := make([]interface{}, 0, len(documents))
	for _, doc := range documents {
		jsonData, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}

		var id string

		err = stmt.QueryRowContext(ctx, jsonData).Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("failed to insert document: %w", err)
		}

		insertedIDs = append(insertedIDs, id)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &client.InsertManyResult{
		InsertedIDs: insertedIDs,
	}, nil
}

// insertHealthEvents handles batch insertion of health events using PostgreSQL-specific schema
func (c *PostgreSQLDatabaseClient) insertHealthEvents(
	ctx context.Context, documents []interface{},
) (*client.InsertManyResult, error) {
	slog.Info("=== [PostgreSQL-DEBUG] insertHealthEvents ENTRY ===",
		"documentCount", len(documents))

	healthStore := NewPostgreSQLHealthEventStore(c.db)
	insertedIDs := make([]interface{}, 0, len(documents))

	for i, doc := range documents {
		slog.Info("[PostgreSQL-DEBUG] Processing health event",
			"index", i,
			"docType", fmt.Sprintf("%T", doc))

		modelEvent, ok := doc.(model.HealthEventWithStatus)
		if !ok {
			slog.Error("[PostgreSQL-DEBUG] Type assertion failed in insertHealthEvents",
				"expectedType", "model.HealthEventWithStatus",
				"actualType", fmt.Sprintf("%T", doc))

			return nil, fmt.Errorf("expected HealthEventWithStatus but got %T", doc)
		}

		slog.Info("[PostgreSQL-DEBUG] Health event type assertion succeeded",
			"index", i)

		// CRITICAL: Extract index fields from the protobuf BEFORE JSON marshaling
		// After JSON marshal/unmarshal, the protobuf is converted to a map and we lose type info
		var indexFields healthEventIndexFields
		if modelEvent.HealthEvent != nil {
			indexFields = healthEventIndexFields{
				nodeName:          modelEvent.HealthEvent.NodeName,
				eventType:         modelEvent.HealthEvent.CheckName,
				severity:          modelEvent.HealthEvent.ComponentClass,
				recommendedAction: modelEvent.HealthEvent.RecommendedAction.String(),
			}

			slog.Info("[PostgreSQL-DEBUG] Extracted index fields from protobuf",
				"nodeName", indexFields.nodeName,
				"eventType", indexFields.eventType,
				"severity", indexFields.severity,
				"recommendedAction", indexFields.recommendedAction)
		} else {
			slog.Warn("[PostgreSQL-DEBUG] modelEvent.HealthEvent is nil, using empty index fields")
		}

		// Convert model.HealthEventWithStatus to datastore.HealthEventWithStatus
		// by marshaling to JSON and unmarshaling to the datastore type
		modelJSON, err := json.Marshal(modelEvent)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal model health event: %w", err)
		}

		var datastoreEvent datastore.HealthEventWithStatus
		if err := json.Unmarshal(modelJSON, &datastoreEvent); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to datastore health event: %w", err)
		}

		// Use the PostgreSQL health event store to insert with proper schema
		// Pass the index fields we extracted from the protobuf
		slog.Info("[PostgreSQL-DEBUG] Calling InsertHealthEventsWithIndexFields on health store")

		err = healthStore.InsertHealthEventsWithIndexFields(ctx, &datastoreEvent, indexFields)
		if err != nil {
			slog.Error("[PostgreSQL-DEBUG] InsertHealthEventsWithIndexFields failed",
				"error", err)

			return nil, fmt.Errorf("[postgresql:insert] failed to insert documents: %w", err)
		}

		slog.Info("[PostgreSQL-DEBUG] InsertHealthEventsWithIndexFields succeeded")

		// For now, use a placeholder ID since InsertHealthEvents doesn't return the ID
		// In the future, we could modify InsertHealthEvents to return the generated UUID
		insertedIDs = append(insertedIDs, "inserted")
	}

	slog.Info("=== [PostgreSQL-DEBUG] insertHealthEvents COMPLETE ===",
		"insertedCount", len(insertedIDs))

	return &client.InsertManyResult{
		InsertedIDs: insertedIDs,
	}, nil
}

// UpdateDocumentStatus updates a specific status field in a document
func (c *PostgreSQLDatabaseClient) UpdateDocumentStatus(
	ctx context.Context, documentID string, statusPath string, status interface{},
) error {
	// Use query builder to create update
	update := query.NewUpdate().Set(statusPath, status)

	// For health_events table with nodequarantined status, also update denormalized column
	if c.tableName == "health_events" && statusPath == "healtheventstatus.nodequarantined" {
		update.Set("node_quarantined", status)
	}

	setClause, args := update.ToSQL()

	// For health_events table, use direct id column comparison
	// For other tables, use JSON path data->>'_id'
	var whereClause string
	if c.tableName == "health_events" {
		whereClause = fmt.Sprintf("id = $%d", len(args)+1)
	} else {
		whereClause = fmt.Sprintf("data->>'_id' = $%d", len(args)+1)
	}

	//nolint:gosec // G201: table name is controlled internally, not from user input
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		c.tableName, setClause, whereClause,
	)

	args = append(args, documentID)

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update document status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("document not found: %s", documentID)
	}

	return nil
}

// UpdateDocument updates a single document matching the filter
func (c *PostgreSQLDatabaseClient) UpdateDocument(
	ctx context.Context, filter interface{}, update interface{},
) (*client.UpdateResult, error) {
	return c.updateDocuments(ctx, filter, update, false)
}

// UpdateManyDocuments updates all documents matching the filter
func (c *PostgreSQLDatabaseClient) UpdateManyDocuments(
	ctx context.Context, filter interface{}, update interface{},
) (*client.UpdateResult, error) {
	return c.updateDocuments(ctx, filter, update, true)
}

// parseMongoOperator parses a MongoDB operator and adds it to the query builder
func (c *PostgreSQLDatabaseClient) parseMongoOperator(
	builder *query.Builder, key string, op string, opValue interface{},
) error {
	switch op {
	case "$ne":
		builder.Build(query.Ne(key, opValue))
	case "$eq":
		builder.Build(query.Eq(key, opValue))
	case "$gt":
		builder.Build(query.Gt(key, opValue))
	case "$gte":
		builder.Build(query.Gte(key, opValue))
	case "$lt":
		builder.Build(query.Lt(key, opValue))
	case "$lte":
		builder.Build(query.Lte(key, opValue))
	case "$in":
		// Type assert to []interface{} for $in operator
		if inValues, ok := opValue.([]interface{}); ok {
			builder.Build(query.In(key, inValues))
		} else {
			return fmt.Errorf("$in operator requires array value")
		}
	default:
		return fmt.Errorf("unsupported MongoDB operator: %s", op)
	}

	return nil
}

// convertFilterToWhereClause converts various filter formats to SQL WHERE clause
// The paramOffset parameter specifies where parameter numbering should start
func (c *PostgreSQLDatabaseClient) convertFilterToWhereClause(
	filter interface{}, paramOffset int,
) (string, []interface{}, error) {
	if builder, ok := filter.(*query.Builder); ok {
		whereClause, filterArgs := builder.ToSQLWithOffset(paramOffset)

		return whereClause, filterArgs, nil
	}

	if filterMap, ok := filter.(map[string]interface{}); ok {
		// Handle both simple equality and MongoDB-style filters
		builder := query.New()

		for key, value := range filterMap {
			// Check if value is a MongoDB operator map (e.g., {"$ne": "value"})
			if valueMap, isMap := value.(map[string]interface{}); isMap {
				// Parse MongoDB operators
				for op, opValue := range valueMap {
					if err := c.parseMongoOperator(builder, key, op, opValue); err != nil {
						return "", nil, err
					}
				}
			} else {
				// Simple equality
				builder.Build(query.Eq(key, value))
			}
		}

		whereClause, filterArgs := builder.ToSQLWithOffset(paramOffset)

		return whereClause, filterArgs, nil
	}

	return "", nil, fmt.Errorf("unsupported filter type: %T", filter)
}

// convertUpdateToSetClause converts various update formats to SQL SET clause
func (c *PostgreSQLDatabaseClient) convertUpdateToSetClause(
	update interface{},
) (string, []interface{}, error) {
	slog.Debug("Converting update to SET clause", "updateType", fmt.Sprintf("%T", update), "update", update)

	if updateBuilder, ok := update.(*query.UpdateBuilder); ok {
		setClause, updateArgs := updateBuilder.ToSQL()
		slog.Debug("Converted UpdateBuilder to SQL", "setClause", setClause, "updateArgs", updateArgs)

		return setClause, updateArgs, nil
	}

	if updateMap, ok := update.(map[string]interface{}); ok {
		// Handle MongoDB-style update with $set operator
		var setFields map[string]interface{}

		if setOp, hasSet := updateMap["$set"]; hasSet {
			var ok bool

			setFields, ok = setOp.(map[string]interface{})
			if !ok {
				return "", nil, fmt.Errorf("$set value must be a map[string]interface{}")
			}

			slog.Debug("Found $set operator", "setFields", setFields)
		} else {
			// Direct field updates (no $set operator)
			setFields = updateMap
			slog.Debug("Direct field updates (no $set)", "setFields", setFields)
		}

		// Build SET clause from fields
		builder := query.NewUpdate()

		for key, value := range setFields {
			slog.Debug("Adding field to UpdateBuilder", "key", key, "value", value, "valueType", fmt.Sprintf("%T", value))
			builder.Set(key, value)
		}

		setClause, updateArgs := builder.ToSQL()
		slog.Debug("Converted map to SQL", "setClause", setClause, "updateArgs", updateArgs)

		return setClause, updateArgs, nil
	}

	return "", nil, fmt.Errorf("unsupported update type: %T", update)
}

// updateDocuments is the internal implementation for update operations
func (c *PostgreSQLDatabaseClient) updateDocuments(
	ctx context.Context, filter interface{}, update interface{}, updateMany bool,
) (*client.UpdateResult, error) {
	// Convert update to SQL SET clause first (starts from $1)
	setClause, updateArgs, err := c.convertUpdateToSetClause(update)
	if err != nil {
		return nil, err
	}

	// Convert filter to SQL WHERE clause (starts after update parameters)
	paramOffset := len(updateArgs) + 1

	whereClause, filterArgs, err := c.convertFilterToWhereClause(filter, paramOffset)
	if err != nil {
		return nil, err
	}

	// Combine arguments (intentionally creating new slice to preserve original args)
	//nolint:gocritic // appendAssign: intentional to avoid modifying updateArgs
	allArgs := append(updateArgs, filterArgs...)

	// Build query
	//nolint:gosec // G201: table name is controlled internally, not from user input
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", c.tableName, setClause, whereClause)
	// PostgreSQL doesn't support LIMIT in UPDATE, use subquery instead for single update
	// In most cases updateMany=true, so we don't need this optimization

	slog.Debug("Executing UPDATE query",
		"sql", sql,
		"updateArgs", updateArgs,
		"filterArgs", filterArgs,
		"allArgs", allArgs)

	result, err := c.db.ExecContext(ctx, sql, allArgs...)
	if err != nil {
		slog.Error("Failed to execute UPDATE query",
			"error", err,
			"sql", sql,
			"updateArgs", updateArgs,
			"filterArgs", filterArgs)

		return nil, fmt.Errorf("failed to update documents: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return &client.UpdateResult{
		MatchedCount:  rowsAffected,
		ModifiedCount: rowsAffected,
	}, nil
}

// UpsertDocument inserts or updates a document
func (c *PostgreSQLDatabaseClient) UpsertDocument(
	ctx context.Context, filter interface{}, document interface{},
) (*client.UpdateResult, error) {
	jsonData, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	// PostgreSQL UPSERT using ON CONFLICT
	//nolint:gosec // G201: table name is controlled internally, not from user input
	query := fmt.Sprintf(
		`INSERT INTO %s (data) VALUES ($1)
		ON CONFLICT ((data->>'_id')) DO UPDATE SET data = EXCLUDED.data`,
		c.tableName,
	)

	result, err := c.db.ExecContext(ctx, query, jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert document: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return &client.UpdateResult{
		MatchedCount:  rowsAffected,
		ModifiedCount: rowsAffected,
		UpsertedCount: rowsAffected,
	}, nil
}

// FindOne finds a single document matching the filter
func (c *PostgreSQLDatabaseClient) FindOne(
	ctx context.Context, filter interface{}, options *client.FindOneOptions,
) (client.SingleResult, error) {
	// Convert filter to SQL WHERE clause
	var whereClause string

	var args []interface{}

	if builder, ok := filter.(*query.Builder); ok {
		whereClause, args = builder.ToSQL()
	} else if filterMap, ok := filter.(map[string]interface{}); ok {
		builder := query.New()
		for key, value := range filterMap {
			builder.Build(query.Eq(key, value))
		}

		whereClause, args = builder.ToSQL()
	} else {
		return nil, fmt.Errorf("unsupported filter type")
	}

	//nolint:gosec // G201: table name is controlled internally, not from user input
	query := fmt.Sprintf("SELECT document FROM %s WHERE %s LIMIT 1", c.tableName, whereClause)

	var jsonData []byte

	err := c.db.QueryRowContext(ctx, query, args...).Scan(&jsonData)
	if err != nil {
		if err == sql.ErrNoRows {
			return &postgresqlSingleResult{err: client.ErrNoDocuments}, nil
		}

		return nil, fmt.Errorf("failed to query document: %w", err)
	}

	return &postgresqlSingleResult{data: jsonData}, nil
}

// Find finds all documents matching the filter
func (c *PostgreSQLDatabaseClient) Find(
	ctx context.Context, filter interface{}, options *client.FindOptions,
) (client.Cursor, error) {
	// Convert filter to SQL WHERE clause
	var whereClause string

	var args []interface{}

	if builder, ok := filter.(*query.Builder); ok {
		whereClause, args = builder.ToSQL()
	} else if filterMap, ok := filter.(map[string]interface{}); ok {
		builder := query.New()
		for key, value := range filterMap {
			builder.Build(query.Eq(key, value))
		}

		whereClause, args = builder.ToSQL()
	} else {
		whereClause = "TRUE" // No filter
	}

	//nolint:gosec // G201: table name is controlled internally, not from user input
	query := fmt.Sprintf("SELECT document FROM %s WHERE %s", c.tableName, whereClause)

	// Apply options
	if options != nil {
		if options.Limit != nil && *options.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", *options.Limit)
		}
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents: %w", err)
	}

	return &postgresqlCursor{rows: rows}, nil
}

// CountDocuments counts documents matching the filter
func (c *PostgreSQLDatabaseClient) CountDocuments(
	ctx context.Context, filter interface{}, options *client.CountOptions,
) (int64, error) {
	var whereClause string

	var args []interface{}

	if builder, ok := filter.(*query.Builder); ok {
		whereClause, args = builder.ToSQL()
	} else if filterMap, ok := filter.(map[string]interface{}); ok {
		builder := query.New()
		for key, value := range filterMap {
			builder.Build(query.Eq(key, value))
		}

		whereClause, args = builder.ToSQL()
	} else {
		whereClause = "TRUE"
	}

	//nolint:gosec // G201: table name is controlled internally, not from user input
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", c.tableName, whereClause)

	var count int64

	err := c.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	return count, nil
}

// Aggregate performs aggregation operations (limited support for PostgreSQL)
func (c *PostgreSQLDatabaseClient) Aggregate(
	ctx context.Context, pipeline interface{},
) (client.Cursor, error) {
	slog.Info("Aggregate called on PostgreSQL database client",
		"tableName", c.tableName)

	// Create a PostgreSQLClient to handle the aggregation
	// This reuses the existing db connection
	postgresClient := client.NewPostgreSQLClientFromDB(c.db, c.tableName)

	// Delegate to the PostgreSQLClient implementation
	return postgresClient.Aggregate(ctx, pipeline)
}

// Ping checks the database connection
func (c *PostgreSQLDatabaseClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// NewChangeStreamWatcher creates a new change stream watcher
func (c *PostgreSQLDatabaseClient) NewChangeStreamWatcher(
	ctx context.Context, tokenConfig client.TokenConfig, pipeline interface{},
) (client.ChangeStreamWatcher, error) {
	watcher := NewPostgreSQLChangeStreamWatcher(c.db, tokenConfig.ClientName, c.tableName)

	// Apply pipeline filter if provided
	if pipeline != nil {
		pipelineFilter, err := NewPipelineFilter(pipeline)
		if err != nil {
			slog.Warn("Failed to create pipeline filter", "error", err)
		} else {
			watcher.pipelineFilter = pipelineFilter
		}
	}

	// Return the adapter that implements client.ChangeStreamWatcher
	return NewPostgreSQLChangeStreamAdapter(watcher), nil
}

// Close closes the database connection
func (c *PostgreSQLDatabaseClient) Close(ctx context.Context) error {
	return c.db.Close()
}

// --- Helper types for results ---

type postgresqlSingleResult struct {
	data []byte
	err  error
}

func (r *postgresqlSingleResult) Decode(v interface{}) error {
	if r.err != nil {
		return r.err
	}

	return json.Unmarshal(r.data, v)
}

func (r *postgresqlSingleResult) Err() error {
	return r.err
}

type postgresqlCursor struct {
	rows *sql.Rows
}

func (c *postgresqlCursor) Next(ctx context.Context) bool {
	return c.rows.Next()
}

func (c *postgresqlCursor) Decode(v interface{}) error {
	var jsonData []byte
	if err := c.rows.Scan(&jsonData); err != nil {
		return fmt.Errorf("failed to scan row: %w", err)
	}

	return json.Unmarshal(jsonData, v)
}

func (c *postgresqlCursor) Close(ctx context.Context) error {
	return c.rows.Close()
}

func (c *postgresqlCursor) Err() error {
	return c.rows.Err()
}

func (c *postgresqlCursor) All(ctx context.Context, results interface{}) error {
	// Results should be a pointer to a slice
	resultsVal := reflect.ValueOf(results)
	if resultsVal.Kind() != reflect.Ptr || resultsVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("results must be a pointer to a slice")
	}

	sliceVal := resultsVal.Elem()
	elemType := sliceVal.Type().Elem()

	for c.rows.Next() {
		// Create a new element of the slice type
		elemPtr := reflect.New(elemType)

		var jsonData []byte
		if err := c.rows.Scan(&jsonData); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if err := json.Unmarshal(jsonData, elemPtr.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal document: %w", err)
		}

		sliceVal = reflect.Append(sliceVal, elemPtr.Elem())
	}

	resultsVal.Elem().Set(sliceVal)

	return c.rows.Err()
}

// Verify that PostgreSQLDatabaseClient implements client.DatabaseClient
var _ client.DatabaseClient = (*PostgreSQLDatabaseClient)(nil)

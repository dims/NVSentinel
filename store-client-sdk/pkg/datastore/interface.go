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

package datastore

import (
	"context"
	"fmt"
	"time"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
)

// Status represents operation status
type Status string

const (
	StatusNotStarted   Status = "NotStarted"
	StatusInProgress   Status = "InProgress"
	StatusFailed       Status = "Failed"
	StatusSucceeded    Status = "Succeeded"
	AlreadyDrained     Status = "AlreadyDrained"
	UnQuarantined      Status = "UnQuarantined"
	Quarantined        Status = "Quarantined"
	AlreadyQuarantined Status = "AlreadyQuarantined"
)

// OperationStatus represents status of an operation
type OperationStatus struct {
	Status   Status                 `json:"status"`
	Message  string                 `json:"message,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// HealthEventStatus represents status of a health event
type HealthEventStatus struct {
	NodeQuarantined          *Status         `json:"nodequarantined"`
	UserPodsEvictionStatus   OperationStatus `json:"userpodsevictionstatus"`
	FaultRemediated          *bool           `json:"faultremediated"`
	LastRemediationTimestamp *time.Time      `json:"lastremediationtimestamp,omitempty"`
}

// HealthEventWithStatus wraps a health event with status information
type HealthEventWithStatus struct {
	CreatedAt         time.Time              `json:"createdAt"`
	HealthEvent       interface{}            `json:"healthevent,omitempty"`
	HealthEventStatus HealthEventStatus      `json:"healtheventstatus"`
	RawEvent          map[string]interface{} // Raw event from change stream (for extracting MongoDB _id etc)
}

// DataStoreProvider defines the supported datastore types
type DataStoreProvider string

const (
	ProviderMongoDB    DataStoreProvider = "mongodb"
	ProviderPostgreSQL DataStoreProvider = "postgresql"
)

// DataStoreConfig holds configuration for any datastore provider
type DataStoreConfig struct {
	Provider   DataStoreProvider `json:"provider" yaml:"provider"`
	Connection ConnectionConfig  `json:"connection" yaml:"connection"`
	Options    map[string]string `json:"options,omitempty" yaml:"options,omitempty"`
}

// ConnectionConfig holds generic connection parameters
type ConnectionConfig struct {
	Host        string            `json:"host" yaml:"host"`
	Port        int               `json:"port" yaml:"port"`
	Database    string            `json:"database" yaml:"database"`
	Username    string            `json:"username,omitempty" yaml:"username,omitempty"`
	Password    string            `json:"password,omitempty" yaml:"password,omitempty"`
	SSLMode     string            `json:"sslmode,omitempty" yaml:"sslmode,omitempty"`
	SSLCert     string            `json:"sslcert,omitempty" yaml:"sslcert,omitempty"`
	SSLKey      string            `json:"sslkey,omitempty" yaml:"sslkey,omitempty"`
	SSLRootCert string            `json:"sslrootcert,omitempty" yaml:"sslrootcert,omitempty"`
	TLSConfig   *TLSConfig        `json:"tls,omitempty" yaml:"tls,omitempty"`
	ExtraParams map[string]string `json:"extraParams,omitempty" yaml:"extraParams,omitempty"`
}

// TLSConfig holds TLS certificate configuration
type TLSConfig struct {
	CertPath string `json:"certPath" yaml:"certPath"`
	KeyPath  string `json:"keyPath" yaml:"keyPath"`
	CAPath   string `json:"caPath" yaml:"caPath"`
}

// DataStore is the main interface that all providers must implement
type DataStore interface {
	// Maintenance Events (CSP Health Monitor)
	MaintenanceEventStore() MaintenanceEventStore

	// Health Events (Platform Connectors)
	HealthEventStore() HealthEventStore

	// Generic operations
	InsertMany(ctx context.Context, documents []interface{}) error

	// Connection management
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// MaintenanceEventStore handles CSP maintenance events
type MaintenanceEventStore interface {
	UpsertMaintenanceEvent(ctx context.Context, event *model.MaintenanceEvent) error
	FindEventsToTriggerQuarantine(ctx context.Context, triggerTimeLimit time.Duration) ([]model.MaintenanceEvent, error)
	FindEventsToTriggerHealthy(ctx context.Context, healthyDelay time.Duration) ([]model.MaintenanceEvent, error)
	UpdateEventStatus(ctx context.Context, eventID string, newStatus model.InternalStatus) error
	GetLastProcessedEventTimestampByCSP(
		ctx context.Context,
		clusterName string,
		cspType model.CSP,
		cspNameForLog string,
	) (timestamp time.Time, found bool, err error)
	FindLatestActiveEventByNodeAndType(
		ctx context.Context,
		nodeName string,
		maintenanceType model.MaintenanceType,
		statuses []model.InternalStatus,
	) (*model.MaintenanceEvent, bool, error)
	FindLatestOngoingEventByNode(ctx context.Context, nodeName string) (*model.MaintenanceEvent, bool, error)
	FindActiveEventsByStatuses(ctx context.Context, csp model.CSP, statuses []string) ([]model.MaintenanceEvent, error)
}

// HealthEventStore handles platform health events
type HealthEventStore interface {
	InsertHealthEvents(ctx context.Context, events *HealthEventWithStatus) error
	UpdateHealthEventStatus(ctx context.Context, id string, status HealthEventStatus) error
	UpdateHealthEventStatusByNode(ctx context.Context, nodeName string, status HealthEventStatus) error
	FindHealthEventsByNode(ctx context.Context, nodeName string) ([]HealthEventWithStatus, error)
	FindHealthEventsByFilter(ctx context.Context, filter map[string]interface{}) ([]HealthEventWithStatus, error)
	// FindHealthEventsByStatus finds health events matching a specific status
	// Used for recovery scenarios (e.g., finding in-progress drain operations on restart)
	FindHealthEventsByStatus(ctx context.Context, status Status) ([]HealthEventWithStatus, error)
}

// EventWithToken wraps a change stream event with its corresponding resume token
// This ensures the token saved matches the event processed, preventing race conditions
type EventWithToken struct {
	Event       map[string]interface{}
	ResumeToken []byte // Provider-agnostic binary token for resume position
}

// ChangeStreamWatcher provides basic change stream functionality (existing interface)
type ChangeStreamWatcher interface {
	Events() <-chan EventWithToken
	Start(ctx context.Context)
	MarkProcessed(ctx context.Context, token []byte) error
	Close(ctx context.Context) error
}

// DataStoreFactory creates datastore instances (used internally by the SDK)
type DataStoreFactory interface {
	NewDataStore(ctx context.Context, config DataStoreConfig) (DataStore, error)
	SupportedProviders() []DataStoreProvider
}

// GenerateID generates a unique identifier for documents
func GenerateID() string {
	// Use a simple UUID-like approach
	// This could be replaced with more sophisticated ID generation
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// Pipeline types for database-agnostic change stream filtering
// These types provide a generic way to construct aggregation pipelines without
// depending on specific database driver types (e.g., mongo.Pipeline, bson.D, bson.A)

// Element represents a key-value pair in a document (equivalent to bson.E)
type Element struct {
	Key   string
	Value interface{}
}

// Document represents an ordered document as a slice of elements (equivalent to bson.D)
type Document []Element

// Array represents an array of values (equivalent to bson.A)
type Array []interface{}

// Pipeline represents a database aggregation pipeline (equivalent to mongo.Pipeline)
// Each stage is a Document containing the aggregation operation
type Pipeline []Document

// D is a convenience constructor for Document
func D(elements ...Element) Document {
	return Document(elements)
}

// E is a convenience constructor for Element
func E(key string, value interface{}) Element {
	return Element{Key: key, Value: value}
}

// A is a convenience constructor for Array
func A(values ...interface{}) Array {
	return Array(values)
}

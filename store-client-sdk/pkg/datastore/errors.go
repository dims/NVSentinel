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
	"errors"
	"fmt"
)

// Common datastore errors
var (
	ErrConnectionFailed   = errors.New("datastore connection failed")
	ErrInvalidConfig      = errors.New("invalid datastore configuration")
	ErrOperationTimeout   = errors.New("datastore operation timeout")
	ErrProviderNotFound   = errors.New("datastore provider not found")
	ErrInvalidProvider    = errors.New("invalid datastore provider")
	ErrRecordNotFound     = errors.New("record not found")
	ErrInvalidData        = errors.New("invalid data format")
	ErrTransactionFailed  = errors.New("transaction failed")
	ErrChangeStreamFailed = errors.New("change stream operation failed")
	ErrCertificateInvalid = errors.New("certificate validation failed")
	ErrSSLConfigInvalid   = errors.New("SSL configuration invalid")
)

// ErrorType represents the category of error
type ErrorType string

const (
	ErrorTypeConnection    ErrorType = "connection"
	ErrorTypeConfiguration ErrorType = "configuration"
	ErrorTypeOperation     ErrorType = "operation"
	ErrorTypeValidation    ErrorType = "validation"
	ErrorTypeCertificate   ErrorType = "certificate"
	ErrorTypeTimeout       ErrorType = "timeout"
)

// DatastoreError represents a structured datastore error
type DatastoreError struct {
	Type     ErrorType
	Provider string
	Message  string
	Cause    error
}

func (e *DatastoreError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s error for %s provider: %s: %v", e.Type, e.Provider, e.Message, e.Cause)
	}

	return fmt.Sprintf("%s error for %s provider: %s", e.Type, e.Provider, e.Message)
}

func (e *DatastoreError) Unwrap() error {
	return e.Cause
}

// NewDatastoreError creates a new structured datastore error
func NewDatastoreError(errorType ErrorType, provider, message string, cause error) *DatastoreError {
	return &DatastoreError{
		Type:     errorType,
		Provider: provider,
		Message:  message,
		Cause:    cause,
	}
}

// WrapConnectionError wraps a connection error with provider context
func WrapConnectionError(err error, provider string) error {
	return NewDatastoreError(ErrorTypeConnection, provider, "failed to connect to datastore", err)
}

// WrapConfigurationError wraps a configuration error with provider context
func WrapConfigurationError(err error, provider, message string) error {
	return NewDatastoreError(ErrorTypeConfiguration, provider, message, err)
}

// WrapOperationError wraps an operation error with provider context
func WrapOperationError(err error, provider, operation string) error {
	return NewDatastoreError(ErrorTypeOperation, provider, fmt.Sprintf("%s operation failed", operation), err)
}

// WrapValidationError wraps a validation error with provider context
func WrapValidationError(err error, provider, field string) error {
	return NewDatastoreError(ErrorTypeValidation, provider, fmt.Sprintf("validation failed for field: %s", field), err)
}

// WrapCertificateError wraps a certificate error with provider context
func WrapCertificateError(err error, provider, message string) error {
	return NewDatastoreError(ErrorTypeCertificate, provider, message, err)
}

// WrapTimeoutError wraps a timeout error with provider context
func WrapTimeoutError(err error, provider, operation string) error {
	return NewDatastoreError(ErrorTypeTimeout, provider, fmt.Sprintf("%s operation timed out", operation), err)
}

// IsConnectionError checks if an error is a connection error
func IsConnectionError(err error) bool {
	var dsErr *DatastoreError
	if errors.As(err, &dsErr) {
		return dsErr.Type == ErrorTypeConnection
	}

	return errors.Is(err, ErrConnectionFailed)
}

// IsConfigurationError checks if an error is a configuration error
func IsConfigurationError(err error) bool {
	var dsErr *DatastoreError
	if errors.As(err, &dsErr) {
		return dsErr.Type == ErrorTypeConfiguration
	}

	return errors.Is(err, ErrInvalidConfig)
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	var dsErr *DatastoreError
	if errors.As(err, &dsErr) {
		return dsErr.Type == ErrorTypeValidation
	}

	return false
}

// IsCertificateError checks if an error is a certificate error
func IsCertificateError(err error) bool {
	var dsErr *DatastoreError
	if errors.As(err, &dsErr) {
		return dsErr.Type == ErrorTypeCertificate
	}

	return errors.Is(err, ErrCertificateInvalid)
}

// IsTimeoutError checks if an error is a timeout error
func IsTimeoutError(err error) bool {
	var dsErr *DatastoreError
	if errors.As(err, &dsErr) {
		return dsErr.Type == ErrorTypeTimeout
	}

	return errors.Is(err, ErrOperationTimeout)
}

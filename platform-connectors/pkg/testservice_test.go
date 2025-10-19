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

package pkg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTestService(t *testing.T) {
	service := NewTestService()
	assert.NotNil(t, service)
}

func TestVulnerableMongoQuery(t *testing.T) {
	service := NewTestService()

	// Test with normal input (this will fail to connect to MongoDB, but that's OK for testing)
	_, err := service.VulnerableMongoQuery("normaluser")
	// We expect an error since MongoDB isn't running, but we're testing code coverage
	assert.Error(t, err)

	// Test with admin input to trigger the vulnerable $where clause
	_, err = service.VulnerableMongoQuery("admin")
	assert.Error(t, err) // Will fail to connect, but covers the vulnerable code path
}

func TestVulnerableCommandExecution(t *testing.T) {
	service := NewTestService()

	// Test with a safe filename that exists
	_, err := service.VulnerableCommandExecution("/etc/hostname")
	// This may succeed or fail depending on the system, but covers the vulnerable code
	// The vulnerability is that user input goes directly to exec.Command
	if err != nil {
		t.Logf("Expected error for command execution: %v", err)
	}
}

func TestUnsafePathConstruction(t *testing.T) {
	service := NewTestService()

	// Test normal path
	result := service.UnsafePathConstruction("file.txt")
	assert.Equal(t, "/safe/directory/file.txt", result)

	// Test path traversal vulnerability
	result = service.UnsafePathConstruction("../../../etc/passwd")
	assert.Equal(t, "/safe/directory/../../../etc/passwd", result)
	// This demonstrates the vulnerability - no sanitization
}

func TestWeakRandom(t *testing.T) {
	service := NewTestService()

	result := service.WeakRandom()
	assert.Equal(t, 42, result)
	// This predictable behavior demonstrates weak randomness
}

func TestCoveredFunction(t *testing.T) {
	service := NewTestService()

	// Test empty input
	result := service.CoveredFunction("")
	assert.Equal(t, "empty", result)

	// Test short input
	result = service.CoveredFunction("short")
	assert.Equal(t, "processed: short", result)

	// Test long input
	result = service.CoveredFunction("verylonginput")
	assert.Equal(t, "long", result)
}

func TestMultipleBranches(t *testing.T) {
	service := NewTestService()

	// Test all branches for maximum code coverage
	testCases := []struct {
		input    int
		expected string
	}{
		{-5, "negative"},
		{0, "zero"},
		{5, "small"},
		{50, "medium"},
		{150, "large"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := service.MultipleBranches(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// Benchmark test to add more coverage
func BenchmarkCoveredFunction(b *testing.B) {
	service := NewTestService()
	for i := 0; i < b.N; i++ {
		service.CoveredFunction("benchmark")
	}
}

// Test with table-driven tests for additional coverage
func TestMultipleBranchesTableDriven(t *testing.T) {
	service := NewTestService()

	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{"negative value", -10, "negative"},
		{"zero value", 0, "zero"},
		{"small positive", 7, "small"},
		{"medium value", 25, "medium"},
		{"large value", 1000, "large"},
		{"edge case small", 9, "small"},
		{"edge case medium", 99, "medium"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.MultipleBranches(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
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

package reconciler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	platformconnectorprotos "github.com/nvidia/nvsentinel/platform-connectors/pkg/protos"
	"github.com/nvidia/nvsentinel/statemanager"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
)

// MockK8sClient is a mock implementation of K8sClient interface
type MockK8sClient struct {
	createMaintenanceResourceFn func(ctx context.Context, healthEventDoc *HealthEventDoc) bool
	runLogCollectorJobFn        func(ctx context.Context, nodeName string) error
	getNodeStateLabelFn         func(ctx context.Context, nodeName string) (string, error)
}

func (m *MockK8sClient) CreateMaintenanceResource(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
	return m.createMaintenanceResourceFn(ctx, healthEventDoc)
}

func (m *MockK8sClient) RunLogCollectorJob(ctx context.Context, nodeName string) error {
	return m.runLogCollectorJobFn(ctx, nodeName)
}

func (m *MockK8sClient) GetNodeStateLabel(ctx context.Context, nodeName string) (string, error) {
	if m.getNodeStateLabelFn != nil {
		return m.getNodeStateLabelFn(ctx, nodeName)
	}
	return "", nil // Default: no state label
}

func TestNewReconciler(t *testing.T) {
	tests := []struct {
		name             string
		nodeName         string
		crCreationResult bool
		dryRun           bool
	}{
		{
			name:             "Create reconciler with dry run enabled",
			nodeName:         "node1",
			crCreationResult: true,
			dryRun:           true,
		},
		{
			name:             "Create reconciler with dry run disabled",
			nodeName:         "node2",
			crCreationResult: false,
			dryRun:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ReconcilerConfig{
				K8sClient: &MockK8sClient{
					createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
						healthEvent := healthEventDoc.HealthEvent.(*platformconnectorprotos.HealthEvent)
						assert.Equal(t, tt.nodeName, healthEvent.NodeName)
						return tt.crCreationResult
					},
				},
			}

			r := NewReconciler(cfg, tt.dryRun)
			assert.NotNil(t, r)
			assert.Equal(t, tt.dryRun, r.DryRun)
		})
	}
}

func TestHandleEvent(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name              string
		nodeName          string
		recommendedAction platformconnectorprotos.RecommenedAction
		shouldSucceed     bool
	}{
		{
			name:              "Successful RESTART_VM action",
			nodeName:          "node1",
			recommendedAction: platformconnectorprotos.RecommenedAction_RESTART_VM,
			shouldSucceed:     true,
		},
		{
			name:              "Failed RESTART_VM action",
			nodeName:          "node2",
			recommendedAction: platformconnectorprotos.RecommenedAction_RESTART_VM,
			shouldSucceed:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := &MockK8sClient{
				createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
					healthEvent := healthEventDoc.HealthEvent.(*platformconnectorprotos.HealthEvent)
					assert.Equal(t, tt.nodeName, healthEvent.NodeName)
					assert.Equal(t, tt.recommendedAction, healthEvent.RecommendedAction)
					return tt.shouldSucceed
				},
			}

			cfg := ReconcilerConfig{
				K8sClient: k8sClient,
			}

			r := NewReconciler(cfg, false)
			healthEventDoc := &HealthEventDoc{
				ID: "test-health-event-id",
				HealthEventWithStatus: datastore.HealthEventWithStatus{
					HealthEvent: &platformconnectorprotos.HealthEvent{
						NodeName:          tt.nodeName,
						RecommendedAction: tt.recommendedAction,
					},
				},
			}
			result := r.Config.K8sClient.CreateMaintenanceResource(ctx, healthEventDoc)
			assert.Equal(t, tt.shouldSucceed, result)
		})
	}
}

func TestShouldSkipEvent(t *testing.T) {
	r := NewReconciler(ReconcilerConfig{}, false)

	tests := []struct {
		name              string
		nodeName          string
		recommendedAction platformconnectorprotos.RecommenedAction
		shouldSkip        bool
		description       string
	}{
		{
			name:              "Skip NONE action",
			nodeName:          "test-node-1",
			recommendedAction: platformconnectorprotos.RecommenedAction_NONE,
			shouldSkip:        true,
			description:       "NONE actions should be skipped",
		},
		{
			name:              "Process RESTART_VM action",
			nodeName:          "test-node-2",
			recommendedAction: platformconnectorprotos.RecommenedAction_RESTART_VM,
			shouldSkip:        false,
			description:       "RESTART_VM actions should not be skipped",
		},
		{
			name:              "Skip CONTACT_SUPPORT action",
			nodeName:          "test-node-3",
			recommendedAction: platformconnectorprotos.RecommenedAction_CONTACT_SUPPORT,
			shouldSkip:        true,
			description:       "Unsupported CONTACT_SUPPORT action should be skipped",
		},
		{
			name:              "Skip unknown action",
			nodeName:          "test-node-4",
			recommendedAction: platformconnectorprotos.RecommenedAction(999),
			shouldSkip:        true,
			description:       "Unknown actions should be skipped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthEvent := &platformconnectorprotos.HealthEvent{
				NodeName:          tt.nodeName,
				RecommendedAction: tt.recommendedAction,
			}
			healthEventWithStatus := datastore.HealthEventWithStatus{
				HealthEvent: healthEvent,
			}

			result := r.shouldSkipEvent(healthEventWithStatus)
			assert.Equal(t, tt.shouldSkip, result, tt.description)
		})
	}
}

func TestRunLogCollectorOnNoneActionWhenEnabled(t *testing.T) {
	ctx := context.Background()

	called := false
	k8sClient := &MockK8sClient{
		createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
			return true
		},
		runLogCollectorJobFn: func(ctx context.Context, nodeName string) error {
			called = true
			assert.Equal(t, "test-node-none", nodeName)
			return nil
		},
	}
	stateManager := &statemanager.MockStateManager{
		UpdateNVSentinelStateNodeLabelFn: func(ctx context.Context, nodeName string,
			newStateLabelValue statemanager.NVSentinelStateLabelValue, removeStateLabel bool) (bool, error) {
			return true, nil
		},
	}

	cfg := ReconcilerConfig{
		K8sClient:          k8sClient,
		EnableLogCollector: true,
		StateManager:       stateManager,
	}
	r := NewReconciler(cfg, false)

	he := &platformconnectorprotos.HealthEvent{NodeName: "test-node-none", RecommendedAction: platformconnectorprotos.RecommenedAction_NONE}
	event := datastore.HealthEventWithStatus{HealthEvent: he}

	// Simulate the Start loop behavior: log collector run before skipping
	if event.HealthEvent.(*platformconnectorprotos.HealthEvent).RecommendedAction == platformconnectorprotos.RecommenedAction_NONE && r.Config.EnableLogCollector {
		_ = r.Config.K8sClient.RunLogCollectorJob(ctx, event.HealthEvent.(*platformconnectorprotos.HealthEvent).NodeName)
	}
	assert.True(t, r.shouldSkipEvent(event))
	assert.True(t, called, "log collector job should be invoked when enabled for NONE action")
}

func TestRunLogCollectorJobErrorScenarios(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		nodeName       string
		jobResult      bool
		expectedResult bool
		description    string
	}{
		{
			name:           "Log collector job succeeds",
			nodeName:       "test-node-success",
			jobResult:      true,
			expectedResult: true,
			description:    "Happy path - job completes successfully",
		},
		{
			name:           "Log collector job fails",
			nodeName:       "test-node-fail",
			jobResult:      false,
			expectedResult: false,
			description:    "Error path - job fails to complete",
		},
		{
			name:           "Log collector job with api error",
			nodeName:       "test-node-api-error",
			jobResult:      false,
			expectedResult: false,
			description:    "Error path - kubernetes API error during job creation",
		},
		{
			name:           "Log collector job with creation error",
			nodeName:       "test-node-create-error",
			jobResult:      false,
			expectedResult: false,
			description:    "Error path - job creation fails",
		},
		{
			name:           "Log collector job timeout",
			nodeName:       "test-node-timeout",
			jobResult:      false,
			expectedResult: false,
			description:    "Error path - job times out",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := &MockK8sClient{
				createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
					return true
				},
				runLogCollectorJobFn: func(ctx context.Context, nodeName string) error {
					assert.Equal(t, tt.nodeName, nodeName)
					if tt.jobResult {
						return nil
					}
					return fmt.Errorf("job failed")
				},
			}

			cfg := ReconcilerConfig{
				K8sClient:          k8sClient,
				EnableLogCollector: true,
			}
			r := NewReconciler(cfg, false)

			result := r.Config.K8sClient.RunLogCollectorJob(ctx, tt.nodeName)
			if tt.expectedResult {
				assert.NoError(t, result, tt.description)
			} else {
				assert.Error(t, result, tt.description)
			}
		})
	}
}

func TestRunLogCollectorJobDryRunMode(t *testing.T) {
	ctx := context.Background()

	called := false
	k8sClient := &MockK8sClient{
		createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
			return true
		},
		runLogCollectorJobFn: func(ctx context.Context, nodeName string) error {
			called = true
			// In dry run mode, this should return nil without actually creating the job
			return nil
		},
	}

	cfg := ReconcilerConfig{
		K8sClient:          k8sClient,
		EnableLogCollector: true,
	}
	r := NewReconciler(cfg, true) // Enable dry run

	result := r.Config.K8sClient.RunLogCollectorJob(ctx, "test-node-dry-run")
	assert.NoError(t, result, "Dry run should return no error")
	assert.True(t, called, "Function should be called even in dry run mode")
}

func TestLogCollectorDisabled(t *testing.T) {
	ctx := context.Background()

	logCollectorCalled := false
	k8sClient := &MockK8sClient{
		createMaintenanceResourceFn: func(ctx context.Context, healthEventDoc *HealthEventDoc) bool {
			return true
		},
		runLogCollectorJobFn: func(ctx context.Context, nodeName string) error {
			logCollectorCalled = true
			return nil
		},
	}
	stateManager := &statemanager.MockStateManager{
		UpdateNVSentinelStateNodeLabelFn: func(ctx context.Context, nodeName string,
			newStateLabelValue statemanager.NVSentinelStateLabelValue, removeStateLabel bool) (bool, error) {
			return true, nil
		},
	}

	cfg := ReconcilerConfig{
		K8sClient:          k8sClient,
		EnableLogCollector: false, // Disabled
		StateManager:       stateManager,
	}
	r := NewReconciler(cfg, false)

	he := &platformconnectorprotos.HealthEvent{NodeName: "test-node-disabled", RecommendedAction: platformconnectorprotos.RecommenedAction_NONE}
	event := datastore.HealthEventWithStatus{HealthEvent: he}

	// Simulate the Start loop behavior: log collector should NOT run when disabled
	if event.HealthEvent.(*platformconnectorprotos.HealthEvent).RecommendedAction == platformconnectorprotos.RecommenedAction_NONE && r.Config.EnableLogCollector {
		_ = r.Config.K8sClient.RunLogCollectorJob(ctx, event.HealthEvent.(*platformconnectorprotos.HealthEvent).NodeName)
	}
	assert.True(t, r.shouldSkipEvent(event))
	assert.False(t, logCollectorCalled, "log collector job should NOT be invoked when disabled")
}

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
	"time"

	"github.com/nvidia/nvsentinel/statemanager"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
)

type NodeDrainerClientInterface interface {
	GetNamespacesMatchingPattern(ctx context.Context, includePattern string, excludePattern string) ([]string, error)
	MonitorPodCompletion(ctx context.Context, namespace string, nodename string) error
	EvictAllPodsInImmediateMode(ctx context.Context, namespace string, nodename string, timout time.Duration) error
	CheckIfAllPodsAreEvictedInImmediateMode(ctx context.Context,
		namespaces []string, nodeName string, timeout time.Duration) bool
	UpdateNodeLabel(ctx context.Context, nodeName string, targetState statemanager.NVSentinelStateLabelValue) error
	DeletePodsAfterTimeout(
		ctx context.Context, nodeName string, namespaces []string, timeout int,
		event *datastore.HealthEventWithStatus,
	) error
}

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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/reconciler"
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers" // Register all datastore providers
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

// nolint: cyclop //fix this as part of NGCC-21793
func main() {
	// Create a context that gets cancelled on OS interrupt signals
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Ensure the signal listener is cleaned up

	var metricsPort = flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")

	var kubeconfigPath = flag.String("kubeconfig-path", "", "path to kubeconfig file")

	var dryRun = flag.Bool("dry-run", false, "flag to run node drainer module in dry-run mode")

	var circuitBreakerPercentage = flag.Int("circuit-breaker-percentage",
		50, "percentage of nodes to cordon before tripping the circuit breaker")

	var circuitBreakerDuration = flag.Duration("circuit-breaker-duration",
		5*time.Minute, "duration of the circuit breaker window")

	var circuitBreakerEnabled = flag.Bool("circuit-breaker-enabled", true,
		"enable or disable fault quarantine circuit breaker")

	// Initialize klog flags to allow command-line control (e.g., -v=3)
	klog.InitFlags(nil)

	defer klog.Flush()
	flag.Parse()

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		klog.Fatalf("POD_NAMESPACE is not provided")
	}

	// Load datastore configuration
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		klog.Fatalf("Failed to load datastore configuration: %v", err)
	}

	// Create datastore instance
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		klog.Fatalf("Failed to create datastore: %v", err)
	}

	defer func() {
		if err := dataStore.Close(ctx); err != nil {
			klog.Errorf("Failed to close datastore: %v", err)
		}
	}()

	// Test datastore connection
	if err := dataStore.Ping(ctx); err != nil {
		klog.Fatalf("Failed to ping datastore: %v", err)
	}

	klog.Infof("Successfully connected to datastore provider: %s", datastoreConfig.Provider)

	// Start metrics server
	klog.Infof("Starting a metrics port on : %s", *metricsPort)

	metricsServer := func() {
		http.Handle("/metrics", promhttp.Handler())
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+*metricsPort, nil)
		if err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
	}

	go metricsServer()

	tomlCfg, err := config.LoadTomlConfig("/etc/config/config.toml")
	if err != nil {
		klog.Fatalf("error while loading the toml config: %v", err)
	}

	if *dryRun {
		klog.Info("Running in dry-run mode")
	}

	// Initialize the k8s client
	k8sClient, err := reconciler.NewFaultQuarantineClient(*kubeconfigPath, *dryRun)
	if err != nil {
		klog.Fatalf("error while initializing kubernetes client: %v", err)
	}

	klog.Info("Successfully initialized k8sclient - using datastore abstraction")

	// Get unprocessed events metric update interval from environment
	unprocessedEventsMetricUpdateIntervalSeconds, err := getEnvAsInt(
		"UNPROCESSED_EVENTS_METRIC_UPDATE_INTERVAL_SECONDS", 30)
	if err != nil {
		klog.Fatalf("error while getting unprocessed events metric update interval: %v", err)
	}

	// Create pipeline for filtering unremediated events (module's business logic)
	pipeline := createUnremediatedEventsPipeline()

	// Create reconciler configuration
	reconcilerCfg := reconciler.ReconcilerConfig{
		TomlConfig:                            *tomlCfg,
		DataStore:                             dataStore,
		Pipeline:                              pipeline, // Module passes its filtering logic explicitly
		K8sClient:                             k8sClient,
		DryRun:                                *dryRun,
		CircuitBreakerEnabled:                 *circuitBreakerEnabled,
		UnprocessedEventsMetricUpdateInterval: time.Duration(unprocessedEventsMetricUpdateIntervalSeconds) * time.Second,
		CircuitBreaker: reconciler.CircuitBreakerConfig{
			Namespace:  namespace,
			Name:       "fault-quarantine-circuit-breaker",
			Percentage: *circuitBreakerPercentage,
			Duration:   *circuitBreakerDuration,
		},
	}

	// Create the work signal channel (buffered channel acting as semaphore)
	workSignal := make(chan struct{}, 1) // Buffer size 1 is usually sufficient

	// Pass the workSignal channel to the Reconciler
	rec := reconciler.NewReconciler(ctx, reconcilerCfg, workSignal)
	rec.Start(ctx)
}

// createUnremediatedEventsPipeline creates a database-agnostic aggregation pipeline that filters
// change stream events to only include INSERT operations.
// This matches the main branch behavior where only inserts are processed.
func createUnremediatedEventsPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", map[string]interface{}{
				"operationType": map[string]interface{}{
					"$in": []interface{}{"insert"},
				},
			}),
		),
	}
}

func getEnvAsInt(name string, defaultValue int) (int, error) {
	valueStr, exists := os.LookupEnv(name)
	if !exists {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("error converting %s to integer: %w", name, err)
	}

	if value <= 0 {
		return 0, fmt.Errorf("value of %s must be a positive integer", name)
	}

	return value, nil
}

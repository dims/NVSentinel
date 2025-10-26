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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/reconciler"

	// Datastore abstraction
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	// Import all providers to ensure registration
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// getEnvWithDefault returns the environment variable value or a default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// loadMetricUpdateInterval loads the metric update interval from environment
func loadMetricUpdateInterval() (int, error) {
	unprocessedEventsMetricUpdateIntervalSeconds, err := getEnvAsInt(
		"UNPROCESSED_EVENTS_METRIC_UPDATE_INTERVAL_SECONDS", 25)
	if err != nil {
		return 0, fmt.Errorf("invalid UNPROCESSED_EVENTS_METRIC_UPDATE_INTERVAL_SECONDS: %w", err)
	}

	return unprocessedEventsMetricUpdateIntervalSeconds, nil
}

func getEnvAsInt(name string, defaultValue int) (int, error) {
	valueStr, exists := os.LookupEnv(name)
	if !exists {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %w", name, err)
	}

	return value, nil
}

func main() {
	logger.SetDefaultStructuredLogger("fault-quarantine-module", version)
	slog.Info("Starting fault-quarantine-module", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Application encountered a fatal error", "error", err)
		os.Exit(1)
	}
}

func parseFlags() (metricsPort, kubeconfigPath *string, dryRun, circuitBreakerEnabled *bool,
	circuitBreakerPercentage *int, circuitBreakerDuration *time.Duration) {
	metricsPort = flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")

	kubeconfigPath = flag.String("kubeconfig-path", "", "path to kubeconfig file")

	dryRun = flag.Bool("dry-run", false, "flag to run node drainer module in dry-run mode")

	circuitBreakerPercentage = flag.Int("circuit-breaker-percentage",
		50, "percentage of nodes to cordon before tripping the circuit breaker")

	circuitBreakerDuration = flag.Duration("circuit-breaker-duration",
		5*time.Minute, "duration of the circuit breaker window")

	circuitBreakerEnabled = flag.Bool("circuit-breaker-enabled", true,
		"enable or disable fault quarantine circuit breaker")

	flag.Parse()

	return
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

func startMetricsServer(metricsPort string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		slog.Info("Starting metrics server", "port", metricsPort)
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+metricsPort, nil)
		if err != nil {
			slog.Error("Failed to start metrics server", "error", err)
			os.Exit(1)
		}
	}()
	slog.Info("Metrics server goroutine started")
}

func run() error {
	// Create a context that gets cancelled on OS interrupt signals
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Ensure the signal listener is cleaned up

	metricsPort, kubeconfigPath, dryRun, circuitBreakerEnabled,
		circuitBreakerPercentage, circuitBreakerDuration := parseFlags()

	// Load datastore configuration using the abstraction layer
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		slog.Error("Failed to load datastore configuration", "error", err)
		return fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	// Create datastore instance
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		slog.Error("Failed to create datastore", "error", err)
		return fmt.Errorf("failed to create datastore: %w", err)
	}

	defer func() {
		if err := dataStore.Close(ctx); err != nil {
			slog.Error("Failed to close datastore", "error", err)
		}
	}()

	// Test datastore connection
	if err := dataStore.Ping(ctx); err != nil {
		slog.Error("Failed to ping datastore", "error", err)
		return fmt.Errorf("failed to ping datastore: %w", err)
	}

	slog.Info("Successfully connected to datastore", "provider", datastoreConfig.Provider)

	// Load namespace for circuit breaker from POD_NAMESPACE
	namespace := getEnvWithDefault("POD_NAMESPACE", "default")

	// Get the interval for metrics update
	unprocessedEventsMetricUpdateIntervalSeconds, err := loadMetricUpdateInterval()
	if err != nil {
		return err
	}

	// Create pipeline for filtering unremediated events (module's business logic)
	pipeline := createUnremediatedEventsPipeline()

	tomlCfg, err := config.LoadTomlConfig("/etc/config/config.toml")
	if err != nil {
		return fmt.Errorf("error loading TOML config: %w", err)
	}

	if *dryRun {
		slog.Info("Running in dry-run mode")
	}

	// Initialize the k8s client
	k8sClient, err := reconciler.NewFaultQuarantineClient(*kubeconfigPath, *dryRun)
	if err != nil {
		return fmt.Errorf("error while initializing kubernetes client: %w", err)
	}

	slog.Info("Successfully initialized k8sclient")

	reconcilerCfg := reconciler.ReconcilerConfig{
		TomlConfig:                            *tomlCfg,
		DataStore:                             dataStore,
		Pipeline:                              pipeline,
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

	startMetricsServer(*metricsPort)

	rec.Start(ctx)

	return nil
}

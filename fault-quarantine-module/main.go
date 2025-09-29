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
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers" // Register all datastore providers

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

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

func loadEnvConfig() (namespace string, err error) {
	namespace = os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		return "", fmt.Errorf("POD_NAMESPACE is not provided")
	}

	return namespace, nil
}

func loadUnprocessedEventsMetricUpdateInterval() (int, error) {
	unprocessedEventsMetricUpdateIntervalSeconds, err := getEnvAsInt(
		"UNPROCESSED_EVENTS_METRIC_UPDATE_INTERVAL_SECONDS", 25)
	if err != nil {
		return 0, fmt.Errorf("invalid UNPROCESSED_EVENTS_METRIC_UPDATE_INTERVAL_SECONDS: %w", err)
	}

	return unprocessedEventsMetricUpdateIntervalSeconds, nil
}

func createPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", datastore.D(
					datastore.E("$in", datastore.A("insert")),
				)),
			)),
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

	namespace, err := loadEnvConfig()
	if err != nil {
		return err
	}

	unprocessedEventsMetricUpdateIntervalSeconds, err := loadUnprocessedEventsMetricUpdateInterval()
	if err != nil {
		return err
	}

	// Load datastore configuration (using new abstraction)
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		return fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	// Create datastore instance (using new abstraction)
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}

	defer func() {
		if err := dataStore.Close(ctx); err != nil {
			slog.Error("Failed to close datastore", "error", err)
		}
	}()

	// Test datastore connection
	if err := dataStore.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping datastore: %w", err)
	}

	slog.Info("Successfully connected to datastore", "provider", datastoreConfig.Provider)

	pipeline := createPipeline()

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
		TomlConfig:            *tomlCfg,
		DataStore:             dataStore, // Use new datastore abstraction
		Pipeline:              pipeline,  // Use new pipeline types
		K8sClient:             k8sClient,
		DryRun:                *dryRun,
		CircuitBreakerEnabled: *circuitBreakerEnabled,
		UnprocessedEventsMetricUpdateInterval: time.Duration(unprocessedEventsMetricUpdateIntervalSeconds) *
			time.Second,
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

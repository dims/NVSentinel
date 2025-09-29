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

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/nvidia/nvsentinel/fault-remediation-module/pkg/reconciler"
	"github.com/nvidia/nvsentinel/statemanager"
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

type config struct {
	namespace               string
	version                 string
	apiGroup                string
	templateMountPath       string
	templateFileName        string
	metricsPort             string
	kubeconfigPath          string
	dryRun                  bool
	enableLogCollector      bool
	updateMaxRetries        int
	updateRetryDelaySeconds int
}

// parseFlags parses command-line flags and returns a config struct.
func parseFlags() *config {
	cfg := &config{}

	flag.StringVar(&cfg.metricsPort, "metrics-port", "2112", "port to expose Prometheus metrics on")
	flag.StringVar(&cfg.kubeconfigPath, "kubeconfig-path", "", "path to kubeconfig file")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "flag to run node drainer module in dry-run mode")
	flag.IntVar(&cfg.updateMaxRetries, "update-max-retries", 5,
		"maximum attempts to update remediation status per event")
	flag.IntVar(&cfg.updateRetryDelaySeconds, "update-retry-delay-seconds", 10,
		"delay in seconds between remediation status update retries")
	flag.Parse()

	return cfg
}

func getRequiredEnvVars() (*config, error) {
	cfg := &config{}

	requiredVars := map[string]*string{
		"MAINTENANCE_NAMESPACE": &cfg.namespace,
		"MAINTENANCE_VERSION":   &cfg.version,
		"MAINTENANCE_API_GROUP": &cfg.apiGroup,
		"TEMPLATE_MOUNT_PATH":   &cfg.templateMountPath,
		"TEMPLATE_FILE_NAME":    &cfg.templateFileName,
	}

	for envVar, ptr := range requiredVars {
		*ptr = os.Getenv(envVar)
		if *ptr == "" {
			return nil, fmt.Errorf("%s is not provided", envVar)
		}
	}

	// Feature flag: default disabled; only "true" enables it
	if v := os.Getenv("ENABLE_LOG_COLLECTOR"); v == "true" {
		cfg.enableLogCollector = true
	}

	slog.Info("Configuration loaded",
		"namespace", cfg.namespace,
		"version", cfg.version,
		"apiGroup", cfg.apiGroup,
		"templateMountPath", cfg.templateMountPath,
		"templateFileName", cfg.templateFileName)

	return cfg, nil
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
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			slog.Error("Metrics server failed", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("Metrics server goroutine started")
}

func getMongoPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", "update"),
				datastore.E("$or", datastore.A(
					// Watch for quarantine events (for remediation)
					datastore.D(
						datastore.E("fullDocument.healtheventstatus.userpodsevictionstatus.status", datastore.D(
							datastore.E("$in", datastore.A(datastore.StatusSucceeded, datastore.AlreadyDrained)),
						)),
						datastore.E("fullDocument.healtheventstatus.nodequarantined", datastore.D(
							datastore.E("$in", datastore.A(datastore.Quarantined, datastore.AlreadyQuarantined)),
						)),
					),
					// Watch for unquarantine events (for annotation cleanup)
					datastore.D(
						datastore.E("fullDocument.healtheventstatus.nodequarantined", datastore.UnQuarantined),
						datastore.E("fullDocument.healtheventstatus.userpodsevictionstatus.status", datastore.StatusSucceeded),
					),
				)),
			)),
		),
	}
}

func run() error {
	ctx := context.Background()

	// Parse flags and get configuration
	cfg := parseFlags()

	// Get required environment variables
	envCfg, err := getRequiredEnvVars()
	if err != nil {
		return fmt.Errorf("failed to get required environment variables: %w", err)
	}

	// Load datastore configuration
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		return fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	// Create datastore instance
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

	// Get MongoDB pipeline
	pipeline := getMongoPipeline()

	// Initialize k8s client
	k8sClient, clientSet, err := reconciler.NewK8sClient(cfg.kubeconfigPath, cfg.dryRun, reconciler.TemplateData{
		Namespace:         envCfg.namespace,
		Version:           envCfg.version,
		ApiGroup:          envCfg.apiGroup,
		TemplateMountPath: envCfg.templateMountPath,
		TemplateFileName:  envCfg.templateFileName,
	})
	if err != nil {
		return fmt.Errorf("error while initializing kubernetes client: %w", err)
	}

	slog.Info("Successfully initialized k8sclient")

	// Initialize and start reconciler
	reconcilerCfg := reconciler.ReconcilerConfig{
		DataStore:          dataStore, // Use new datastore abstraction
		Pipeline:           pipeline,  // Use new pipeline types
		K8sClient:          k8sClient,
		StateManager:       statemanager.NewStateManager(clientSet),
		EnableLogCollector: envCfg.enableLogCollector,
	}

	reconciler := reconciler.NewReconciler(reconcilerCfg, cfg.dryRun)

	startMetricsServer(cfg.metricsPort)

	reconciler.Start(ctx)

	return nil
}

func main() {
	logger.SetDefaultStructuredLogger("fault-remediation-module", version)
	slog.Info("Starting fault-remediation-module", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

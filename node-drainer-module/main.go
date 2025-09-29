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
	"syscall"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/nvidia/nvsentinel/node-drainer-module/pkg/config"
	"github.com/nvidia/nvsentinel/node-drainer-module/pkg/reconciler"
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

func main() {
	logger.SetDefaultStructuredLogger("node-drainer-module", version)
	slog.Info("Starting node-drainer-module", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Node drainer module exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var metricsPort = flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")

	// Legacy MongoDB flag - kept for backward compatibility but ignored
	var _ = flag.String(
		"mongo-client-cert-mount-path", "",
		"DEPRECATED: MongoDB client cert mount path (ignored, use datastore abstraction instead)",
	)

	var kubeconfigPath = flag.String("kubeconfig-path", "", "path to kubeconfig file")

	var tomlConfigPath = flag.String("config-path", "/etc/config/config.toml",
		"path where the node drainer config file is present")

	var dryRun = flag.Bool("dry-run", false, "flag to run node drainer module in dry-run mode")

	flag.Parse()

	// Start metrics server
	slog.Info("Starting metrics server", "port", *metricsPort)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+*metricsPort, nil)
		if err != nil {
			slog.Error("Failed to start metrics server", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("Metrics server goroutine started")

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

	tomlCfg, err := config.LoadTomlConfig(*tomlConfigPath)
	if err != nil {
		return fmt.Errorf("error while loading the toml config: %w", err)
	}

	if *dryRun {
		slog.Info("Running in dry-run mode")
	}

	// Initialize the k8s client with pod timeout configuration
	k8sClient, clientSet, err := reconciler.NewNodeDrainerClient(*kubeconfigPath, *dryRun, &tomlCfg.NotReadyTimeoutMinutes)
	if err != nil {
		return fmt.Errorf("error while initializing kubernetes client: %w", err)
	}

	slog.Info("Successfully initialized k8sclient")

	// Create pipeline to watch for UPDATE events where nodequarantined changes
	pipeline := createNodeDrainerPipeline()

	reconcilerCfg := reconciler.ReconcilerConfig{
		TomlConfig:   *tomlCfg,
		DataStore:    dataStore,
		Pipeline:     pipeline,
		K8sClient:    k8sClient,
		StateManager: statemanager.NewStateManager(clientSet),
	}

	reconciler := reconciler.NewReconciler(reconcilerCfg, *dryRun)
	reconciler.Start(ctx)

	return nil
}

// createNodeDrainerPipeline creates a database-agnostic aggregation pipeline that filters
// change stream events to only include UPDATE operations where nodequarantined changes.
// Note: The MongoDB update sets flat fields (not nested), so we match "nodeQuarantined"
// not "healtheventstatus.nodequarantined"
// See store-client-sdk/pkg/datastore/providers/mongodb/datastore.go:666
func createNodeDrainerPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", "update"),
				datastore.E("$or", datastore.A(
					datastore.D(datastore.E("updateDescription.updatedFields.nodeQuarantined", datastore.Quarantined)),
					datastore.D(datastore.E("updateDescription.updatedFields.nodeQuarantined", datastore.UnQuarantined)),
				)),
			)),
		),
	}
}

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
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/reconciler"
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	logger.SetDefaultStructuredLogger("health-events-analyzer", version)
	slog.Info("Starting health-events-analyzer", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func loadDatastoreConfig(ctx context.Context) (datastore.DataStore, *datastore.DataStoreConfig, error) {
	// Load datastore configuration using the abstraction layer
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	// Create datastore instance
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Test datastore connection
	if err := dataStore.Ping(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ping datastore: %w", err)
	}

	return dataStore, datastoreConfig, nil
}

func connectToPlatform(socket string) (*publisher.PublisherConfig, *grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	conn, err := grpc.NewClient(socket, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial platform connector UDS %s: %w", socket, err)
	}

	platformConnectorClient := pb.NewPlatformConnectorClient(conn)
	pub := publisher.NewPublisher(platformConnectorClient)

	return pub, conn, nil
}

func createPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", "insert"),
				datastore.E("fullDocument.healthevent.isfatal", false),
				datastore.E("fullDocument.healthevent.ishealthy", false),
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
	ctx := context.Background()

	metricsPort := flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")
	socket := flag.String("socket", "unix:///var/run/nvsentinel.sock", "unix domain socket")

	flag.Parse()

	dataStore, datastoreConfig, err := loadDatastoreConfig(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := dataStore.Close(ctx); err != nil {
			slog.Error("Failed to close datastore", "error", err)
		}
	}()

	slog.Info("Successfully connected to datastore", "provider", datastoreConfig.Provider)

	pipeline := createPipeline()

	pub, conn, err := connectToPlatform(*socket)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Parse the TOML content
	tomlConfig, err := config.LoadTomlConfig("/etc/config/config.toml")
	if err != nil {
		return fmt.Errorf("error loading TOML config: %w", err)
	}

	// Get collection name from environment (with defaults)
	collection := getEnvWithDefault("DATASTORE_COLLECTION_NAME", "HealthEvents")

	reconcilerCfg := reconciler.HealthEventsAnalyzerReconcilerConfig{
		DataStore:                 dataStore, // Use new datastore abstraction
		Pipeline:                  pipeline,  // Use new pipeline types
		HealthEventsAnalyzerRules: tomlConfig,
		Publisher:                 pub,
		CollectionName:            collection,
	}

	rec := reconciler.NewReconciler(reconcilerCfg)

	startMetricsServer(*metricsPort)

	rec.Start(ctx)

	return nil
}

// getEnvWithDefault returns the environment variable value or a default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

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
	"net/http"
	"os"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/reconciler"

	// Datastore abstraction
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"

	// Import all providers to ensure registration
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog"
)

//nolint:cyclop // todo
func main() {
	ctx := context.Background()

	var metricsPort = flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")

	var socket = flag.String("socket", "unix:///var/run/nvsentinel.sock", "unix domain socket")

	// Initialize klog flags to allow command-line control (e.g., -v=3)
	klog.InitFlags(nil)

	defer klog.Flush()
	flag.Parse()

	// Load datastore configuration using the abstraction layer
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

	// Get collection name from environment (with defaults)
	collection := getEnvWithDefault("DATASTORE_COLLECTION_NAME", "HealthEvents")

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(*socket, opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	platformConnectorClient := pb.NewPlatformConnectorClient(conn)
	publisher := publisher.NewPublisher(platformConnectorClient)

	// Parse the TOML content
	tomlConfig, err := config.LoadTomlConfig("/etc/config/config.toml")
	if err != nil {
		klog.Fatalf("Failed to load config file: %v", err)
	}

	// Create pipeline to watch for INSERT events of non-fatal unhealthy events
	pipeline := createHealthEventsAnalyzerPipeline()

	reconcilerCfg := reconciler.HealthEventsAnalyzerReconcilerConfig{
		HealthEventsAnalyzerRules: tomlConfig,
		Publisher:                 publisher,
		DataStore:                 dataStore,
		CollectionName:            collection,
		Pipeline:                  pipeline,
	}

	reconciler := reconciler.NewReconciler(reconcilerCfg)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+*metricsPort, nil)
		if err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	reconciler.Start(ctx)
}

// createHealthEventsAnalyzerPipeline creates a database-agnostic aggregation pipeline that filters
// change stream events to only include INSERT operations of non-fatal, unhealthy events.
// Note: InsertHealthEvents stores the eventWithStatus under "document" key
// See store-client-sdk/pkg/datastore/providers/mongodb/datastore.go:633
func createHealthEventsAnalyzerPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", "insert"),
				datastore.E("fullDocument.document.healthevent.isfatal", false),
				datastore.E("fullDocument.document.healthevent.ishealthy", false),
			)),
		),
	}
}

// getEnvWithDefault returns the environment variable value or a default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

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
	"log"
	"net/http"
	"os"

	"github.com/nvidia/nvsentinel/fault-remediation-module/pkg/reconciler"
	"github.com/nvidia/nvsentinel/statemanager"
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers" // Register all datastore providers
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog"
)

type config struct {
	namespace          string
	version            string
	apiGroup           string
	templateMountPath  string
	templateFileName   string
	metricsPort        string
	kubeconfigPath     string
	dryRun             bool
	enableLogCollector bool
}

func parseFlags() *config {
	cfg := &config{}

	flag.StringVar(&cfg.metricsPort, "metrics-port", "2112", "port to expose Prometheus metrics on")
	flag.StringVar(&cfg.kubeconfigPath, "kubeconfig-path", "", "path to kubeconfig file")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "flag to run node drainer module in dry-run mode")

	// Initialize klog flags (including -v for verbosity)
	klog.InitFlags(nil)

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

	log.Printf("namespace: %s, version: %s, apigroup: %s, templateMountPath: %s, templateFileName: %s",
		cfg.namespace, cfg.version, cfg.apiGroup, cfg.templateMountPath, cfg.templateFileName)

	return cfg, nil
}

func startMetricsServer(metricsPort string) {
	klog.Infof("Starting a metrics port on : %s", metricsPort)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		//nolint:gosec // G114: Ignoring the use of http.ListenAndServe without timeouts
		err := http.ListenAndServe(":"+metricsPort, nil)
		if err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
	}()
}
func main() {
	ctx := context.Background()

	// Parse flags and get configuration
	cfg := parseFlags()

	// Get required environment variables
	envCfg, err := getRequiredEnvVars()
	if err != nil {
		log.Fatalf("Failed to get required environment variables: %v", err)
	}

	// Start metrics server
	startMetricsServer(cfg.metricsPort)

	// Load datastore configuration
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		log.Fatalf("Failed to load datastore configuration: %v", err)
	}

	// Create datastore instance
	dataStore, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		log.Fatalf("Failed to create datastore: %v", err)
	}

	defer func() {
		if err := dataStore.Close(ctx); err != nil {
			log.Printf("Failed to close datastore: %v", err)
		}
	}()

	// Test datastore connection
	if err := dataStore.Ping(ctx); err != nil {
		log.Printf("Failed to ping datastore: %v", err)
		return
	}

	log.Printf("Successfully connected to datastore provider: %s", datastoreConfig.Provider)

	// Initialize k8s client
	k8sClient, clientSet, err := reconciler.NewK8sClient(cfg.kubeconfigPath, cfg.dryRun, reconciler.TemplateData{
		Namespace:         envCfg.namespace,
		Version:           envCfg.version,
		ApiGroup:          envCfg.apiGroup,
		TemplateMountPath: envCfg.templateMountPath,
		TemplateFileName:  envCfg.templateFileName,
	})
	if err != nil {
		log.Printf("error while initializing kubernetes client: %v", err)
		return
	}

	log.Println("Successfully initialized k8sclient")

	// Create pipeline to watch for UPDATE events where drain succeeded and node is quarantined
	pipeline := createFaultRemediationPipeline()

	// Initialize and start reconciler
	reconcilerCfg := reconciler.ReconcilerConfig{
		DataStore:          dataStore,
		Pipeline:           pipeline,
		K8sClient:          k8sClient,
		StateManager:       statemanager.NewStateManager(clientSet),
		EnableLogCollector: envCfg.enableLogCollector,
	}

	reconciler := reconciler.NewReconciler(reconcilerCfg, cfg.dryRun)

	reconciler.Start(ctx)
}

// createFaultRemediationPipeline creates a database-agnostic aggregation pipeline that filters
// change stream events to only include UPDATE operations where user pods eviction succeeded.
// Note: Node-drainer uses UpdateHealthEventStatus which sets flat field "userPodsEvictionStatus"
// See store-client-sdk/pkg/datastore/providers/mongodb/datastore.go:667
func createFaultRemediationPipeline() datastore.Pipeline {
	return datastore.Pipeline{
		datastore.D(
			datastore.E("$match", datastore.D(
				datastore.E("operationType", "update"),
				datastore.E("$and", datastore.A(
					datastore.D(datastore.E("updateDescription.updatedFields.userPodsEvictionStatus", "Succeeded")),
					datastore.D(datastore.E("fullDocument.nodeQuarantined", datastore.Quarantined)),
				)),
			)),
		),
	}
}

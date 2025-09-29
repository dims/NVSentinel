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
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/metrics"
	trigger "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/triggerengine"
	pb "github.com/nvidia/nvsentinel/platform-connectors/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers/mongodb"
)

const (
	defaultConfigPathSidecar  = "/etc/config/config.toml"
	defaultUdsPathSidecar     = "/run/nvsentinel/nvsentinel.sock"
	defaultMetricsPortSidecar = "2113"
)

type appConfig struct {
	configPath  string
	udsPath     string
	metricsPort string
}

func parseFlags() *appConfig {
	cfg := &appConfig{}
	// Command-line flags
	flag.StringVar(&cfg.configPath, "config", defaultConfigPathSidecar, "Path to the TOML configuration file.")
	flag.StringVar(&cfg.udsPath, "uds-path", defaultUdsPathSidecar, "Path to the Platform Connector UDS socket.")
	flag.StringVar(&cfg.metricsPort, "metrics-port", defaultMetricsPortSidecar, "Port for the sidecar Prometheus metrics.")

	// Initialise klog and parse flags
	klog.InitFlags(nil)

	// Parse flags after initialising klog
	flag.Parse()

	return cfg
}

func logStartupInfo(cfg *appConfig) {
	klog.Infof("Starting Quarantine Trigger Engine Sidecar...")
	klog.Infof("Using configuration file: %s", cfg.configPath)
	klog.Infof("Platform Connector UDS Path: %s", cfg.udsPath)
	klog.Infof("Exposing sidecar metrics on port: %s", cfg.metricsPort)
	klog.V(2).Infof("Klog verbosity level is set based on the -v flag for sidecar.")
}

func startMetricsServer(metricsPort string) {
	// Start metrics endpoint in a separate goroutine
	go func() {
		listenAddress := fmt.Sprintf(":%s", metricsPort)
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		server := &http.Server{
			Addr:         listenAddress,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
		}

		klog.Infof("Metrics server (sidecar) starting to listen on %s", listenAddress)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Errorf("Metrics server (sidecar) failed: %v", err)
		}

		klog.Info("Metrics server (sidecar) stopped.")
	}()
}

func setupUDSConnection(udsPath string) (*grpc.ClientConn, pb.PlatformConnectorClient) {
	klog.Infof("Sidecar attempting to connect to Platform Connector UDS at: unix:%s", udsPath)
	target := fmt.Sprintf("unix:%s", udsPath)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		metrics.TriggerUDSSendErrors.Inc()
		klog.Fatalf("Sidecar failed to dial Platform Connector UDS %s: %v", target, err)
	}

	klog.Info("Sidecar successfully connected to Platform Connector UDS.")

	return conn, pb.NewPlatformConnectorClient(conn)
}

func setupKubernetesClient() kubernetes.Interface {
	var restCfg *rest.Config

	var err error

	restCfg, err = rest.InClusterConfig()
	if err != nil {
		klog.Warningf("Trigger Engine: failed to obtain in-cluster Kubernetes config: %v", err)
		return nil
	}

	k8sClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		klog.Errorf("Trigger Engine: failed to create Kubernetes clientset: %v", err)
		return nil
	}

	klog.Info("Trigger Engine: Kubernetes clientset initialized successfully for node readiness checks.")

	return k8sClient
}

func main() {
	appCfg := parseFlags()

	defer klog.Flush()

	logStartupInfo(appCfg)

	cfg, err := config.LoadConfig(appCfg.configPath)
	if err != nil {
		klog.Fatalf("Failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startMetricsServer(appCfg.metricsPort)

	// Load datastore configuration based on environment variables
	datastoreConfig, err := loadDatastoreConfig()
	if err != nil {
		klog.Fatalf("Failed to load datastore configuration: %v", err)
	}

	store, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		klog.Fatalf("Failed to initialize datastore for sidecar: %v", err)
	}

	klog.Infof("Successfully connected to datastore provider: %s (sidecar)", datastoreConfig.Provider)

	conn, platformConnectorClient := setupUDSConnection(appCfg.udsPath)
	defer func() {
		klog.Info("Closing UDS connection for sidecar.")

		if errClose := conn.Close(); errClose != nil {
			klog.Errorf("Error closing sidecar UDS connection: %v", errClose)
		}
	}()

	k8sClient := setupKubernetesClient()

	engine := trigger.NewEngine(cfg, store, platformConnectorClient, k8sClient)

	klog.Info("Trigger engine starting...")
	engine.Start(ctx) // This is blocking

	klog.Info("Quarantine Trigger Engine Sidecar shut down.")
}

// loadDatastoreConfig loads datastore configuration from environment variables (same as main.go)
func loadDatastoreConfig() (*datastore.DataStoreConfig, error) {
	config := &datastore.DataStoreConfig{}

	// Get provider from environment variable (defaults to MongoDB for backward compatibility)
	providerStr := os.Getenv("DATASTORE_PROVIDER")
	if providerStr == "" {
		klog.Infof("DATASTORE_PROVIDER not set, defaulting to MongoDB for backward compatibility")

		config.Provider = datastore.ProviderMongoDB
	} else {
		config.Provider = datastore.DataStoreProvider(providerStr)
	}

	// Load connection configuration based on provider
	switch config.Provider {
	case datastore.ProviderMongoDB:
		config.Connection = datastore.ConnectionConfig{
			Host:     os.Getenv("MONGODB_URI"), // MongoDB uses URI format
			Database: getEnvWithDefault("MONGODB_DATABASE_NAME", "nvsentinel"),
		}
		if config.Connection.Host == "" {
			return nil, fmt.Errorf("MONGODB_URI environment variable is required for MongoDB provider")
		}

	default:
		return nil, fmt.Errorf("unsupported datastore provider: %s", config.Provider)
	}

	klog.Infof("Loaded datastore configuration for provider: %s", config.Provider)

	return config, nil
}

// getEnvWithDefault returns environment variable value or default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

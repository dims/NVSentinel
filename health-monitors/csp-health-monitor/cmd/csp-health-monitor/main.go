// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	klog "k8s.io/klog/v2"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	awsclient "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/aws"
	gcpclient "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/gcp"
	eventpkg "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/event"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/metrics"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers/mongodb"
)

const (
	defaultConfigPath  = "/etc/config/config.toml"
	defaultKubeconfig  = ""
	defaultMetricsPort = "2112"
	eventChannelSize   = 100
)

// startActiveMonitorAndLog starts the provided CSP monitor in a new goroutine
// and logs its lifecycle and any runtime errors.
func startActiveMonitorAndLog(
	ctx context.Context,
	wg *sync.WaitGroup,
	activeMonitor csp.Monitor,
	eventChan chan<- model.MaintenanceEvent,
) {
	if activeMonitor == nil {
		// If no monitor is configured, the application cannot perform its core
		// function.
		klog.Fatalf("No active CSP monitor configured or enabled. Application cannot start.")

		return
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		klog.Infof("Starting active monitor: %s", activeMonitor.GetName())
		monitorErr := activeMonitor.StartMonitoring(ctx, eventChan)

		if monitorErr != nil {
			if !errors.Is(monitorErr, context.Canceled) && !errors.Is(monitorErr, context.DeadlineExceeded) {
				metrics.CSPMonitorErrors.WithLabelValues(string(activeMonitor.GetName()), "runtime_error").Inc()
				klog.Fatalf("Monitor %s stopped with critical error: %v", activeMonitor.GetName(), monitorErr)
			} else {
				klog.Infof("Monitor %s shut down due to context: %v", activeMonitor.GetName(), monitorErr)
			}
		} else {
			klog.Infof("Monitor %s shut down cleanly.", string(activeMonitor.GetName()))
		}
	}()
}

func main() {
	configPath := flag.String("config", defaultConfigPath, "Path to the TOML configuration file.")
	metricsPort := flag.String("metrics-port", defaultMetricsPort, "Port to expose Prometheus metrics on.")
	kubeconfig := flag.String(
		"kubeconfig",
		defaultKubeconfig,
		"Path to a kubeconfig file. Only required if running out-of-cluster.",
	)

	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	klog.Infof("Starting CSP Health Monitor (Main Container)...")

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		klog.Fatalf("Failed to load configuration from %s: %v", *configPath, err)
	}

	effectiveKubeconfigPath := *kubeconfig

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Load datastore configuration based on environment variables
	datastoreConfig, err := loadDatastoreConfig()
	if err != nil {
		klog.Fatalf("Failed to load datastore configuration: %v", err)
	}

	store, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		klog.Fatalf("Failed to initialize datastore: %v", err)
	}

	klog.Infof("Successfully connected to datastore provider: %s", datastoreConfig.Provider)

	eventChan := make(chan model.MaintenanceEvent, eventChannelSize)
	// Processor is lightweight; it already encapsulates required dependencies.
	eventProcessor := eventpkg.NewProcessor(cfg, store)
	if eventProcessor == nil {
		klog.Fatalf("Failed to initialize event processor")
	}

	klog.Info("Event processor initialized successfully.")

	activeMonitor := initActiveMonitor(
		ctx,
		cfg,
		effectiveKubeconfigPath,
		store,
	) // Pass kubeconfigPath for clients to init their own k8s clients

	var wg sync.WaitGroup

	startActiveMonitorAndLog(ctx, &wg, activeMonitor, eventChan)

	wg.Add(1)

	go func() {
		defer wg.Done()
		runEventProcessorLoop(ctx, eventChan, eventProcessor)
		klog.Info("Event processing loop stopped.")
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		startMetricsServer(*metricsPort)
	}()

	klog.Info("CSP Health Monitor (Main Container) components started successfully.")
	<-ctx.Done()
	klog.Info("Shutdown signal received by main monitor. Waiting for components to shut down gracefully...")
	wg.Wait()
	klog.Info("CSP Health Monitor (Main Container) shut down completed.")
}

// loadDatastoreConfig loads datastore configuration from environment variables
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

// initActiveMonitor instantiates the appropriate CSP monitor (GCP/AWS) based on
// the supplied configuration. It returns nil when no CSP is enabled.
func initActiveMonitor(
	ctx context.Context,
	cfg *config.Config,
	kubeconfigPath string,
	store datastore.DataStore,
) csp.Monitor {
	if cfg.GCP.Enabled {
		klog.Info("GCP configuration is enabled.")

		gcpMonitor, err := gcpclient.NewClient(ctx, cfg.GCP, cfg.ClusterName, kubeconfigPath, store)
		if err != nil {
			metrics.CSPMonitorErrors.WithLabelValues(string(model.CSPGCP), "init_error").Inc()
			klog.Errorf("Failed to initialize GCP monitor: %v. GCP will not be monitored.", err)

			return nil
		}

		klog.Infof("GCP monitor initialized (Project: %s)", cfg.GCP.TargetProjectID)

		return gcpMonitor
	}

	if cfg.AWS.Enabled {
		klog.Info("AWS configuration is enabled.")

		awsMonitor, err := awsclient.NewClient(ctx, cfg.AWS, cfg.ClusterName, kubeconfigPath, store)
		if err != nil {
			metrics.CSPMonitorErrors.WithLabelValues(string(model.CSPAWS), "init_error").Inc()
			klog.Errorf("Failed to initialize AWS monitor: %v. AWS will not be monitored.", err)

			return nil
		}

		klog.Infof("AWS monitor initialized (Account: %s, Region: %s)", cfg.AWS.AccountID, cfg.AWS.Region)

		return awsMonitor
	}

	klog.Info("No CSP is explicitly enabled in the configuration (GCP or AWS).")

	return nil
}

// startMetricsServer exposes Prometheus metrics for the main container.
func startMetricsServer(port string) {
	listenAddress := fmt.Sprintf(":%s", port)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         listenAddress,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	klog.Infof("Metrics server (main monitor) starting to listen on %s/metrics", listenAddress)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		klog.Fatalf("Metrics server (main monitor) failed: %v", err)
	}

	klog.Info("Metrics server (main monitor) stopped.")
}

// runEventProcessorLoop consumes normalized events from eventChan and hands
// them to the datastore-backed Processor until the context is cancelled.
func runEventProcessorLoop(
	ctx context.Context,
	eventChan <-chan model.MaintenanceEvent,
	processor *eventpkg.Processor,
) {
	klog.Info("Starting event processing worker loop (main monitor)...")

	for {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping event processing worker loop (main monitor).")
			return
		case receivedEvent, ok := <-eventChan:
			if !ok {
				klog.Info("Event channel closed, stopping event processing worker loop (main monitor).")
				return
			}

			metrics.MainEventsReceived.WithLabelValues(string(receivedEvent.CSP)).Inc()
			klog.V(1).
				Infof("Processor received event: %s (CSP: %s, Node: %s, Status: %s)",
					receivedEvent.EventID, receivedEvent.CSP, receivedEvent.NodeName, receivedEvent.Status)

			start := time.Now()
			err := processor.ProcessEvent(ctx, &receivedEvent)
			duration := time.Since(start).Seconds()
			metrics.MainEventProcessingDuration.WithLabelValues(string(receivedEvent.CSP)).Observe(duration)

			if err != nil {
				metrics.MainProcessingErrors.WithLabelValues(string(receivedEvent.CSP), "process_event").Inc()
				klog.Errorf(
					"Error processing event %s (Node: %s): %v",
					receivedEvent.EventID,
					receivedEvent.NodeName,
					err,
				)
			} else {
				metrics.MainEventsProcessedSuccess.WithLabelValues(string(receivedEvent.CSP)).Inc()
				klog.V(2).Infof("Successfully processed event %s", receivedEvent.EventID)
			}
		}
	}
}

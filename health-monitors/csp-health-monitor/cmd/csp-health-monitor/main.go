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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	awsclient "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/aws"
	gcpclient "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/gcp"
	eventpkg "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/event"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/metrics"
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers"
)

const (
	defaultConfigPath  = "/etc/config/config.toml"
	defaultKubeconfig  = ""
	defaultMetricsPort = "2112"
	eventChannelSize   = 100
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
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
		slog.Error("No active CSP monitor configured or enabled. Application cannot start.")

		return
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		slog.Info("Starting active monitor", "name", activeMonitor.GetName())
		monitorErr := activeMonitor.StartMonitoring(ctx, eventChan)

		if monitorErr != nil {
			if !errors.Is(monitorErr, context.Canceled) && !errors.Is(monitorErr, context.DeadlineExceeded) {
				metrics.CSPMonitorErrors.WithLabelValues(string(activeMonitor.GetName()), "runtime_error").Inc()
				slog.Error("Monitor stopped with critical error", "name", activeMonitor.GetName(), "error", monitorErr)
			} else {
				slog.Info("Monitor shut down due to context", "name", activeMonitor.GetName(), "error", monitorErr)
			}
		} else {
			slog.Info("Monitor shut down cleanly", "name", activeMonitor.GetName())
		}
	}()
}

// main is the entry point for the CSP Health Monitor application.
func main() {
	logger.SetDefaultStructuredLogger("csp-health-monitor", version)
	slog.Info("Starting csp-health-monitor", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("CSP Health Monitor exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", defaultConfigPath, "Path to the TOML configuration file.")
	metricsPort := flag.String("metrics-port", defaultMetricsPort, "Port to expose Prometheus metrics on.")
	kubeconfig := flag.String(
		"kubeconfig",
		defaultKubeconfig,
		"Path to a kubeconfig file. Only required if running out-of-cluster.",
	)

	flag.Parse()

	slog.Info("Starting CSP Health Monitor (Main Container)...")

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration from %s: %w", *configPath, err)
	}

	effectiveKubeconfigPath := *kubeconfig

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Load datastore configuration using the store-client-sdk
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		return fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	store, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize datastore: %w", err)
	}

	slog.Info("Datastore initialized successfully", "provider", datastoreConfig.Provider)

	eventChan := make(chan model.MaintenanceEvent, eventChannelSize)
	// Processor is lightweight; it already encapsulates required dependencies.
	eventProcessor := eventpkg.NewProcessor(cfg, store)
	if eventProcessor == nil {
		return fmt.Errorf("failed to initialize event processor")
	}

	slog.Info("Event processor initialized successfully.")

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
		slog.Info("Event processing loop stopped.")
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		startMetricsServer(*metricsPort)
	}()

	slog.Info("CSP Health Monitor (Main Container) components started successfully.")
	<-ctx.Done()
	slog.Info("Shutdown signal received by main monitor. Waiting for components to shut down gracefully...")
	wg.Wait()
	slog.Info("CSP Health Monitor (Main Container) shut down completed.")

	return nil
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
		slog.Info("GCP configuration is enabled.")

		gcpMonitor, err := gcpclient.NewClient(ctx, cfg.GCP, cfg.ClusterName, kubeconfigPath, store)
		if err != nil {
			metrics.CSPMonitorErrors.WithLabelValues(string(model.CSPGCP), "init_error").Inc()
			slog.Error("Failed to initialize GCP monitor. GCP will not be monitored.", "error", err)

			return nil
		}

		slog.Info("GCP monitor initialized", "project", cfg.GCP.TargetProjectID)

		return gcpMonitor
	}

	if cfg.AWS.Enabled {
		slog.Info("AWS configuration is enabled.")

		awsMonitor, err := awsclient.NewClient(ctx, cfg.AWS, cfg.ClusterName, kubeconfigPath, store)
		if err != nil {
			metrics.CSPMonitorErrors.WithLabelValues(string(model.CSPAWS), "init_error").Inc()
			slog.Error("Failed to initialize AWS monitor. AWS will not be monitored.", "error", err)

			return nil
		}

		slog.Info("AWS monitor initialized", "account", cfg.AWS.AccountID, "region", cfg.AWS.Region)

		return awsMonitor
	}

	slog.Info("No CSP is explicitly enabled in the configuration (GCP or AWS).")

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

	slog.Info("Metrics server (main monitor) starting to listen", "address", listenAddress+"/metrics")

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Metrics server (main monitor) failed", "error", err)
	}

	slog.Info("Metrics server (main monitor) stopped.")
}

// runEventProcessorLoop consumes normalized events from eventChan and hands
// them to the datastore-backed Processor until the context is cancelled.
func runEventProcessorLoop(
	ctx context.Context,
	eventChan <-chan model.MaintenanceEvent,
	processor *eventpkg.Processor,
) {
	slog.Info("Starting event processing worker loop (main monitor)...")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping event processing worker loop (main monitor).")
			return
		case receivedEvent, ok := <-eventChan:
			if !ok {
				slog.Info("Event channel closed, stopping event processing worker loop (main monitor).")
				return
			}

			metrics.MainEventsReceived.WithLabelValues(string(receivedEvent.CSP)).Inc()
			slog.Debug("Processor received event",
				"eventID", receivedEvent.EventID,
				"csp", receivedEvent.CSP,
				"node", receivedEvent.NodeName,
				"status", receivedEvent.Status)

			start := time.Now()
			err := processor.ProcessEvent(ctx, &receivedEvent)
			duration := time.Since(start).Seconds()
			metrics.MainEventProcessingDuration.WithLabelValues(string(receivedEvent.CSP)).Observe(duration)

			if err != nil {
				metrics.MainProcessingErrors.WithLabelValues(string(receivedEvent.CSP), "process_event").Inc()
				slog.Error("Error processing event",
					"eventID", receivedEvent.EventID,
					"node", receivedEvent.NodeName,
					"error", err,
				)
			} else {
				metrics.MainEventsProcessedSuccess.WithLabelValues(string(receivedEvent.CSP)).Inc()
				slog.Debug("Successfully processed event", "eventID", receivedEvent.EventID)
			}
		}
	}
}

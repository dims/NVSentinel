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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/metrics"
	trigger "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/triggerengine"
	sdkconfig "github.com/nvidia/nvsentinel/store-client-sdk/pkg/config"
	"github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers"
)

const (
	defaultConfigPathSidecar  = "/etc/config/config.toml"
	defaultUdsPathSidecar     = "/run/nvsentinel/nvsentinel.sock"
	defaultMetricsPortSidecar = "2113"
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
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

	// Parse flags
	flag.Parse()

	return cfg
}

func logStartupInfo(cfg *appConfig) {
	slog.Info("Starting Quarantine Trigger Engine Sidecar...")
	slog.Info("Using configuration file", "path", cfg.configPath)
	slog.Info("Platform Connector UDS Path", "path", cfg.udsPath)
	slog.Info("Exposing sidecar metrics on port", "port", cfg.metricsPort)
	slog.Debug("Log verbosity level is set based on the -v flag for sidecar.")
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

		slog.Info("Metrics server (sidecar) starting to listen", "address", listenAddress)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Metrics server (sidecar) failed", "error", err)
		}

		slog.Info("Metrics server (sidecar) stopped.")
	}()
}

func setupUDSConnection(udsPath string) (*grpc.ClientConn, pb.PlatformConnectorClient) {
	slog.Info("Sidecar attempting to connect to Platform Connector UDS", "path", fmt.Sprintf("unix:%s", udsPath))
	target := fmt.Sprintf("unix:%s", udsPath)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		metrics.TriggerUDSSendErrors.Inc()
		slog.Error("Sidecar failed to dial Platform Connector UDS", "target", target, "error", err)
		os.Exit(1)
	}

	slog.Info("Sidecar successfully connected to Platform Connector UDS.")

	return conn, pb.NewPlatformConnectorClient(conn)
}

func setupKubernetesClient() kubernetes.Interface {
	var restCfg *rest.Config

	var err error

	restCfg, err = rest.InClusterConfig()
	if err != nil {
		slog.Warn("Trigger Engine: failed to obtain in-cluster Kubernetes config", "error", err)
		return nil
	}

	k8sClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		slog.Error("Trigger Engine: failed to create Kubernetes clientset", "error", err)
		return nil
	}

	slog.Info("Trigger Engine: Kubernetes clientset initialized successfully for node readiness checks.")

	return k8sClient
}

func main() {
	logger.SetDefaultStructuredLogger("maintenance-notifier", version)
	slog.Info("Starting maintenance-notifier", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	appCfg := parseFlags()

	logStartupInfo(appCfg)

	cfg, err := config.LoadConfig(appCfg.configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startMetricsServer(appCfg.metricsPort)

	// Load datastore configuration using the store-client-sdk
	datastoreConfig, err := sdkconfig.LoadDatastoreConfig()
	if err != nil {
		return fmt.Errorf("failed to load datastore configuration: %w", err)
	}

	store, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize datastore for sidecar: %w", err)
	}

	slog.Info("Successfully connected to datastore", "provider", datastoreConfig.Provider, "component", "sidecar")

	conn, platformConnectorClient := setupUDSConnection(appCfg.udsPath)
	defer func() {
		slog.Info("Closing UDS connection for sidecar.")

		if errClose := conn.Close(); errClose != nil {
			slog.Error("Error closing sidecar UDS connection", "error", errClose)
		}
	}()

	k8sClient := setupKubernetesClient()

	engine := trigger.NewEngine(cfg, store, platformConnectorClient, k8sClient)

	slog.Info("Trigger engine starting...")
	engine.Start(ctx) // This is blocking

	slog.Info("Quarantine Trigger Engine Sidecar shut down.")

	return nil
}

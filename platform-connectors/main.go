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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/nvidia/nvsentinel/platform-connectors/pkg/connectors/kubernetes"
	"github.com/nvidia/nvsentinel/platform-connectors/pkg/connectors/store"
	_ "github.com/nvidia/nvsentinel/store-client-sdk/pkg/datastore/providers" // Register all datastore providers
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"k8s.io/apimachinery/pkg/util/json"

	"github.com/nvidia/nvsentinel/platform-connectors/pkg/ringbuffer"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"

	"github.com/nvidia/nvsentinel/platform-connectors/pkg/server"
	"google.golang.org/grpc"
)

const (
	True = "true"
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	logger.SetDefaultStructuredLogger("platform-connectors", version)
	slog.Info("Starting platform-connectors", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Platform connectors exited with error", "error", err)
		os.Exit(1)
	}
}

//nolint:cyclop
func run() error {
	socket := flag.String("socket", "", "unix socket path")
	configFilePath := flag.String("config", "/etc/config/config.json", "path to the config file")

	var metricsPort = flag.String("metrics-port", "2112", "port to expose Prometheus metrics on")

	flag.Parse()

	if *socket == "" {
		return fmt.Errorf("socket is not present")
	}

	sigs := make(chan os.Signal, 1)
	stopCh := make(chan struct{})

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	data, err := os.ReadFile(*configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read platform-connector-configmap: %w", err)
	}

	result := make(map[string]interface{})

	err = json.Unmarshal(data, &result)
	if err != nil {
		return fmt.Errorf("failed to unmarshal platform-connector-configmap: %w", err)
	}

	enableK8sPlatformConnector := result["enableK8sPlatformConnector"]
	enableMongoDBStorePlatformConnector := result["enableMongoDBStorePlatformConnector"]

	var k8sRingBuffer *ringbuffer.RingBuffer
	k8sRingBuffer = nil

	if enableK8sPlatformConnector == True {
		k8sRingBuffer = ringbuffer.NewRingBuffer("kubernetes", ctx)
		server.InitializeAndAttachRingBufferForConnectors(k8sRingBuffer)

		qpsTemp, ok := result["K8sConnectorQps"].(float64)
		if !ok {
			return fmt.Errorf("failed to convert K8sConnectorQps to float: %v", result["K8sConnectorQps"])
		}

		qps := float32(qpsTemp)

		burst, ok := result["K8sConnectorBurst"].(int64)
		if !ok {
			return fmt.Errorf("failed to convert K8sConnectorBurst to int: %v", result["K8sConnectorBurst"])
		}

		k8sConnector, err := kubernetes.InitializeK8sConnector(ctx, k8sRingBuffer, qps, int(burst), stopCh)
		if err != nil {
			return fmt.Errorf("failed to initialize K8s connector: %w", err)
		}

		go k8sConnector.FetchAndProcessHealthMetric(ctx)
	}

	if enableMongoDBStorePlatformConnector == True {
		ringBuffer := ringbuffer.NewRingBuffer("mongodbStore", ctx)
		server.InitializeAndAttachRingBufferForConnectors(ringBuffer)
		// Use store-client-sdk abstraction instead of direct MongoDB initialization
		storeConnector := store.InitializeStoreConnector(ctx, ringBuffer)

		go storeConnector.FetchAndProcessHealthMetric(ctx)
	}

	err = os.Remove(*socket)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	lis, err := net.Listen("unix", *socket)
	if err != nil {
		return fmt.Errorf("error creating platform-connector unixsocket: %w", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterPlatformConnectorServer(grpcServer, &server.PlatformConnectorServer{})

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			// Check if error is due to closed connection during shutdown
			if strings.Contains(err.Error(), "use of closed network connection") {
				slog.Info("gRPC server stopped due to closed connection during shutdown", "error", err)
			} else {
				slog.Error("Not able to accept incoming connections", "error", err)
			}
		}
	}()

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
		}
	}()

	slog.Info("Metrics server goroutine started")

	slog.Info("Waiting for signal")
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	slog.Info("Received signal", "signal", sig)

	close(stopCh)

	if lis != nil {
		k8sRingBuffer.ShutDownHealthMetricQueue()
		lis.Close()
		os.Remove(*socket)
	}

	cancel()

	return nil
}

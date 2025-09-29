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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "simple-health-client/protos"
)

func main() {
	socketPath := "/var/run/nvsentinel.sock"
	port := "8080"

	log.Printf("Starting health event API server on port %s", port)
	log.Printf("Using socket path: %s", socketPath)

	http.HandleFunc("/health-event", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read the request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusBadRequest)
			return
		}

		// Use protojson to properly unmarshal protobuf messages with enum fields
		var healthEvent pb.HealthEvent
		if err := protojson.Unmarshal(bodyBytes, &healthEvent); err != nil {
			http.Error(w, fmt.Sprintf("Error parsing JSON: %v", err), http.StatusBadRequest)
			return
		}

		healthEvent.GeneratedTimestamp = timestamppb.Now()

		// Debug logging to verify recommendedAction is properly parsed
		log.Printf("Received health event: Node=%s, RecommendedAction=%v (%d)",
			healthEvent.NodeName, healthEvent.RecommendedAction, healthEvent.RecommendedAction)

		conn, err := grpc.NewClient(
			fmt.Sprintf("unix://%s", socketPath),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to connect to socket: %v", err), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		client := pb.NewPlatformConnectorClient(conn)

		healthEvents := &pb.HealthEvents{
			Version: 1,
			Events:  []*pb.HealthEvent{&healthEvent},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		log.Printf("Sending health event for node: %s", healthEvent.NodeName)
		_, err = client.HealthEventOccuredV1(ctx, healthEvents)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send health event: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("SUCCESS: Health event sent for node %s", healthEvent.NodeName)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Health event sent"})
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

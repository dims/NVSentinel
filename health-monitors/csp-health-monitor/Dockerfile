# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# syntax=docker/dockerfile:1.6

ARG TAG=v3.1.13
ARG IMG=nvcr.io/nvidia/distroless/go

FROM public.ecr.aws/docker/library/golang:1.24-bullseye AS builder

# TODO: remove when pushing to GitHub
ENV GOPRIVATE=*.nvidia.com
ENV CGO_ENABLED=0

WORKDIR /go/src

# Copy go modules and dependencies
COPY health-monitors/csp-health-monitor/go.mod health-monitors/csp-health-monitor/go.sum ./health-monitors/csp-health-monitor/
COPY store-client-sdk/go.mod store-client-sdk/go.sum ./store-client-sdk/
COPY platform-connectors/go.mod platform-connectors/go.sum ./platform-connectors/

# Copy dependency source code
COPY store-client-sdk/ ./store-client-sdk/
COPY platform-connectors/ ./platform-connectors/

# Copy main module source
COPY health-monitors/csp-health-monitor/ ./health-monitors/csp-health-monitor/

WORKDIR /go/src/health-monitors/csp-health-monitor

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

RUN --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -o csp-health-monitor ./cmd/csp-health-monitor/main.go

RUN --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -o maintenance-notifier ./cmd/maintenance-notifier/main.go

FROM ${IMG}:${TAG}

WORKDIR /app

COPY --from=builder /go/src/health-monitors/csp-health-monitor/csp-health-monitor ./
COPY --from=builder /go/src/health-monitors/csp-health-monitor/maintenance-notifier ./

ENTRYPOINT ["/app/csp-health-monitor"]

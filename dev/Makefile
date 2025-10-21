# Development environment Makefile
# Contains targets for development environment setup and management

# Configuration variables
CTLPTL_CONFIG_FILE ?= ../ctlptl-config.yaml
CLUSTER_NAME ?= kind-nvsentinel
REGISTRY_NAME ?= ctlptl-registry
REGISTRY_PORT ?= 5001
TILTFILE ?= ../Tiltfile

# Tilt development targets
.PHONY: tilt-up
tilt-up:
	@echo "Starting Tilt development environment..."
	@if ! command -v tilt >/dev/null 2>&1; then \
		echo "Error: tilt is not installed. Please install from https://tilt.dev/"; \
		exit 1; \
	fi
	cd .. && tilt up -f $(TILTFILE)

.PHONY: tilt-down
tilt-down:
	@echo "Stopping Tilt development environment..."
	@if command -v tilt >/dev/null 2>&1; then \
		cd .. && tilt down -f $(TILTFILE); \
	else \
		echo "Warning: tilt is not installed"; \
	fi

.PHONY: tilt-ci
tilt-ci:
	@echo "Running Tilt in CI mode (no UI)..."
	@if ! command -v tilt >/dev/null 2>&1; then \
		echo "Error: tilt is not installed. Please install from https://tilt.dev/"; \
		exit 1; \
	fi
	cd .. && tilt ci -f $(TILTFILE)

# ctlptl cluster configuration
.PHONY: cluster-create
cluster-create:
	@echo "Creating local development cluster with ctlptl..."
	@if ! command -v ctlptl >/dev/null 2>&1; then \
		echo "Error: ctlptl is not installed. Please install from https://github.com/tilt-dev/ctlptl"; \
		echo "Installation options:"; \
		echo "  - Homebrew: brew install tilt-dev/tap/ctlptl"; \
		echo "  - Go: go install github.com/tilt-dev/ctlptl/cmd/ctlptl@latest"; \
		exit 1; \
	fi
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "Error: docker is not installed. Please install Docker"; \
		exit 1; \
	fi
	@if ! command -v kind >/dev/null 2>&1; then \
		echo "Error: kind is not installed. Please install from https://kind.sigs.k8s.io/"; \
		exit 1; \
	fi
	@echo "Creating cluster and registry with ctlptl..."
	@if [ ! -f "$(CTLPTL_CONFIG_FILE)" ]; then \
		echo "Error: ctlptl config file $(CTLPTL_CONFIG_FILE) not found"; \
		exit 1; \
	fi
	@# Generate dynamic ctlptl config with cluster name suffix and unique registry
	@CLUSTER_NAME_WITH_SUFFIX="$(CLUSTER_NAME)$(CLUSTER_NAME_SUFFIX)"; \
	REGISTRY_NAME_WITH_SUFFIX="$(REGISTRY_NAME)$(CLUSTER_NAME_SUFFIX)"; \
	if [ -n "$(CLUSTER_NAME_SUFFIX)" ]; then \
		REGISTRY_PORT_SUFFIX=$$(echo "$(CLUSTER_NAME_SUFFIX)" | md5sum | head -c4); \
		REGISTRY_PORT_WITH_SUFFIX=$$((5001 + 0x$$REGISTRY_PORT_SUFFIX % 1000)); \
	else \
		REGISTRY_PORT_WITH_SUFFIX=$(REGISTRY_PORT); \
	fi; \
	sed -e "s/name: kind-nvsentinel/name: $$CLUSTER_NAME_WITH_SUFFIX/" \
	    -e "s/name: ctlptl-registry/name: $$REGISTRY_NAME_WITH_SUFFIX/" \
	    -e "s/registry: ctlptl-registry/registry: $$REGISTRY_NAME_WITH_SUFFIX/" \
	    -e "s/port: 5001/port: $$REGISTRY_PORT_WITH_SUFFIX/" \
	    $(CTLPTL_CONFIG_FILE) > /tmp/ctlptl-config-$$$$.yaml; \
	echo "Creating cluster $$CLUSTER_NAME_WITH_SUFFIX with registry $$REGISTRY_NAME_WITH_SUFFIX on port $$REGISTRY_PORT_WITH_SUFFIX"; \
	ctlptl apply -f /tmp/ctlptl-config-$$$$.yaml; \
	rm -f /tmp/ctlptl-config-$$$$.yaml
	@echo "Cluster created successfully!"
	@echo "Registry available at localhost:$(REGISTRY_PORT)"

.PHONY: cluster-delete
cluster-delete:
	@echo "Deleting local development cluster..."
	@if [ ! -f "$(CTLPTL_CONFIG_FILE)" ]; then \
		echo "Error: ctlptl config file $(CTLPTL_CONFIG_FILE) not found"; \
		exit 1; \
	fi
	@# Generate dynamic ctlptl config with cluster name suffix and unique registry for deletion
	@CLUSTER_NAME_WITH_SUFFIX="$(CLUSTER_NAME)$(CLUSTER_NAME_SUFFIX)"; \
	REGISTRY_NAME_WITH_SUFFIX="$(REGISTRY_NAME)$(CLUSTER_NAME_SUFFIX)"; \
	if [ -n "$(CLUSTER_NAME_SUFFIX)" ]; then \
		REGISTRY_PORT_SUFFIX=$$(echo "$(CLUSTER_NAME_SUFFIX)" | md5sum | head -c4); \
		REGISTRY_PORT_WITH_SUFFIX=$$((5001 + 0x$$REGISTRY_PORT_SUFFIX % 1000)); \
	else \
		REGISTRY_PORT_WITH_SUFFIX=$(REGISTRY_PORT); \
	fi; \
	sed -e "s/name: kind-nvsentinel/name: $$CLUSTER_NAME_WITH_SUFFIX/" \
	    -e "s/name: ctlptl-registry/name: $$REGISTRY_NAME_WITH_SUFFIX/" \
	    -e "s/registry: ctlptl-registry/registry: $$REGISTRY_NAME_WITH_SUFFIX/" \
	    -e "s/port: 5001/port: $$REGISTRY_PORT_WITH_SUFFIX/" \
	    $(CTLPTL_CONFIG_FILE) > /tmp/ctlptl-config-$$$$.yaml; \
	echo "Deleting cluster $$CLUSTER_NAME_WITH_SUFFIX with registry $$REGISTRY_NAME_WITH_SUFFIX"; \
	ctlptl delete -f /tmp/ctlptl-config-$$$$.yaml || echo "Resources with cluster name $$CLUSTER_NAME_WITH_SUFFIX not found"; \
	rm -f /tmp/ctlptl-config-$$$$.yaml

.PHONY: cluster-status
cluster-status:
	@echo "Checking cluster status..."
	echo "ctlptl clusters:"; \
	ctlptl get clusters || echo "No ctlptl clusters found"; \
	echo "ctlptl registries:"; \
	ctlptl get registries || echo "No ctlptl registries found"; \

# Combined development environment targets
.PHONY: env-up
env-up: cluster-create tilt-up

.PHONY: env-down
env-down: tilt-down cluster-delete

# Quick development workflow targets
.PHONY: restart
restart: tilt-down tilt-up

.PHONY: reset
reset: env-down env-up

# Help target
.PHONY: help
help:
	@echo "Development Environment Makefile"
	@echo ""
	@echo "Main targets:"
	@echo "  env-up         - Create cluster and start Tilt (full setup)"
	@echo "  env-down       - Stop Tilt and delete cluster (full cleanup)"
	@echo ""
	@echo "Tilt targets:"
	@echo "  tilt-up        - Start Tilt development environment"
	@echo "  tilt-down      - Stop Tilt development environment"
	@echo "  tilt-ci        - Run Tilt in CI mode (no UI)"
	@echo "  restart        - Restart Tilt (tilt-down + tilt-up)"
	@echo ""
	@echo "Cluster targets (using ctlptl):"
	@echo "  cluster-create - Create local ctlptl-managed Kind cluster with registry"
	@echo "  cluster-delete - Delete local ctlptl-managed cluster and registry"
	@echo "  cluster-status - Show cluster and registry status"
	@echo ""
	@echo "Quick workflow targets:"
	@echo "  reset          - Full reset (env-down + env-up)"
	@echo ""
	@echo "Configuration variables:"
	@echo "  CTLPTL_CONFIG_FILE     - ctlptl config file (default: ../ctlptl-config.yaml)"
	@echo "  CLUSTER_NAME           - Cluster name (default: nvsentinel)"
	@echo "  REGISTRY_NAME          - Registry name (default: ctlptl-registry)"
	@echo "  REGISTRY_PORT          - Registry port (default: 5001)"
	@echo "  TILTFILE               - Tiltfile to use (default: ../Tiltfile)"
	@echo ""
	@echo "Notes:"
	@echo "  - ctlptl cluster requires Docker, ctlptl, and Kind to be installed"
	@echo "  - Registry is available at localhost:5001 (or custom port)"
	@echo "  - ctlptl provides declarative cluster management with YAML configuration"
	@echo "  - Install ctlptl: brew install tilt-dev/tap/ctlptl or go install github.com/tilt-dev/ctlptl/cmd/ctlptl@latest"

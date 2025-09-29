# Health Event Client

A CLI tool for sending health events to NVSentinel and monitoring their remediation status.

## Overview

The Health Event Client is a command-line utility that:
- Sends health events to the Platform Connector via gRPC
- Monitors the datastore for event processing status
- Tracks node quarantine and remediation workflows
- Automatically uncordons nodes when remediation completes

## Configuration

### Datastore Configuration

The client now uses the `store-client-sdk` for database abstraction. Configure the datastore using environment variables:

#### Required Environment Variables

```bash
# Datastore provider (mongodb, postgresql, etc.)
DATASTORE_PROVIDER=mongodb

# Connection details
DATASTORE_HOST=mongodb://mongodb-0.mongodb-headless.nvsentinel.svc.cluster.local:27017
DATASTORE_DATABASE=nvsentinel

# TLS Configuration (if using MongoDB with TLS)
DATASTORE_TLS_ENABLED=true
DATASTORE_TLS_CERT_PATH=/etc/ssl/mongo-client/tls.crt
DATASTORE_TLS_KEY_PATH=/etc/ssl/mongo-client/tls.key
DATASTORE_TLS_CA_PATH=/etc/ssl/mongo-client/ca.crt
```

#### Legacy MongoDB Environment Variables (Still Supported)

For backward compatibility, the following environment variables are also supported:

```bash
MONGODB_URI=mongodb://mongodb-0.mongodb-headless.nvsentinel.svc.cluster.local:27017
MONGODB_DATABASE_NAME=nvsentinel
MONGODB_COLLECTION_NAME=HealthEvents
MONGODB_CLIENT_CERT_MOUNT_PATH=/etc/ssl/mongo-client
```

### Platform Connector Configuration

```bash
# Socket path for Platform Connector
SOCKET_PATH=/var/run/nvsentinel/nvsentinel.sock  # Default
```

## Usage

### Send a Health Event

```bash
health-event-client \
  --node-name=worker-node-1 \
  --error-code=XID_79 \
  --reason="GPU XID error detected" \
  --recommended-action=2 \
  --creator-id=health-monitor \
  --socket=/var/run/nvsentinel/nvsentinel.sock
```

### Monitor an Existing Event

```bash
health-event-client \
  --event-id=<health-event-id> \
  --socket=/var/run/nvsentinel/nvsentinel.sock
```

### Command-Line Flags

| Flag | Description | Required | Default |
|------|-------------|----------|---------|
| `--node-name` | Name of the node | Yes | - |
| `--error-code` | Error code for the health event | Yes | - |
| `--reason` | Reason/description for the operation | Yes | - |
| `--recommended-action` | Recommended action (0-3) | No | 1 |
| `--creator-id` | Creator identifier | No | "default" |
| `--is-healthy` | Mark as healthy event | No | false |
| `--skip-quarantine` | Skip quarantine step | No | false |
| `--skip-drain` | Skip drain step | No | false |
| `--force` | Force the operation | No | false |
| `--socket` | Platform connector socket path | No | `/var/run/nvsentinel/nvsentinel.sock` |
| `--event-id` | Event ID for monitoring existing events | No | - |

### Recommended Action Values

| Value | Description |
|-------|-------------|
| 0 | No action |
| 1 | Cordon |
| 2 | Cordon + Drain |
| 3 | Cordon + Drain + Reboot |

## Running in Kubernetes

### Prerequisites

The client requires:
1. **RBAC permissions** - Installed via the `health-event-client` Helm chart
2. **Access to datastore** - MongoDB connection and credentials
3. **Platform Connector socket** - Mounted from the host

### Example Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: health-event-client-job
  namespace: nvsentinel
spec:
  serviceAccountName: health-event-client
  restartPolicy: Never
  containers:
  - name: health-event-client
    image: nvcr.io/nv-ngc-devops/health-event-client:main
    command:
    - /usr/local/bin/health-event-client
    args:
    - "--node-name=worker-node-1"
    - "--error-code=XID_79"
    - "--reason=GPU XID error detected"
    - "--recommended-action=2"
    env:
    # Datastore configuration (SDK format)
    - name: DATASTORE_PROVIDER
      value: "mongodb"
    - name: DATASTORE_HOST
      value: "mongodb://mongodb-0.mongodb-headless.nvsentinel.svc.cluster.local:27017"
    - name: DATASTORE_DATABASE
      value: "nvsentinel"
    - name: DATASTORE_TLS_ENABLED
      value: "true"
    - name: DATASTORE_TLS_CERT_PATH
      value: "/etc/ssl/mongo-client/tls.crt"
    - name: DATASTORE_TLS_KEY_PATH
      value: "/etc/ssl/mongo-client/tls.key"
    - name: DATASTORE_TLS_CA_PATH
      value: "/etc/ssl/mongo-client/ca.crt"
    # Or use legacy MongoDB format (also supported)
    envFrom:
    - configMapRef:
        name: mongodb-config
        optional: true
    volumeMounts:
    - name: mongo-app-client-cert
      mountPath: /etc/ssl/mongo-client
      readOnly: true
    - name: var-run-nvsentinel
      mountPath: /var/run/nvsentinel
  volumes:
  - name: mongo-app-client-cert
    secret:
      secretName: mongo-app-client-cert-secret
      optional: true
  - name: var-run-nvsentinel
    hostPath:
      path: /var/run/nvsentinel
```

### Using with ConfigMap

The client can also read configuration from the centralized `mongodb-config` ConfigMap:

```yaml
envFrom:
- configMapRef:
    name: mongodb-config
    optional: true
```

This ConfigMap provides both legacy (`MONGODB_*`) and new (`DATASTORE_*`) environment variables.

## Monitoring Behavior

When sending a health event, the client will:

1. **Send Event** - Sends the event to Platform Connector via gRPC
2. **Wait for Insert** - Polls the datastore (1-second interval, 30-second timeout) until the event appears
3. **Monitor Status** - Polls for status updates (2-second interval, 5-minute timeout):
   - Waits for `nodeQuarantined` status
   - Waits for `faultRemediated` status
4. **Monitor CR** - Watches the Kubernetes `RebootNode` CR for completion
5. **Uncordon Node** - Automatically sends a healthy event to uncordon the node

### Polling vs Change Streams

The client uses **polling** instead of MongoDB change streams for simplicity and reliability:
- Suitable for CLI tool (not long-running service)
- 2-second poll interval is acceptable for remediation workflows
- Easier to test and debug
- Works with any datastore provider

## Migration from Direct MongoDB

If you're migrating from a version that used direct MongoDB connections:

### Old Configuration
```bash
MONGODB_URI=mongodb://...
MONGODB_DATABASE_NAME=nvsentinel
MONGODB_COLLECTION_NAME=HealthEvents
MONGODB_SECRET_NAME=mongodb-client-creds
MONGODB_SECRET_NAMESPACE=nvsentinel
```

### New Configuration (Recommended)
```bash
DATASTORE_PROVIDER=mongodb
DATASTORE_HOST=mongodb://...
DATASTORE_DATABASE=nvsentinel
DATASTORE_TLS_ENABLED=true
DATASTORE_TLS_CERT_PATH=/etc/ssl/mongo-client/tls.crt
DATASTORE_TLS_KEY_PATH=/etc/ssl/mongo-client/tls.key
DATASTORE_TLS_CA_PATH=/etc/ssl/mongo-client/ca.crt
```

**Note:** The old format is still supported for backward compatibility.

## Troubleshooting

### Connection Errors

**Error:** `failed to load datastore config`
- **Cause:** Missing or invalid environment variables
- **Solution:** Ensure `DATASTORE_PROVIDER` and `DATASTORE_HOST` are set

**Error:** `failed to ping datastore`
- **Cause:** Cannot connect to database
- **Solution:** Check MongoDB is running and accessible, verify TLS certificates are mounted correctly

### Event Not Found

**Error:** `timeout waiting for health event insert`
- **Cause:** Event not inserted into database within 30 seconds
- **Solution:** Check Platform Connector logs, verify MongoDB is accepting writes

### Monitoring Timeout

**Error:** `timeout while monitoring health event`
- **Cause:** Event status not updated within 5 minutes
- **Solution:** Check fault-quarantine, fault-remediation, and node-drainer module logs

## Development

### Building

```bash
cd health-event-client
go build -o health-event-client
```

### Testing

```bash
go test ./...
```

### Dependencies

The client uses:
- `store-client-sdk` - Database abstraction layer
- `platform-connectors` - gRPC client for sending events
- `k8s.io/client-go` - Kubernetes client for CR monitoring

## See Also

- [Store Client SDK Documentation](../store-client-sdk/README.md)
- [Platform Connectors](../platform-connectors/README.md)
- [NVSentinel Architecture](../README.md)


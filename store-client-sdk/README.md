# Store Client SDK

A Go SDK that provides a unified interface for connecting to MongoDB datastore and watching for change events. This SDK abstracts datastore-specific implementations behind a common interface, making it easy to add new providers.

## Core Interfaces

The SDK is built around several key interfaces defined in `pkg/datastore/interface.go`:

### DataStore
The main interface that all providers must implement:
- **MaintenanceEventStore()** - For CSP maintenance events (used by health monitors)
- **HealthEventStore()** - For platform health events (used by platform connectors)
- **InsertMany()** - Generic bulk insert operations
- **Ping()** / **Close()** - Connection management

### MaintenanceEventStore
Handles CSP maintenance events with operations like:
- Upserting events, finding events to trigger quarantine/healthy status
- Updating event status, getting last processed timestamps
- Finding active events by node and type

### HealthEventStore
Handles platform health events with operations for:
- Inserting health events with status
- Updating health event status
- Finding events by node or custom filters

### Change Stream Support
The SDK provides change stream watching capabilities:
- **ChangeStreamWatcher** - Basic change stream functionality
- **FilteredChangeStreamFactory** - Creates filtered change stream watchers
- **EnhancedDataStore** - Extended interface supporting filtered change streams

## Supported Providers

Currently supported datastore providers:
- **MongoDB** (`ProviderMongoDB`)

## Adding a New Provider

To add a new datastore provider:

1. **Create provider package**: Create a new directory under `pkg/datastore/providers/your-provider/`

2. **Implement required interfaces**: Your provider must implement:
   - `DataStore` interface (and optionally `EnhancedDataStore` for change streams)
   - `MaintenanceEventStore` interface
   - `HealthEventStore` interface

3. **Create registration function**: Add a `register.go` file with an `init()` function:
   ```go
   func init() {
       datastore.RegisterProvider("your-provider", NewYourProviderDataStore)
   }
   ```

4. **Provider factory function**: Implement a factory function matching the signature:
   ```go
   func NewYourProviderDataStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error)
   ```

5. **Update constants**: Add your provider to the `DataStoreProvider` constants in `interface.go`

6. **Import for registration**: Ensure your provider is imported somewhere so the `init()` function runs. See examples in existing provider imports.

The factory pattern with automatic registration allows providers to be plugged in simply by importing their package. The `DataStoreConfig` struct provides a generic configuration format that can be adapted to any provider's connection requirements.

// Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
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

package datastore

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/klog/v2"
)

// ProviderFactory is a function that creates a datastore instance
type ProviderFactory func(ctx context.Context, config DataStoreConfig) (DataStore, error)

// Global provider registry
var (
	providerRegistry = make(map[DataStoreProvider]ProviderFactory)
	registryMutex    sync.RWMutex
)

// RegisterProvider registers a datastore provider with the global registry
func RegisterProvider(provider DataStoreProvider, factory ProviderFactory) {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	providerRegistry[provider] = factory

	klog.V(2).Infof("Registered datastore provider: %s", provider)
}

// GetProvider gets a provider factory from the global registry
func GetProvider(provider DataStoreProvider) ProviderFactory {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	return providerRegistry[provider]
}

// SupportedProviders returns a list of all registered provider types
func SupportedProviders() []DataStoreProvider {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	providers := make([]DataStoreProvider, 0, len(providerRegistry))
	for provider := range providerRegistry {
		providers = append(providers, provider)
	}

	return providers
}

// Factory implements DataStoreFactory interface
type Factory struct {
	providers map[DataStoreProvider]ProviderFactory
}

// NewFactory creates a new datastore factory
func NewFactory() DataStoreFactory {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	// Copy the global registry to this factory instance
	providers := make(map[DataStoreProvider]ProviderFactory)
	for provider, factory := range providerRegistry {
		providers[provider] = factory
	}

	klog.V(2).Infof("Created datastore factory with %d providers: %v", len(providers), getProviderNames(providers))

	return &Factory{providers: providers}
}

// Helper function to get provider names for logging
func getProviderNames(providers map[DataStoreProvider]ProviderFactory) []DataStoreProvider {
	names := make([]DataStoreProvider, 0, len(providers))
	for provider := range providers {
		names = append(names, provider)
	}

	return names
}

// NewDataStore creates a datastore instance using the factory
func (f *Factory) NewDataStore(ctx context.Context, config DataStoreConfig) (DataStore, error) {
	klog.V(1).Infof("Creating datastore for provider: %s", config.Provider)

	factory, exists := f.providers[config.Provider]
	if !exists {
		klog.Errorf("Unsupported datastore provider: %s (available: %v)", config.Provider, f.SupportedProviders())
		return nil, fmt.Errorf("unsupported datastore provider: %s", config.Provider)
	}

	datastore, err := factory(ctx, config)
	if err != nil {
		klog.Errorf("Failed to create datastore instance for provider %s: %v", config.Provider, err)
		return nil, err
	}

	klog.V(1).Infof("Successfully created datastore instance for provider: %s", config.Provider)

	return datastore, nil
}

// SupportedProviders returns supported providers for this factory instance
func (f *Factory) SupportedProviders() []DataStoreProvider {
	providers := make([]DataStoreProvider, 0, len(f.providers))
	for provider := range f.providers {
		providers = append(providers, provider)
	}

	return providers
}

// Default global factory instance - created lazily to ensure proper sequencing
var defaultFactory DataStoreFactory
var factoryMutex sync.Mutex

// getDefaultFactory returns the default factory, creating it lazily if needed
func getDefaultFactory() DataStoreFactory {
	factoryMutex.Lock()

	defer factoryMutex.Unlock()

	if defaultFactory == nil {
		klog.V(2).Infof("Creating default datastore factory on first use")

		defaultFactory = NewFactory()
	}

	return defaultFactory
}

// NewDataStore creates a datastore using the default global factory
func NewDataStore(ctx context.Context, config DataStoreConfig) (DataStore, error) {
	factory := getDefaultFactory()
	return factory.NewDataStore(ctx, config)
}

// ValidateConfig validates datastore configuration
func ValidateConfig(config DataStoreConfig) error {
	if config.Provider == "" {
		return fmt.Errorf("datastore provider is required")
	}

	if config.Connection.Host == "" {
		return fmt.Errorf("connection host is required")
	}

	if config.Connection.Database == "" {
		return fmt.Errorf("database name is required")
	}

	// Additional validation can be added here
	return nil
}

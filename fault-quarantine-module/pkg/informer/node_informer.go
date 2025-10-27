// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informer

import (
	"fmt"
	"sync"
	"time"

	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine-module/pkg/nodeinfo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// GpuNodeLabel is the label used to identify nodes with GPUs relevant to NVSentinel.
	GpuNodeLabel = "nvidia.com/gpu.present"
)

// NodeInfoProvider defines the interface for getting node counts.
type NodeInfoProvider interface {
	// GetGpuNodeCounts returns the total number of nodes with the GpuNodeLabel
	// and the number of those nodes that are currently unschedulable (cordoned).
	GetGpuNodeCounts() (totalGpuNodes int, cordonedNodesMap map[string]bool, err error)
	// HasSynced returns true if the underlying informer cache has synced.
	HasSynced() bool
}

// NodeInformer watches specific nodes and provides counts.
type NodeInformer struct {
	clientset kubernetes.Interface
	informer  cache.SharedIndexInformer
	lister    corelisters.NodeLister

	// Mutex protects access to the counts below
	mutex         sync.RWMutex
	totalGpuNodes int

	informerSynced cache.InformerSynced

	// workSignal is used to notify the reconciler about relevant node changes
	workSignal chan struct{}

	// nodeInfo is used to store the node quarantine status
	nodeInfo *nodeinfo.NodeInfo

	// onQuarantinedNodeDeleted is called when a quarantined node with annotations is deleted
	onQuarantinedNodeDeleted func(nodeName string)

	// onNodeAnnotationsChanged is called when a node's annotations change
	onNodeAnnotationsChanged func(nodeName string, annotations map[string]string)

	// onManualUncordon is called when a node is manually uncordoned while having FQ annotations
	onManualUncordon func(nodeName string) error
}

// Lister returns the informer's node lister.
func (ni *NodeInformer) Lister() corelisters.NodeLister {
	return ni.lister
}

// NewNodeInformer creates a new NodeInformer focused on nodes with the GpuNodeLabel.
func NewNodeInformer(clientset kubernetes.Interface,
	resyncPeriod time.Duration, workSignal chan struct{}, nodeInfo *nodeinfo.NodeInfo) (*NodeInformer, error) {
	// Filter nodes based on the presence of the GPU label
	gpuNodeSelector := labels.Set{GpuNodeLabel: "true"}.AsSelector()

	tweakListOptions := func(options *metav1.ListOptions) {
		options.LabelSelector = gpuNodeSelector.String()
	}

	// Create an informer factory filtered for the specific label
	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, resyncPeriod,
		informers.WithTweakListOptions(tweakListOptions))
	nodeInformer := informerFactory.Core().V1().Nodes()

	ni := &NodeInformer{
		clientset:      clientset,
		informer:       nodeInformer.Informer(),
		lister:         nodeInformer.Lister(),
		informerSynced: nodeInformer.Informer().HasSynced,
		workSignal:     workSignal,
		nodeInfo:       nodeInfo,
	}

	// Register event handlers
	_, err := ni.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ni.handleAddNode,
		UpdateFunc: ni.handleUpdateNode,
		DeleteFunc: ni.handleDeleteNode,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler: %w", err)
	}

	klog.Infof("NodeInformer created, watching nodes with label %s=true", GpuNodeLabel)

	return ni, nil
}

// Run starts the informer and waits for cache sync.
func (ni *NodeInformer) Run(stopCh <-chan struct{}) error {
	klog.Info("Starting NodeInformer")

	// Start the informer goroutine
	go ni.informer.Run(stopCh)

	// Wait for the initial cache synchronization
	klog.Info("Waiting for NodeInformer cache to sync...")

	if ok := cache.WaitForCacheSync(stopCh, ni.informerSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("NodeInformer cache synced")

	// Retry recalculateCounts if it returns 0 nodes (cache might still be populating)
	// This handles the race between cache sync and cache population
	maxRetries := 10
	retryDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		changed, err := ni.recalculateCounts()
		if err != nil {
			klog.Errorf("Initial count calculation failed on attempt %d: %v", attempt+1, err)

			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				retryDelay *= 2 // Exponential backoff

				if retryDelay > 2*time.Second {
					retryDelay = 2 * time.Second
				}

				continue
			}

			break
		}

		// Check if we got nodes
		ni.mutex.RLock()
		totalNodes := ni.totalGpuNodes
		ni.mutex.RUnlock()

		if totalNodes > 0 {
			klog.Infof("NodeInformer cache populated: %d GPU nodes found", totalNodes)
			break
		}

		if attempt < maxRetries-1 {
			klog.V(2).Infof("NodeInformer cache still empty after sync, retry %d/%d in %v",
				attempt+1, maxRetries, retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff

			if retryDelay > 2*time.Second {
				retryDelay = 2 * time.Second
			}
		} else {
			klog.Fatalf("NodeInformer cache still reports 0 nodes after %d retries and ~%v total wait time. "+
				"This indicates a critical initialization failure - either cluster has no GPU nodes "+
				"or NodeInformer cannot populate its cache. Pod will restart.",
				maxRetries, time.Duration(maxRetries)*retryDelay)
		}

		_ = changed // Ignore changed status during initial population
	}

	return nil
}

// HasSynced checks if the informer's cache has been synchronized.
func (ni *NodeInformer) HasSynced() bool {
	return ni.informerSynced()
}

// GetGpuNodeCounts returns the current counts of total and unschedulable GPU nodes.
func (ni *NodeInformer) GetGpuNodeCounts() (totalGpuNodes int, cordonedNodesMap map[string]bool, err error) {
	if !ni.HasSynced() {
		return 0, nil, fmt.Errorf("node informer cache not synced yet")
	}

	ni.mutex.RLock()
	defer ni.mutex.RUnlock()

	return ni.totalGpuNodes, ni.nodeInfo.GetQuarantinedNodesCopy(), nil
}

// hasQuarantineAnnotationsChanged checks if any of the quarantine-related annotations have changed
func hasQuarantineAnnotationsChanged(oldAnnotations, newAnnotations map[string]string) bool {
	// List of annotation keys we care about
	quarantineKeys := []string{
		common.QuarantineHealthEventAnnotationKey,
		common.QuarantineHealthEventAppliedTaintsAnnotationKey,
		common.QuarantineHealthEventIsCordonedAnnotationKey,
		common.QuarantinedNodeUncordonedManuallyAnnotationKey,
	}

	// Check if any of the quarantine annotation values have changed
	for _, key := range quarantineKeys {
		oldValue := oldAnnotations[key]
		newValue := newAnnotations[key]

		if oldValue != newValue {
			return true
		}
	}

	return false
}

// getQuarantineAnnotations extracts only the quarantine-related annotations from a node's annotations
func getQuarantineAnnotations(annotations map[string]string) map[string]string {
	quarantineAnnotations := make(map[string]string)

	// List of annotation keys we care about
	quarantineKeys := []string{
		common.QuarantineHealthEventAnnotationKey,
		common.QuarantineHealthEventAppliedTaintsAnnotationKey,
		common.QuarantineHealthEventIsCordonedAnnotationKey,
		common.QuarantinedNodeUncordonedManuallyAnnotationKey,
	}

	// Extract only the quarantine annotations
	for _, key := range quarantineKeys {
		if value, exists := annotations[key]; exists {
			quarantineAnnotations[key] = value
		}
	}

	return quarantineAnnotations
}

// handleAddNode recalculates counts when a node is added.
func (ni *NodeInformer) handleAddNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.Errorf("Add event: expected Node object, got %T", obj)
		return
	}

	klog.V(4).Infof("Node added: %s", node.Name)

	ni.mutex.Lock()

	ni.totalGpuNodes++

	annotationExist := false

	if !ni.nodeInfo.GetNodeQuarantineStatusCache(node.Name) {
		if _, exists := node.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey]; exists {
			annotationExist = true
		}
	}

	// Mark as quarantined if the node is unschedulable or has the quarantine annotation
	if node.Spec.Unschedulable || annotationExist {
		ni.nodeInfo.MarkNodeQuarantineStatusCache(node.Name, true, annotationExist)
	}

	ni.mutex.Unlock()

	// Notify about the node's quarantine annotations (including empty ones)
	// This ensures all nodes get cached, preventing API calls for clean nodes
	if ni.onNodeAnnotationsChanged != nil {
		quarantineAnnotations := getQuarantineAnnotations(node.Annotations)
		ni.onNodeAnnotationsChanged(node.Name, quarantineAnnotations)
	}

	ni.signalWork()
}

// detectAndHandleManualUncordon checks if a node was manually uncordoned and handles it
func (ni *NodeInformer) detectAndHandleManualUncordon(oldNode, newNode *v1.Node) bool {
	// Check if node transitioned from unschedulable to schedulable
	if !(oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable) {
		return false
	}

	// Check if node has FQ quarantine annotations
	_, hasCordonAnnotation := newNode.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey]
	if !hasCordonAnnotation {
		return false
	}

	klog.Infof("Detected manual uncordon of FQ-quarantined node: %s", newNode.Name)

	// Call the manual uncordon handler if registered
	if ni.onManualUncordon != nil {
		if err := ni.onManualUncordon(newNode.Name); err != nil {
			klog.Errorf("Failed to handle manual uncordon for node %s: %v", newNode.Name, err)
		}
	} else {
		klog.Warningf("Manual uncordon callback not registered for node %s - manual uncordon will not be handled",
			newNode.Name)
	}

	return true
}

// handleUpdateNode recalculates counts when a node is updated.
func (ni *NodeInformer) handleUpdateNode(oldObj, newObj interface{}) {
	oldNode, okOld := oldObj.(*v1.Node)

	newNode, okNew := newObj.(*v1.Node)
	if !okOld || !okNew {
		klog.Errorf("Update event: expected Node objects, got %T and %T", oldObj, newObj)
		return
	}

	// Check if quarantine annotations have changed
	quarantineAnnotationsChanged := hasQuarantineAnnotationsChanged(oldNode.Annotations, newNode.Annotations)

	// Check for manual uncordon and handle it
	if ni.detectAndHandleManualUncordon(oldNode, newNode) {
		// Return early as the manual uncordon handler will take care of everything
		return
	}

	// Only process if unschedulable status changed or if the quarantine annotation is present.
	// the reason it needs to be checked for quarantine annotation is because in dryrun node,
	// node is not marked as unschedulable but still annotation will be present, so we need to track those nodes as well.
	if oldNode.Spec.Unschedulable != newNode.Spec.Unschedulable ||
		oldNode.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey] !=
			newNode.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey] {
		klog.V(4).Infof("Node updated: %s (Unschedulable: %t -> %t)", newNode.Name,
			oldNode.Spec.Unschedulable, newNode.Spec.Unschedulable)
		ni.updateNodeQuarantineStatus(newNode)
		ni.signalWork()
	} else {
		klog.V(4).Infof("Node update ignored (no relevant change): %s", newNode.Name)
	}

	// Notify about quarantine annotation changes
	if quarantineAnnotationsChanged && ni.onNodeAnnotationsChanged != nil {
		quarantineAnnotations := getQuarantineAnnotations(newNode.Annotations)
		ni.onNodeAnnotationsChanged(newNode.Name, quarantineAnnotations)
	}
}

// updateNodeQuarantineStatus updates the node's quarantine status based on its schedulability
// and returns true if the status was changed
func (ni *NodeInformer) updateNodeQuarantineStatus(node *v1.Node) bool {
	ni.mutex.Lock()
	defer ni.mutex.Unlock()

	nodeName := node.Name
	shouldBeQuarantined := node.Spec.Unschedulable
	currentlyQuarantined := ni.nodeInfo.GetNodeQuarantineStatusCache(nodeName)

	// Only update if there's a difference between current and desired state
	if currentlyQuarantined != shouldBeQuarantined {
		annotationExist := false

		if _, exists := node.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey]; exists {
			annotationExist = true
		}

		ni.nodeInfo.MarkNodeQuarantineStatusCache(nodeName, shouldBeQuarantined, annotationExist)

		return true
	}

	return false
}

// SetOnQuarantinedNodeDeletedCallback sets the callback function for when a quarantined node is deleted
func (ni *NodeInformer) SetOnQuarantinedNodeDeletedCallback(callback func(nodeName string)) {
	ni.onQuarantinedNodeDeleted = callback
}

// SetOnNodeAnnotationsChangedCallback sets the callback function for when a node's annotations change
func (ni *NodeInformer) SetOnNodeAnnotationsChangedCallback(callback func(nodeName string,
	annotations map[string]string)) {
	ni.onNodeAnnotationsChanged = callback
}

// SetOnManualUncordonCallback sets the callback function for when a node is manually uncordoned
func (ni *NodeInformer) SetOnManualUncordonCallback(callback func(nodeName string) error) {
	ni.onManualUncordon = callback
}

// handleDeleteNode recalculates counts when a node is deleted.
func (ni *NodeInformer) handleDeleteNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		// Handle deletion notifications potentially wrapped in DeletedFinalStateUnknown
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Delete event: expected Node object or DeletedFinalStateUnknown, got %T", obj)
			return
		}

		node, ok = tombstone.Obj.(*v1.Node)
		if !ok {
			klog.Errorf("Delete event: DeletedFinalStateUnknown contained non-Node object %T", tombstone.Obj)
			return
		}
	}

	klog.Infof("Node deleted: %s", node.Name)

	ni.mutex.Lock()

	// Check if the node was quarantined and had the quarantine annotation
	hadQuarantineAnnotation := false

	if ni.nodeInfo.GetNodeQuarantineStatusCache(node.Name) {
		if _, exists := node.Annotations[common.QuarantineHealthEventIsCordonedAnnotationKey]; exists {
			hadQuarantineAnnotation = true
		}

		// Update the cache and delete the node name from the map if the annotation is present
		ni.nodeInfo.MarkNodeQuarantineStatusCache(node.Name, false, false)
	}

	// handle a case where a node is cordoned but not by nvsentinel, then if its entry is there in the cache,
	// we need to remove it
	if node.Spec.Unschedulable {
		ni.nodeInfo.MarkNodeQuarantineStatusCache(node.Name, false, false)
	}

	ni.totalGpuNodes--
	ni.mutex.Unlock()

	// If the node was quarantined and had the annotation, call the callback so that
	// currentQuarantinedNodes metric is decremented
	if hadQuarantineAnnotation && ni.onQuarantinedNodeDeleted != nil {
		ni.onQuarantinedNodeDeleted(node.Name)
	}

	// Notify about node deletion to clear from annotations cache
	if ni.onNodeAnnotationsChanged != nil {
		// Pass nil to indicate the node has been deleted
		ni.onNodeAnnotationsChanged(node.Name, nil)
	}

	ni.signalWork()
}

// recalculateCounts lists all relevant nodes from the cache and updates the counts.
// It returns true if the counts changed, false otherwise.
func (ni *NodeInformer) recalculateCounts() (bool, error) {
	// Use List with Everything selector as the lister is already filtered by the factory
	nodes, err := ni.lister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("failed to list nodes from informer cache: %w", err)
	}

	klog.V(3).Infof("NodeInformer lister returned %d nodes from cache", len(nodes))

	total := 0
	unschedulable := 0

	for _, node := range nodes {
		// Double-check the label, although the informer should only list matching nodes
		if _, exists := node.Labels[GpuNodeLabel]; exists {
			total++

			if node.Spec.Unschedulable {
				ni.nodeInfo.MarkNodeQuarantineStatusCache(node.Name, true, false)

				unschedulable++
			}
		} else {
			klog.Warningf("Node %s found in informer cache despite missing label %s", node.Name, GpuNodeLabel)
		}
	}

	ni.mutex.Lock()
	quarantinedCount := ni.nodeInfo.GetQuarantinedNodesCount()
	changed := ni.totalGpuNodes != total || quarantinedCount != unschedulable
	ni.totalGpuNodes = total
	ni.mutex.Unlock()

	switch {
	case total == 0:
		klog.Warningf("Node counts recalculated: Total GPU Nodes=0 (informer cache may still be populating)")
	case changed:
		klog.Infof("Node counts updated: Total GPU Nodes=%d, Unschedulable GPU Nodes=%d", total, unschedulable)
	default:
		klog.V(4).Infof("Node counts recalculated, no change: Total GPU Nodes=%d, Unschedulable GPU Nodes=%d",
			total, unschedulable)
	}

	return changed, nil
}

// signalWork sends a non-blocking signal to the reconciler's work channel.
func (ni *NodeInformer) signalWork() {
	if ni.workSignal == nil {
		klog.Errorf("No channel configured for node informer")
		return // No channel configured
	}
	select {
	case ni.workSignal <- struct{}{}:
		klog.V(3).Infof("Signalled work channel due to node change.")
	default:
		klog.V(3).Infof("Work channel already signalled, skipping signal for node change.")
	}
}

// Copyright (C) 2020, MinIO, Inc.
//
// This code is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License, version 3,
// along with this program.  If not, see <http://www.gnu.org/licenses/>

package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachineryPkgRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/minio/minio-go/v7/pkg/set"
	topologyv1alpha1 "github.com/minio/operator/pkg/apis/topology.xai/v1alpha1"
	clientset "github.com/minio/operator/pkg/client/clientset/versioned"
)

const (
	// IDCFailureThreshold defines the percentage of NotReady nodes that triggers IDC failure
	IDCFailureThreshold = 75.0
	// IDCRecoveryThreshold defines the percentage below which IDC is considered recovered
	IDCRecoveryThreshold = 75.0
)

// IDCFailureManager manages IDC failure detection and tenant nodeAffinity updates
type IDCFailureManager struct {
	// dynamicClient for accessing IDC topology CRD
	dynamicClient dynamic.Interface
	// minioClientSet for updating tenants
	minioClientSet clientset.Interface
	// namespacesToWatch restricts the action to specific namespaces
	namespacesToWatch set.StringSet
	// informer for watching IDC topology changes
	informer cache.SharedIndexInformer
	// stopCh for stopping the informer
	stopCh chan struct{}
}

// NewIDCFailureManager creates a new IDC failure manager
func NewIDCFailureManager(dynamicClient dynamic.Interface, minioClientSet clientset.Interface, namespacesToWatch set.StringSet) *IDCFailureManager {
	return &IDCFailureManager{
		dynamicClient:     dynamicClient,
		minioClientSet:    minioClientSet,
		namespacesToWatch: namespacesToWatch,
		stopCh:            make(chan struct{}),
	}
}

// Start starts the IDC failure manager
func (m *IDCFailureManager) Start(ctx context.Context) error {
	klog.Info("[YBS] Starting IDC Failure Manager...")

	// Create dynamic informer for IDC topology
	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(m.dynamicClient, time.Minute*5)
	gvr := schema.GroupVersionResource{
		Group:    "topology.xai",
		Version:  "v1alpha1",
		Resource: "idctopologies",
	}

	m.informer = informerFactory.ForResource(gvr).Informer()

	// Add event handlers
	m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			m.handleIDCTopologyUpdate(ctx, oldObj, newObj)
		},
	})

	// Start the informer
	go informerFactory.Start(m.stopCh)

	// Wait for cache sync
	klog.Info("[YBS] Waiting for IDC topology cache to sync...")
	if !cache.WaitForCacheSync(m.stopCh, m.informer.HasSynced) {
		return fmt.Errorf("failed to sync IDC topology cache")
	}

	klog.Info("[YBS] IDC Failure Manager started successfully")
	return nil
}

// Stop stops the IDC failure manager
func (m *IDCFailureManager) Stop() {
	klog.Info("[YBS] Stopping IDC Failure Manager...")
	close(m.stopCh)
}

// handleIDCTopologyUpdate handles IDC topology update events
func (m *IDCFailureManager) handleIDCTopologyUpdate(ctx context.Context, oldObj, newObj interface{}) {
	klog.V(4).Info("[YBS] IDC topology update detected")

	oldTopology, err := m.convertToIDCTopology(oldObj)
	if err != nil {
		klog.Errorf("[YBS] Failed to convert old topology: %v", err)
		return
	}

	newTopology, err := m.convertToIDCTopology(newObj)
	if err != nil {
		klog.Errorf("[YBS] Failed to convert new topology: %v", err)
		return
	}

	// Detect IDC failures and recoveries
	failedIDCs := m.detectFailedIDCs(oldTopology, newTopology)
	recoveredIDCs := m.detectRecoveredIDCs(oldTopology, newTopology)

	// Update tenants if needed
	if len(failedIDCs) > 0 {
		klog.Warningf("[YBS] Detected failed IDCs: %v", failedIDCs)
		if err := m.updateTenantsForFailedIDCs(ctx, failedIDCs); err != nil {
			klog.Errorf("[YBS] Failed to update tenants for failed IDCs: %v", err)
		}
	}

	if len(recoveredIDCs) > 0 {
		klog.Infof("[YBS] Detected recovered IDCs: %v", recoveredIDCs)
		if err := m.updateTenantsForRecoveredIDCs(ctx, recoveredIDCs); err != nil {
			klog.Errorf("[YBS] Failed to update tenants for recovered IDCs: %v", err)
		}
	}
}

// convertToIDCTopology converts unstructured object to IDCTopology
func (m *IDCFailureManager) convertToIDCTopology(obj interface{}) (*topologyv1alpha1.IDCTopology, error) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object is not unstructured")
	}

	topology := &topologyv1alpha1.IDCTopology{}
	err := apimachineryPkgRuntime.DefaultUnstructuredConverter.FromUnstructured(
		unstructuredObj.UnstructuredContent(), topology)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to IDCTopology: %v", err)
	}

	return topology, nil
}

// detectFailedIDCs detects newly failed IDCs (crossing 75% threshold)
func (m *IDCFailureManager) detectFailedIDCs(oldTopology, newTopology *topologyv1alpha1.IDCTopology) []string {
	var failedIDCs []string

	for _, newIDC := range newTopology.Spec.IDCs {
		idcName := newIDC.IDCName
		newFailureRate := m.calculateNotReadyPercentage(newIDC)

		// Check if this IDC crossed the failure threshold
		if newFailureRate >= IDCFailureThreshold {
			// Check if it wasn't failed before
			oldFailureRate := m.getOldIDCFailureRate(oldTopology, idcName)

			if oldFailureRate < IDCFailureThreshold {
				failedIDCs = append(failedIDCs, idcName)
				klog.Warningf("[YBS] IDC %s failed: %.1f%% nodes are NotReady (threshold: %.1f%%)",
					idcName, newFailureRate, IDCFailureThreshold)
			}
		}
	}

	return failedIDCs
}

// detectRecoveredIDCs detects IDCs that have recovered (below 75% threshold)
func (m *IDCFailureManager) detectRecoveredIDCs(oldTopology, newTopology *topologyv1alpha1.IDCTopology) []string {
	var recoveredIDCs []string

	for _, newIDC := range newTopology.Spec.IDCs {
		idcName := newIDC.IDCName
		newFailureRate := m.calculateNotReadyPercentage(newIDC)

		// Check if this IDC recovered (now below threshold)
		if newFailureRate < IDCRecoveryThreshold {
			// Check if it was failed before
			oldFailureRate := m.getOldIDCFailureRate(oldTopology, idcName)

			if oldFailureRate >= IDCRecoveryThreshold {
				recoveredIDCs = append(recoveredIDCs, idcName)
				klog.Infof("[YBS] IDC %s recovered: %.1f%% nodes are NotReady (threshold: %.1f%%)",
					idcName, newFailureRate, IDCRecoveryThreshold)
			}
		}
	}

	return recoveredIDCs
}

// calculateNotReadyPercentage calculates the percentage of NotReady nodes in an IDC
func (m *IDCFailureManager) calculateNotReadyPercentage(idc topologyv1alpha1.IDC) float64 {
	totalNodes := len(idc.Nodes)
	if totalNodes == 0 {
		return 0.0
	}

	notReadyCount := 0
	for _, node := range idc.Nodes {
		if node.NodeStatus == "NotReady" {
			notReadyCount++
		}
	}

	return float64(notReadyCount) / float64(totalNodes) * 100.0
}

// getOldIDCFailureRate gets the failure rate of an IDC from the old topology
func (m *IDCFailureManager) getOldIDCFailureRate(oldTopology *topologyv1alpha1.IDCTopology, idcName string) float64 {
	for _, oldIDC := range oldTopology.Spec.IDCs {
		if oldIDC.IDCName == idcName {
			return m.calculateNotReadyPercentage(oldIDC)
		}
	}
	// IDC didn't exist before, consider it as healthy
	return 0.0
}

// updateTenantsForFailedIDCs updates all tenants to avoid failed IDCs
func (m *IDCFailureManager) updateTenantsForFailedIDCs(ctx context.Context, failedIDCs []string) error {
	return m.updateAllTenantsNodeAffinity(ctx, failedIDCs, true)
}

// updateTenantsForRecoveredIDCs updates all tenants to allow recovered IDCs
func (m *IDCFailureManager) updateTenantsForRecoveredIDCs(ctx context.Context, recoveredIDCs []string) error {
	return m.updateAllTenantsNodeAffinity(ctx, recoveredIDCs, false)
}

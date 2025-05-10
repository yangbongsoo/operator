// Copyright (C) 2021, MinIO, Inc.
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
	"strings"
	"time"

	"github.com/minio/madmin-go/v3"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryPkgRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"

	miniov2 "github.com/minio/operator/pkg/apis/minio.min.io/v2"
	topologyv1alpha1 "github.com/minio/operator/pkg/apis/topology.xai/v1alpha1"
)

const (
	// HealthUnavailableMessage means MinIO is down
	HealthUnavailableMessage = "Service Unavailable"
	// HealthHealingMessage means MinIO is healing one of more drives
	HealthHealingMessage = "Healing"
	// HealthReduceAvailabilityMessage some drives are offline
	HealthReduceAvailabilityMessage = "Reduced Availability"
)

// recurrentTenantStatusMonitor loop that checks every N minutes for tenants health
func (c *Controller) recurrentTenantStatusMonitor(stopCh <-chan struct{}) {
	// How often will this function run
	interval := miniov2.GetMonitoringInterval()
	ticker := time.NewTicker(time.Duration(interval) * time.Minute)
	defer func() {
		klog.Info("recurrent pod status monitor closed")
	}()
	for {
		select {
		case <-ticker.C:
			if err := c.tenantsHealthMonitor(); err != nil {
				klog.Infof("%v", err)
			}
		case <-stopCh:
			ticker.Stop()
			return
		}
	}
}

func (c *Controller) tenantsHealthMonitor() error {
	// list all tenants and get their cluster health
	tenants, err := c.minioClientSet.MinioV2().Tenants("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, t := range tenants.Items {
		tenant, err := c.updateHealthStatusForTenant(&t)
		if err != nil {
			klog.Errorf("%v", err)
			return err
		}
		// Add tenant to the health check queue until is green again
		if tenant != nil && tenant.Status.HealthStatus != miniov2.HealthStatusGreen {
			key := fmt.Sprintf("%s/%s", tenant.GetNamespace(), tenant.GetName())
			c.healthCheckQueue.Add(key)
		}
	}
	return nil
}

func (c *Controller) updateHealthStatusForTenant(tenant *miniov2.Tenant) (*miniov2.Tenant, error) {
	// don't get the tenant cluster health if it doesn't have at least 1 pool initialized
	oneInitialized := false
	for _, pool := range tenant.Status.Pools {
		if pool.State == miniov2.PoolInitialized {
			oneInitialized = true
		}
	}
	if !oneInitialized {
		klog.Infof("'%s/%s' no pool is initialized", tenant.Namespace, tenant.Name)
		return tenant, nil
	}

	tenantConfiguration, err := c.getTenantCredentials(context.Background(), tenant)
	if err != nil {
		return nil, err
	}

	adminClnt, err := tenant.NewMinIOAdmin(tenantConfiguration, c.getTransport())
	if err != nil {
		klog.Errorf("Error instantiating adminClnt '%s/%s': %v", tenant.Namespace, tenant.Name, err)
		return nil, err
	}

	aClnt, err := madmin.NewAnonymousClient(tenant.MinIOServerHostAddress(), tenant.TLS())
	if err != nil {
		// show the error and continue
		klog.Infof("'%s/%s': %v", tenant.Namespace, tenant.Name, err)
		return tenant, nil
	}
	aClnt.SetCustomTransport(c.getTransport())

	hctx, hcancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer hcancel()

	// get cluster health for tenant
	healthResult, err := aClnt.Healthy(hctx, madmin.HealthOpts{})
	if err != nil {
		// show the error and continue
		klog.Infof("'%s/%s' Failed to get cluster health: %v", tenant.Namespace, tenant.Name, err)
		return tenant, nil
	}

	tenant.Status.DrivesHealing = int32(healthResult.HealingDrives)
	tenant.Status.WriteQuorum = int32(healthResult.WriteQuorum)

	if healthResult.Healthy {
		tenant.Status.HealthStatus = miniov2.HealthStatusGreen
		tenant.Status.HealthMessage = ""
	} else {
		tenant.Status.HealthStatus = miniov2.HealthStatusRed
		tenant.Status.HealthMessage = HealthUnavailableMessage
	}

	// check all the tenant pods, if at least 1 is not running, we go yellow
	tenantPods, err := c.kubeClientSet.CoreV1().Pods(tenant.Namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", miniov2.TenantLabel, tenant.Name),
	})
	if err != nil {
		return nil, err
	}

	allPodsRunning := true
	for _, pod := range tenantPods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			allPodsRunning = false
		}
	}
	if !allPodsRunning && tenant.Status.HealthStatus != miniov2.HealthStatusRed {
		tenant.Status.HealthStatus = miniov2.HealthStatusYellow
	}

	// partial status update, since the storage info might take a while
	if tenantUpdate, err := c.updatePoolStatus(context.Background(), tenant); err != nil {
		klog.Infof("'%s/%s' Can't update tenant status: %v", tenant.Namespace, tenant.Name, err)
	} else {
		tenant = tenantUpdate
	}

	srvInfoCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	storageInfo, err := adminClnt.StorageInfo(srvInfoCtx)
	if err != nil {
		// show the error and continue
		klog.Infof("'%s/%s' Failed to get storage info: %v", tenant.Namespace, tenant.Name, err)
		return tenant, nil
	}

	// Raw capacity: Total amount of physical disk space reserved for MinIO
	// Raw usage: Total amount of physical disk space actually in use by MinIO
	// Capacity: Net capacity that can actually be stored inside MinIO
	// Usage: Net usage actually stored data inside MinIO
	//
	// The net capacity/usage is derived from the raw capacity/usage by
	// subtracting the additional stored parity data from the physical data.
	// Because objects are stored in blocks, the "net usage" reported here
	// may be higher than the sum of all object sizes.
	var rawCapacity, rawUsage, capacity, usage uint64

	standardSCData := storageInfo.Backend.StandardSCData
	if nrOfPools := len(standardSCData); nrOfPools > 0 {
		standardSCParities := storageInfo.Backend.StandardSCParities
		if len(standardSCParities) == 0 {
			// Per-pool parity is not always returned, so it's assumed
			// that each pool uses the standard parity if not
			standardSCParities = make([]int, nrOfPools)
			for pool := range nrOfPools {
				standardSCParities[pool] = storageInfo.Backend.StandardSCParity
			}
		}

		// calculate the raw capacity/usage per pool
		rawPoolCapacities := make([]uint64, nrOfPools)
		rawPoolUsages := make([]uint64, nrOfPools)
		for _, disk := range storageInfo.Disks {
			pi := disk.PoolIndex
			if pi >= nrOfPools {
				// make sure that invalid pool index won't panic the operator
				// the result will be 0 for (raw) capacity/usage.
				goto bailout
			}
			rawPoolCapacities[pi] = rawPoolCapacities[pi] + disk.AvailableSpace
			rawPoolUsages[pi] = rawPoolUsages[pi] + disk.UsedSpace
		}

		// calculate the total capacity/usage for the cluster
		for pool := range len(standardSCData) {
			rawCapacity = rawCapacity + rawPoolCapacities[pool]
			rawUsage = rawUsage + rawPoolUsages[pool]

			poolEfficiency := float64(standardSCData[pool]) / float64(standardSCData[pool]+standardSCParities[pool])
			capacity = capacity + uint64(poolEfficiency*float64(rawPoolCapacities[pool]))
			usage = usage + uint64(poolEfficiency*float64(rawPoolUsages[pool]))
		}
	}

bailout:
	// use safe conversions to signed integer to avoid negative sizes
	tenant.Status.Usage.RawCapacity = safeToInt64(rawCapacity)
	tenant.Status.Usage.RawUsage = safeToInt64(rawUsage)
	tenant.Status.Usage.Capacity = safeToInt64(capacity)
	tenant.Status.Usage.Usage = safeToInt64(usage)

	var onlineDisks, offlineDisks int32
	for _, disk := range storageInfo.Disks {
		if disk.State == madmin.DriveStateOk {
			onlineDisks++
		} else {
			offlineDisks++
		}
	}

	tenant.Status.DrivesOnline = onlineDisks
	tenant.Status.DrivesOffline = offlineDisks

	if tenant.Status.DrivesOffline > 0 || tenant.Status.DrivesHealing > 0 {
		tenant.Status.HealthStatus = miniov2.HealthStatusYellow
		if tenant.Status.DrivesHealing > 0 {
			tenant.Status.HealthMessage = HealthHealingMessage
		} else {
			tenant.Status.HealthMessage = HealthReduceAvailabilityMessage
		}
	}
	if tenant.Status.DrivesOnline < tenant.Status.WriteQuorum {
		tenant.Status.HealthStatus = miniov2.HealthStatusRed
		tenant.Status.HealthMessage = HealthUnavailableMessage
	}

	// only if no disks are offline and we are not healing, we are green
	if tenant.Status.DrivesOffline == 0 && tenant.Status.DrivesHealing == 0 {
		tenant.Status.HealthStatus = miniov2.HealthStatusGreen
		tenant.Status.HealthMessage = ""
	}

	if tenant, err = c.updatePoolStatus(context.Background(), tenant); err != nil {
		klog.Infof("'%s/%s' Can't update tenant status: %v", tenant.Namespace, tenant.Name, err)
	}

	// Store the usage reported by the tiers
	tiersStatsCtx, cancelTiers := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelTiers()
	tInfos, err := adminClnt.TierStats(tiersStatsCtx)
	if err != nil {
		klog.Infof("'%s/%s' Can't retrieve tenant tiers: %v", tenant.Namespace, tenant.Name, err)
	}
	if tInfos != nil {
		var tiersUsage []miniov2.TierUsage
		for _, tier := range tInfos {
			tiersUsage = append(tiersUsage, miniov2.TierUsage{
				Name:      tier.Name,
				Type:      tier.Type,
				TotalSize: int64(tier.Stats.TotalSize),
			})
		}
		tenant.Status.Usage.Tiers = tiersUsage
		if tenant, err = c.updatePoolStatus(context.Background(), tenant); err != nil {
			klog.Infof("'%s/%s' Can't update tenant status with tiers: %v", tenant.Namespace, tenant.Name, err)
		}
	}

	return tenant, nil
}

// HealthResult holds the results from cluster/health query into MinIO
type HealthResult struct {
	StatusCode        int
	HealingDrives     int
	WriteQuorumDrives int
}

// syncHealthCheckHandler acts on work items from the healthCheckQueue
func (c *Controller) syncHealthCheckHandler(key string) (Result, error) {
	// Convert the namespace/name string into a distinct namespace and name
	if key == "" {
		runtime.HandleError(fmt.Errorf("Invalid resource key: %s", key))
		return WrapResult(Result{}, nil)
	}

	namespace, tenantName := key2NamespaceName(key)

	// Get the Tenant resource with this namespace/name
	tenant, err := c.minioClientSet.MinioV2().Tenants(namespace).Get(context.Background(), tenantName, metav1.GetOptions{})
	if err != nil {
		// The Tenant resource may no longer exist, in which case we stop processing.
		if k8serrors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("Tenant '%s' in work queue no longer exists", key))
			return WrapResult(Result{}, nil)
		}
		return WrapResult(Result{}, err)
	}

	tenant.EnsureDefaults()

	tenant, err = c.updateHealthStatusForTenant(tenant)
	if err != nil {
		klog.Errorf("%v", err)
		return WrapResult(Result{}, err)
	}

	// Add tenant to the health check queue again until is green again
	if tenant != nil && tenant.Status.HealthStatus != miniov2.HealthStatusGreen {
		c.healthCheckQueue.AddAfter(key, 1*time.Second)
	}

	return WrapResult(Result{}, nil)
}

// podHealthFailures tracks consecutive health check failures for each pod
var podHealthFailures = make(map[string]int)

// checkMinIOPodsHealth checks the health of each MinIO pod in a tenant
func (c *Controller) checkMinIOPodsHealth(tenant *miniov2.Tenant) error {
	klog.Infof("[YBS] checkMinIOPodsHealth called")
	// Get all pods for the tenant
	tenantPods, err := c.kubeClientSet.CoreV1().Pods(tenant.Namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", miniov2.TenantLabel, tenant.Name),
	})
	if err != nil {
		return fmt.Errorf("[YBS] failed to get pods for tenant %s/%s: %v", tenant.Namespace, tenant.Name, err)
	}

	// Get existing IDCTopology
	idcTopologyRes := schema.GroupVersionResource{
		Group:    "topology.xai",
		Version:  "v1alpha1",
		Resource: "idctopologies",
	}

	idcTopology, err := c.dynamicClient.Resource(idcTopologyRes).Namespace("default").Get(context.TODO(), "minio-topology", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.Infof("[YBS] IDCTopology not found yet, skipping health check updates")
			return nil
		}
		klog.Errorf("[YBS] Failed to get IDCTopology: %v", err)
		return nil
	}

	existingTopology := &topologyv1alpha1.IDCTopology{}
	if err := apimachineryPkgRuntime.DefaultUnstructuredConverter.FromUnstructured(idcTopology.UnstructuredContent(), existingTopology); err != nil {
		klog.Errorf("[YBS] Failed to convert from unstructured: %v", err)
		return nil
	}

	// Check health for each pod
	for _, pod := range tenantPods.Items {
		// Get pod's MinIO server address
		podAddress := fmt.Sprintf("%s.%s.%s.svc.%s:9000",
			pod.Name,
			tenant.MinIOHLServiceName(),
			tenant.Namespace,
			miniov2.GetClusterDomain())

		klog.Infof("[YBS] Checking pod %s at address %s", pod.Name, podAddress)

		// Create anonymous client for health check
		aClnt, err := madmin.NewAnonymousClient(podAddress, tenant.TLS())
		if err != nil {
			klog.Errorf("[YBS] Failed to create anonymous client for pod %s: %v", pod.Name, err)
			podHealthFailures[pod.Name]++
			continue
		}
		aClnt.SetCustomTransport(c.getTransport())

		// Set timeout for health check
		hctx, hcancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer hcancel()

		// Check pod health
		// healthResult, err := aClnt.Healthy(hctx, madmin.HealthOpts{})
		aliveCh := aClnt.Alive(hctx, madmin.AliveOpts{})
		result := <-aliveCh
		if result.Error != nil {
			if isNetworkError(result.Error) {
				klog.Infof("[YBS] isNetworkError podHealthFailures[%s]: %d, network error: %v", pod.Name, podHealthFailures[pod.Name], result.Error)
				podHealthFailures[pod.Name]++
			} else {
				klog.Infof("[YBS] Pod.Name: %s, non-network error, resetting counter. err: %v", pod.Name, result.Error)
				podHealthFailures[pod.Name] = 0
			}
		} else {
			// Reset failure count if health check succeeds
			klog.Infof("[YBS] health check succeeded for pod %s", pod.Name)
			podHealthFailures[pod.Name] = 0
		}
		// if err != nil {
		// 	if isNetworkError(err) {
		// 		klog.Infof("[YBS] isNetworkError podHealthFailures[%s]: %d, network error: %v", pod.Name, podHealthFailures[pod.Name], err)
		// 		podHealthFailures[pod.Name]++
		// 	} else {
		// 		klog.Infof("[YBS] Pod.Name: %s, non-network error, resetting counter. err: %v", pod.Name, err)
		// 		podHealthFailures[pod.Name] = 0
		// 	}
		// } else if healthResult.Healthy {
		// 	// Reset failure count if health check succeeds
		// 	klog.Infof("[YBS] healthResult.Healthy podHealthFailures[%s]: %d, err: %v", pod.Name, podHealthFailures[pod.Name], err)
		// 	podHealthFailures[pod.Name] = 0
		// } else {
		// 	klog.Infof("[YBS] Else podHealthFailures[%s]: %d, healthResult: %v", pod.Name, podHealthFailures[pod.Name], healthResult)
		// 	// For unhealthy but reachable pods, reset the counter
		// 	podHealthFailures[pod.Name] = 0
		// }

		// If pod has failed 3 consecutive network checks, update IDCTopology
		if podHealthFailures[pod.Name] >= 2 {
			klog.Infof("[YBS] podHealthFailures[%s]: %d, updating IDCTopology", pod.Name, podHealthFailures[pod.Name])
			// Find the pod in existing topology and update its NodeStatus
			for _, idc := range existingTopology.Spec.IDCs {
				for _, node := range idc.Nodes {
					if node.Pod == pod.Name {
						klog.Infof("[YBS] node.Pod == pod.Name %s, updating IDCTopology", pod.Name)
						// Create new NodeInfo with updated status
						nodeInfo := NodeInfo{
							IDC:        idc.IDCName,
							Node:       node.Node,
							NodeStatus: "NotReady",
							Pod:        node.Pod,
							PodStatus:  node.PodStatus,
						}
						updateOrCreateIDCTopology(c.dynamicClient, nodeInfo)
						// Reset failure count after update
						podHealthFailures[pod.Name] = 0
						break
					}
				}
			}
		}
	}

	return nil
}

// isNetworkError checks if the error is a network-related error
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common network error types
	if strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "context deadline exceeded") ||
		strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "network is unreachable") ||
		strings.Contains(err.Error(), "connection timed out") {
		return true
	}

	return false
}

// startPodHealthMonitor starts a goroutine that periodically checks the health of all MinIO pods
func (c *Controller) startPodHealthMonitor(stopCh <-chan struct{}) {
	klog.Info("[YBS] startPodHealthMonitor called")
	time.Sleep(1 * time.Minute)
	klog.Info("[YBS] startPodHealthMonitor after 1 minute")

	ticker := time.NewTicker(10 * time.Second) // 10초마다 체크
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tenants, err := c.minioClientSet.MinioV2().Tenants("").List(context.Background(), metav1.ListOptions{})
			if err != nil {
				klog.Infof("[YBS] Failed to list tenants: %v", err)
				continue
			}

			// Check health for each tenant's pods
			for _, tenant := range tenants.Items {
				if err := c.checkMinIOPodsHealth(&tenant); err != nil {
					klog.Infof("[YBS] Failed to check pod health for tenant %s/%s: %v",
						tenant.Namespace, tenant.Name, err)
				}
			}
		case <-stopCh:
			return
		}
	}
}

// safeToInt64 converts an unsigned 64-bit integer to a signed int64
// and round to the upper limit of the integer, instead of returning
// a negative value.
func safeToInt64(u uint64) int64 {
	const maxInt64 = ^uint64(0) >> 1
	if u > maxInt64 {
		u = maxInt64
	}
	return int64(u)
}

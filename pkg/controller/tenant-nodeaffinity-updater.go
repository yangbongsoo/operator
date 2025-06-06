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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	miniov2 "github.com/minio/operator/pkg/apis/minio.min.io/v2"
)

// updateAllTenantsNodeAffinity updates nodeAffinity for all tenants in watched namespaces
func (m *IDCFailureManager) updateAllTenantsNodeAffinity(ctx context.Context, idcs []string, shouldAvoid bool) error {
	// Handle empty or nil namespace set (watch all namespaces)
	if m.namespacesToWatch.IsEmpty() {
		// Watch all namespaces - get all tenants from all namespaces
		tenants, err := m.minioClientSet.MinioV2().Tenants("").List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.Errorf("[YBS] Failed to list tenants in all namespaces: %v", err)
			return err
		}

		for _, tenant := range tenants.Items {
			if err := m.updateTenantNodeAffinity(ctx, &tenant, idcs, shouldAvoid); err != nil {
				klog.Errorf("[YBS] Failed to update tenant %s/%s nodeAffinity: %v",
					tenant.Namespace, tenant.Name, err)
			}
		}
	} else {
		// Watch specific namespaces
		for namespace := range m.namespacesToWatch {
			tenants, err := m.minioClientSet.MinioV2().Tenants(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				klog.Errorf("[YBS] Failed to list tenants in namespace %s: %v", namespace, err)
				continue
			}

			for _, tenant := range tenants.Items {
				if err := m.updateTenantNodeAffinity(ctx, &tenant, idcs, shouldAvoid); err != nil {
					klog.Errorf("[YBS] Failed to update tenant %s/%s nodeAffinity: %v",
						tenant.Namespace, tenant.Name, err)
				}
			}
		}
	}
	return nil
}

// updateTenantNodeAffinity updates a single tenant's nodeAffinity for specific IDCs
func (m *IDCFailureManager) updateTenantNodeAffinity(ctx context.Context, tenant *miniov2.Tenant, idcs []string, shouldAvoid bool) error {
	if len(idcs) == 0 {
		return nil
	}

	updated := false
	tenantCopy := tenant.DeepCopy()

	// Update each pool's nodeAffinity
	for i := range tenantCopy.Spec.Pools {
		pool := &tenantCopy.Spec.Pools[i]

		// Initialize affinity structure if needed
		m.initializePoolAffinity(pool)

		if shouldAvoid {
			// Add NotIn constraint for failed IDCs
			if m.addNotInConstraint(pool, idcs) {
				updated = true
			}
		} else {
			// Remove NotIn constraint for recovered IDCs
			if m.removeNotInConstraint(pool, idcs) {
				updated = true
			}
		}
	}

	if updated {
		// Update the tenant spec
		_, err := m.minioClientSet.MinioV2().Tenants(tenant.Namespace).Update(ctx, tenantCopy, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update tenant: %v", err)
		}

		action := "avoid"
		if !shouldAvoid {
			action = "allow"
		}

		klog.Infof("[YBS] Updated tenant %s/%s nodeAffinity to %s IDCs: %v",
			tenant.Namespace, tenant.Name, action, idcs)
	}

	return nil
}

// initializePoolAffinity initializes the affinity structure for a pool
func (m *IDCFailureManager) initializePoolAffinity(pool *miniov2.Pool) {
	if pool.Affinity == nil {
		pool.Affinity = &corev1.Affinity{}
	}
	if pool.Affinity.NodeAffinity == nil {
		pool.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if pool.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		pool.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{},
		}
	}
}

// addNotInConstraint adds NotIn constraint for specified IDCs
func (m *IDCFailureManager) addNotInConstraint(pool *miniov2.Pool, idcs []string) bool {
	nodeSelector := pool.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

	// Find existing zone constraint with NotIn operator
	for i, term := range nodeSelector.NodeSelectorTerms {
		for j, expr := range term.MatchExpressions {
			if expr.Key == "topology.kubernetes.io/zone" && expr.Operator == corev1.NodeSelectorOpNotIn {
				// Merge with existing NotIn values, avoiding duplicates
				existingValues := make(map[string]bool)
				for _, val := range expr.Values {
					existingValues[val] = true
				}

				var newValues []string
				changed := false

				// Keep existing values
				newValues = append(newValues, expr.Values...)

				// Add new IDCs if not already present
				for _, idc := range idcs {
					if !existingValues[idc] {
						newValues = append(newValues, idc)
						changed = true
					}
				}

				if changed {
					nodeSelector.NodeSelectorTerms[i].MatchExpressions[j].Values = newValues
					klog.V(4).Infof("[YBS] Updated existing NotIn constraint with IDCs: %v", idcs)
				}
				return changed
			}
		}
	}

	// No existing NotIn constraint found, create new one
	newTerm := corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{
			{
				Key:      "topology.kubernetes.io/zone",
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   idcs,
			},
		},
	}
	nodeSelector.NodeSelectorTerms = append(nodeSelector.NodeSelectorTerms, newTerm)
	klog.V(4).Infof("[YBS] Added new NotIn constraint for IDCs: %v", idcs)
	return true
}

// removeNotInConstraint removes NotIn constraint for specified IDCs
func (m *IDCFailureManager) removeNotInConstraint(pool *miniov2.Pool, idcs []string) bool {
	nodeSelector := pool.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

	for i, term := range nodeSelector.NodeSelectorTerms {
		for j, expr := range term.MatchExpressions {
			if expr.Key == "topology.kubernetes.io/zone" && expr.Operator == corev1.NodeSelectorOpNotIn {
				// Remove specified IDCs from NotIn values
				var newValues []string
				changed := false

				for _, val := range expr.Values {
					shouldRemove := false
					for _, idc := range idcs {
						if val == idc {
							shouldRemove = true
							changed = true
							break
						}
					}
					if !shouldRemove {
						newValues = append(newValues, val)
					}
				}

				if changed {
					if len(newValues) == 0 {
						// Remove the entire expression if no values left
						nodeSelector.NodeSelectorTerms[i].MatchExpressions = append(
							term.MatchExpressions[:j],
							term.MatchExpressions[j+1:]...)

						// Remove the entire term if no expressions left
						if len(nodeSelector.NodeSelectorTerms[i].MatchExpressions) == 0 {
							nodeSelector.NodeSelectorTerms = append(
								nodeSelector.NodeSelectorTerms[:i],
								nodeSelector.NodeSelectorTerms[i+1:]...)
						}
						klog.V(4).Infof("[YBS] Removed entire NotIn constraint for IDCs: %v", idcs)
					} else {
						nodeSelector.NodeSelectorTerms[i].MatchExpressions[j].Values = newValues
						klog.V(4).Infof("[YBS] Removed IDCs from NotIn constraint: %v", idcs)
					}
					return true
				}
			}
		}
	}

	return false
}

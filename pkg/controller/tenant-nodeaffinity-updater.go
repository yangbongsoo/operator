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

	// First, try to find and update existing zone NotIn constraint
	for i := range nodeSelector.NodeSelectorTerms {
		term := &nodeSelector.NodeSelectorTerms[i]
		for j := range term.MatchExpressions {
			expr := &term.MatchExpressions[j]
			if expr.Key == "topology.kubernetes.io/zone" && expr.Operator == corev1.NodeSelectorOpNotIn {
				// Found existing NotIn constraint, merge values
				existingValues := make(map[string]bool)
				for _, val := range expr.Values {
					existingValues[val] = true
				}

				changed := false
				// Add new IDCs if not already present
				for _, idc := range idcs {
					if !existingValues[idc] {
						expr.Values = append(expr.Values, idc)
						changed = true
					}
				}

				if changed {
					klog.V(4).Infof("[YBS] Updated existing NotIn constraint with IDCs: %v", idcs)
				}
				return changed
			}
		}
	}

	// No existing NotIn constraint found, add to first term or create new term
	if len(nodeSelector.NodeSelectorTerms) > 0 {
		// Add to the first term
		term := &nodeSelector.NodeSelectorTerms[0]
		term.MatchExpressions = append(term.MatchExpressions, corev1.NodeSelectorRequirement{
			Key:      "topology.kubernetes.io/zone",
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   idcs,
		})
		klog.V(4).Infof("[YBS] Added NotIn constraint to existing term for IDCs: %v", idcs)
	} else {
		// Create new term
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
		klog.V(4).Infof("[YBS] Created new term with NotIn constraint for IDCs: %v", idcs)
	}
	return true
}

// removeNotInConstraint removes NotIn constraint for specified IDCs
func (m *IDCFailureManager) removeNotInConstraint(pool *miniov2.Pool, idcs []string) bool {
	nodeSelector := pool.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	changed := false

	// Create map for faster lookup
	idcsToRemove := make(map[string]bool)
	for _, idc := range idcs {
		idcsToRemove[idc] = true
	}

	// Use reverse iteration to safely remove elements
	for i := len(nodeSelector.NodeSelectorTerms) - 1; i >= 0; i-- {
		term := &nodeSelector.NodeSelectorTerms[i]

		// Use reverse iteration for expressions too
		for j := len(term.MatchExpressions) - 1; j >= 0; j-- {
			expr := &term.MatchExpressions[j]
			if expr.Key == "topology.kubernetes.io/zone" && expr.Operator == corev1.NodeSelectorOpNotIn {
				// Remove specified IDCs from NotIn values
				var newValues []string
				exprChanged := false

				for _, val := range expr.Values {
					if !idcsToRemove[val] {
						newValues = append(newValues, val)
					} else {
						exprChanged = true
					}
				}

				if exprChanged {
					if len(newValues) == 0 {
						// Remove the entire expression if no values left
						term.MatchExpressions = append(term.MatchExpressions[:j], term.MatchExpressions[j+1:]...)
						klog.V(4).Infof("[YBS] Removed entire NotIn expression for IDCs: %v", idcs)
					} else {
						expr.Values = newValues
						klog.V(4).Infof("[YBS] Removed IDCs from NotIn constraint: %v", idcs)
					}
					changed = true
				}
			}
		}

		// Remove the entire term if no expressions left
		if len(term.MatchExpressions) == 0 {
			nodeSelector.NodeSelectorTerms = append(nodeSelector.NodeSelectorTerms[:i], nodeSelector.NodeSelectorTerms[i+1:]...)
			klog.V(4).Infof("[YBS] Removed empty NodeSelectorTerm")
			changed = true
		}
	}

	return changed
}

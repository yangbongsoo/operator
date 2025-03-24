package controller

import (
	"context"

	"gopkg.in/yaml.v2"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	miniov2 "github.com/minio/operator/pkg/apis/minio.min.io/v2"
)

// NodePodStatus represents the status of a node and the pod running on it.
type NodePodStatus struct {
	Node       string `json:"node" yaml:"node"`
	Pod        string `json:"pod" yaml:"pod"`
	NodeStatus string `json:"nodeStatus" yaml:"nodeStatus"`
	PodStatus  string `json:"podStatus" yaml:"podStatus"`
}

// IDCNodePodMap maps IDC zones to a list of NodePodStatus.
// Format: idc1, idc2, idc3 -> NodePodStatus list
type IDCNodePodMap map[string][]NodePodStatus

const (
	// IDCNodePodMapName is the name of the ConfigMap that stores the IDC-Node-Pod mapping information.
	IDCNodePodMapName = "idc-node-pod-map"
	// IDCNodePodMapKey is the key used in the ConfigMap to store the mapping data.
	IDCNodePodMapKey = "map.yaml"
	// TopologyZoneLabel is the label used to identify the IDC zone of a node.
	TopologyZoneLabel = "topology.kubernetes.io/zone"
	// AppLabel is the label used to identify the application type.
	AppLabel = "app"
	// MinIOAppName is the value of the app label for MinIO pods.
	MinIOAppName = "minio"
)

// GetNodePodMapConfigMap retrieves the current idc-node-pod-map ConfigMap.
// Returns nil and error if the ConfigMap does not exist.
func (c *Controller) GetNodePodMapConfigMap(ctx context.Context, namespace string) (*corev1.ConfigMap, error) {
	configMap, err := c.kubeClientSet.CoreV1().ConfigMaps(namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.V(2).Infof("[YBS] ConfigMap %s/%s not found", namespace, IDCNodePodMapName)
			return nil, nil
		}
		klog.Errorf("[YBS] Failed to get ConfigMap %s/%s: %v", namespace, IDCNodePodMapName, err)
		return nil, err
	}
	return configMap, nil
}

// CreateNodePodMapConfigMap creates a new idc-node-pod-map ConfigMap.
func (c *Controller) CreateNodePodMapConfigMap(ctx context.Context, namespace string, idcMap IDCNodePodMap) error {
	// Marshal IDCNodePodMap to YAML
	yamlData, err := yaml.Marshal(idcMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to marshal IDCNodePodMap: %v", err)
		return err
	}

	// Create ConfigMap object
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IDCNodePodMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			IDCNodePodMapKey: string(yamlData),
		},
	}

	// Create ConfigMap via Kubernetes API
	_, err = c.kubeClientSet.CoreV1().ConfigMaps(namespace).Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("[YBS] Failed to create ConfigMap %s/%s: %v", namespace, IDCNodePodMapName, err)
		return err
	}

	klog.Infof("[YBS] Successfully created ConfigMap %s/%s", namespace, IDCNodePodMapName)
	return nil
}

// UpsertNodePodMapConfigMap updates an existing idc-node-pod-map ConfigMap.
// Creates the ConfigMap if it does not exist.
func (c *Controller) UpsertNodePodMapConfigMap(ctx context.Context, namespace string, idcMap IDCNodePodMap) error {
	// Get existing ConfigMap
	existingConfigMap, err := c.GetNodePodMapConfigMap(ctx, namespace)
	if err != nil {
		return err
	}

	// Marshal IDCNodePodMap to YAML
	yamlData, err := yaml.Marshal(idcMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to marshal IDCNodePodMap: %v", err)
		return err
	}

	// Create ConfigMap if it doesn't exist
	if existingConfigMap == nil {
		return c.CreateNodePodMapConfigMap(ctx, namespace, idcMap)
	}

	// Update existing ConfigMap
	updatedConfigMap := existingConfigMap.DeepCopy()
	if updatedConfigMap.Data == nil {
		updatedConfigMap.Data = make(map[string]string)
	}
	updatedConfigMap.Data[IDCNodePodMapKey] = string(yamlData)

	_, err = c.kubeClientSet.CoreV1().ConfigMaps(namespace).Update(ctx, updatedConfigMap, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("[YBS] Failed to update ConfigMap %s/%s: %v", namespace, IDCNodePodMapName, err)
		return err
	}

	klog.V(2).Infof("[YBS] Successfully updated ConfigMap %s/%s", namespace, IDCNodePodMapName)
	return nil
}

// CollectIDCNodePodInfo collects node and MinIO pod information for each IDC zone.
// It identifies IDC zones based on the topology.kubernetes.io/zone label and
// maps MinIO pods running on each node.
func (c *Controller) CollectIDCNodePodInfo(ctx context.Context, tenant *miniov2.Tenant) (IDCNodePodMap, error) {
	// Initialize map to store results
	idcNodePodMap := make(IDCNodePodMap)

	// Collect all IDC zones
	idcMap := make(map[string]bool)
	nodeList, err := c.kubeClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("[YBS] Failed to list nodes: %v", err)
		return nil, err
	}

	// Log node list
	klog.V(3).Infof("[YBS] Collected %d nodes in cluster", len(nodeList.Items))
	for i, node := range nodeList.Items {
		if i < 5 { // 처음 5개 노드만 자세히 출력
			klog.V(4).Infof("[YBS] Node %d: Name=%s, Labels=%v, Status=%v",
				i, node.Name, node.Labels, node.Status.Conditions)
		}
	}

	// Extract IDC zone list by iterating through all nodes
	for _, node := range nodeList.Items {
		if idc, ok := node.Labels[TopologyZoneLabel]; ok {
			idcMap[idc] = true
		}
	}

	// Log collected IDC zones
	klog.V(3).Infof("[YBS] Collected idcMap: %#v", idcMap)

	// Get MinIO pod list
	tenantNamespace := tenant.Namespace
	tenantName := tenant.Name

	// Label selector to identify MinIO pods
	labelSelector := labels.SelectorFromSet(map[string]string{
		miniov2.TenantLabel: tenantName,
		AppLabel:            MinIOAppName,
	}).String()

	klog.V(3).Infof("[YBS] Looking for MinIO pods with label selector: %s", labelSelector)

	podList, err := c.kubeClientSet.CoreV1().Pods(tenantNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		klog.Errorf("[YBS] Failed to list MinIO pods for tenant %s/%s: %v", tenantNamespace, tenantName, err)
		return nil, err
	}

	// Log pod list
	klog.V(3).Infof("[YBS] Found %d MinIO pods for tenant %s/%s", len(podList.Items), tenantNamespace, tenantName)
	for i, pod := range podList.Items {
		podStatus := "Unknown"
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				if condition.Status == corev1.ConditionTrue {
					podStatus = "Ready"
				} else {
					podStatus = "NotReady"
				}
				break
			}
		}

		// Log detailed information for each pod
		klog.V(4).Infof("[YBS] Pod %d: Name=%s, Node=%s, Phase=%s, Status=%s",
			i, pod.Name, pod.Spec.NodeName, pod.Status.Phase, podStatus)
	}

	// Collect node and pod information for each IDC zone
	for idc := range idcMap {
		// Get nodes in this IDC zone
		zoneSelector := labels.SelectorFromSet(map[string]string{
			TopologyZoneLabel: idc,
		}).String()

		idcNodeList, err := c.kubeClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: zoneSelector,
		})
		if err != nil {
			klog.Errorf("[YBS] Failed to list nodes for idc %s: %v", idc, err)
			continue // Continue with other zones if there's an error in one
		}

		klog.V(3).Infof("[YBS] Found %d nodes in idc %s", len(idcNodeList.Items), idc)

		// Iterate through nodes in this IDC zone
		for _, node := range idcNodeList.Items {
			nodeName := node.Name

			// Check node status (Ready/NotReady)
			nodeStatus := "offline"
			for _, condition := range node.Status.Conditions {
				if condition.Type == corev1.NodeReady {
					if condition.Status == corev1.ConditionTrue {
						nodeStatus = "online"
					}
					break
				}
			}

			// Find MinIO pod running on this node
			var nodePodStatus NodePodStatus
			nodePodStatus.Node = nodeName
			nodePodStatus.NodeStatus = nodeStatus
			nodePodStatus.Pod = ""               // Default value
			nodePodStatus.PodStatus = "NotFound" // Default value

			for _, pod := range podList.Items {
				if pod.Spec.NodeName == nodeName {
					nodePodStatus.Pod = pod.Name
					nodePodStatus.PodStatus = string(pod.Status.Phase)

					// More accurate status check for Running pods
					if pod.Status.Phase == corev1.PodRunning {
						ready := true
						for _, condition := range pod.Status.Conditions {
							if condition.Type == corev1.PodReady && condition.Status != corev1.ConditionTrue {
								ready = false
								break
							}
						}

						if ready {
							nodePodStatus.PodStatus = "Ready"
						} else {
							nodePodStatus.PodStatus = "NotReady"
						}
					}
					break
				}
			}

			// Add to result map
			if _, exists := idcNodePodMap[idc]; !exists {
				idcNodePodMap[idc] = []NodePodStatus{}
			}
			idcNodePodMap[idc] = append(idcNodePodMap[idc], nodePodStatus)
		}
	}

	// Log final mapping information
	for idc, nodePodStatuses := range idcNodePodMap {
		klog.V(3).Infof("[YBS] IDC %s has %#v node-pod mappings\n", idc, nodePodStatuses)
	}

	return idcNodePodMap, nil
}

// ReconcileNodePodMap collects IDC-Node-Pod mapping information for the given tenant and updates the ConfigMap.
// This function should be called from the controller's Reconcile loop.
func (c *Controller) ReconcileNodePodMap(ctx context.Context, tenant *miniov2.Tenant) error {
	klog.V(2).Infof("[YBS] Reconciling IDC-Node-Pod map for tenant %s/%s", tenant.Namespace, tenant.Name)

	// Collect IDC-Node-Pod information regardless of tenant state
	idcNodePodMap, err := c.CollectIDCNodePodInfo(ctx, tenant)
	if err != nil {
		klog.Errorf("[YBS] Failed to collect IDC-Node-Pod information for tenant %s/%s: %v",
			tenant.Namespace, tenant.Name, err)
		return err
	}

	// If no information is collected, keep the existing ConfigMap and stop processing
	// Updating the ConfigMap with an empty map would store meaningless data,
	// so it's safer to keep the current state when no information is available
	if len(idcNodePodMap) == 0 {
		klog.Warningf("[YBS] No IDC-Node-Pod information collected for tenant %s/%s. Keeping existing ConfigMap if any.",
			tenant.Namespace, tenant.Name)
		return nil
	}

	// Update ConfigMap
	err = c.UpsertNodePodMapConfigMap(ctx, tenant.Namespace, idcNodePodMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to update IDC-Node-Pod ConfigMap for tenant %s/%s: %v",
			tenant.Namespace, tenant.Name, err)
		return err
	}

	klog.Infof("[YBS] Successfully reconciled IDC-Node-Pod map for tenant %s/%s", tenant.Namespace, tenant.Name)
	return nil
}

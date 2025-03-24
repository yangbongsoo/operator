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

type NodePodStatus struct {
	Node       string `json:"node" yaml:"node"`
	Pod        string `json:"pod" yaml:"pod"`
	NodeStatus string `json:"nodeStatus" yaml:"nodeStatus"`
	PodStatus  string `json:"podStatus" yaml:"podStatus"`
}

type IDCNodePodMap map[string][]NodePodStatus // idc1, idc2, idc3 -> NodePodStatus 목록

const (
	IDCNodePodMapName = "idc-node-pod-map"
	IDCNodePodMapKey  = "map.yaml"
	TopologyZoneLabel = "topology.kubernetes.io/zone"
	AppLabel          = "app"
	MinIOAppName      = "minio"
)

// GetNodePodMapConfigMap은 현재 idc-node-pod-map ConfigMap을 조회합니다.
// 존재하지 않는 경우 nil과 에러를 반환합니다.
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

// CreateNodePodMapConfigMap은 새로운 idc-node-pod-map ConfigMap을 생성합니다.
func (c *Controller) CreateNodePodMapConfigMap(ctx context.Context, namespace string, idcMap IDCNodePodMap) error {
	// IDCNodePodMap을 YAML로 마샬링
	yamlData, err := yaml.Marshal(idcMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to marshal IDCNodePodMap: %v", err)
		return err
	}

	// ConfigMap 객체 생성
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IDCNodePodMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			IDCNodePodMapKey: string(yamlData),
		},
	}

	// Kubernetes API를 통해 ConfigMap 생성
	_, err = c.kubeClientSet.CoreV1().ConfigMaps(namespace).Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("[YBS] Failed to create ConfigMap %s/%s: %v", namespace, IDCNodePodMapName, err)
		return err
	}

	klog.Infof("[YBS] Successfully created ConfigMap %s/%s", namespace, IDCNodePodMapName)
	return nil
}

// UpsertNodePodMapConfigMap은 기존 idc-node-pod-map ConfigMap을 업데이트합니다.
// ConfigMap이 존재하지 않는 경우 생성합니다.
func (c *Controller) UpsertNodePodMapConfigMap(ctx context.Context, namespace string, idcMap IDCNodePodMap) error {
	// 기존 ConfigMap 조회
	existingConfigMap, err := c.GetNodePodMapConfigMap(ctx, namespace)
	if err != nil {
		return err
	}

	// IDCNodePodMap을 YAML로 마샬링
	yamlData, err := yaml.Marshal(idcMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to marshal IDCNodePodMap: %v", err)
		return err
	}

	// ConfigMap이 존재하지 않는 경우 생성
	if existingConfigMap == nil {
		return c.CreateNodePodMapConfigMap(ctx, namespace, idcMap)
	}

	// 기존 ConfigMap 업데이트
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

// CollectIDCNodePodInfo는 각 IDC 영역별 노드와 MinIO 파드 정보를 수집합니다.
// topology.kubernetes.io/zone 레이블을 기반으로 IDC 영역을 식별하고,
// 각 노드에서 실행 중인 MinIO 파드 정보를 매핑합니다.
func (c *Controller) CollectIDCNodePodInfo(ctx context.Context, tenant *miniov2.Tenant) (IDCNodePodMap, error) {
	// 결과를 저장할 맵 초기화
	idcNodePodMap := make(IDCNodePodMap)

	// 모든 IDC 영역 목록 수집
	idcMap := make(map[string]bool)
	nodeList, err := c.kubeClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("[YBS] Failed to list nodes: %v", err)
		return nil, err
	}

	// nodeList 로그 출력
	klog.V(3).Infof("[YBS] Collected %d nodes in cluster", len(nodeList.Items))
	for i, node := range nodeList.Items {
		if i < 5 { // 처음 5개 노드만 자세히 출력
			klog.V(4).Infof("[YBS] Node %d: Name=%s, Labels=%v, Status=%v",
				i, node.Name, node.Labels, node.Status.Conditions)
		}
	}

	// 모든 노드를 순회하며 IDC 영역 목록 추출
	for _, node := range nodeList.Items {
		if idc, ok := node.Labels[TopologyZoneLabel]; ok {
			idcMap[idc] = true
		}
	}

	// 수집된 IDC 영역 로그 출력
	klog.V(3).Infof("[YBS] Collected idcMap: %#v", idcMap)

	// MinIO 파드 목록 조회
	tenantNamespace := tenant.Namespace
	tenantName := tenant.Name

	// MinIO 파드를 식별하기 위한 레이블 셀렉터
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

	// podList 로그 출력
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

		// 각 파드에 대한 자세한 정보 출력
		klog.V(4).Infof("[YBS] Pod %d: Name=%s, Node=%s, Phase=%s, Status=%s",
			i, pod.Name, pod.Spec.NodeName, pod.Status.Phase, podStatus)
	}

	// 각 IDC 영역별로 노드와 파드 정보 수집
	for idc := range idcMap {
		// IDC 영역에 해당하는 노드 목록 조회
		zoneSelector := labels.SelectorFromSet(map[string]string{
			TopologyZoneLabel: idc,
		}).String()

		idcNodeList, err := c.kubeClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: zoneSelector,
		})
		if err != nil {
			klog.Errorf("[YBS] Failed to list nodes for idc %s: %v", idc, err)
			continue // 한 영역에서 오류가 발생해도 다른 영역은 계속 처리
		}

		klog.V(3).Infof("[YBS] Found %d nodes in idc %s", len(idcNodeList.Items), idc)

		// 해당 IDC 영역의 노드들을 순회
		for _, node := range idcNodeList.Items {
			nodeName := node.Name

			// 노드 상태 확인 (Ready/NotReady)
			nodeStatus := "offline"
			for _, condition := range node.Status.Conditions {
				if condition.Type == corev1.NodeReady {
					if condition.Status == corev1.ConditionTrue {
						nodeStatus = "online"
					}
					break
				}
			}

			// 이 노드에서 실행 중인 MinIO 파드 찾기
			var nodePodStatus NodePodStatus
			nodePodStatus.Node = nodeName
			nodePodStatus.NodeStatus = nodeStatus
			nodePodStatus.Pod = ""               // 기본값
			nodePodStatus.PodStatus = "NotFound" // 기본값

			for _, pod := range podList.Items {
				if pod.Spec.NodeName == nodeName {
					nodePodStatus.Pod = pod.Name
					nodePodStatus.PodStatus = string(pod.Status.Phase)

					// 파드가 Running 상태인 경우 더 정확한 상태 확인
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

			// 결과 맵에 추가
			if _, exists := idcNodePodMap[idc]; !exists {
				idcNodePodMap[idc] = []NodePodStatus{}
			}
			idcNodePodMap[idc] = append(idcNodePodMap[idc], nodePodStatus)
		}
	}

	// 최종 수집된 매핑 정보 로그 출력
	for idc, nodePodStatuses := range idcNodePodMap {
		klog.V(3).Infof("[YBS] IDC %s has %#v node-pod mappings\n", idc, nodePodStatuses)
	}

	return idcNodePodMap, nil
}

// ReconcileNodePodMap은 주어진 테넌트의 IDC-Node-Pod 매핑 정보를 수집하고 ConfigMap을 업데이트합니다.
// 이 함수는 컨트롤러의 Reconcile 루프에서 호출되어야 합니다.
func (c *Controller) ReconcileNodePodMap(ctx context.Context, tenant *miniov2.Tenant) error {
	klog.V(2).Infof("[YBS] Reconciling IDC-Node-Pod map for tenant %s/%s", tenant.Namespace, tenant.Name)

	// 테넌트 상태에 관계없이 IDC-Node-Pod 정보를 수집합니다
	// IDC-Node-Pod 정보 수집
	idcNodePodMap, err := c.CollectIDCNodePodInfo(ctx, tenant)
	if err != nil {
		klog.Errorf("[YBS] Failed to collect IDC-Node-Pod information for tenant %s/%s: %v",
			tenant.Namespace, tenant.Name, err)
		return err
	}

	// 수집된 정보가 없으면 기존 ConfigMap을 유지하고 처리를 중단합니다.
	// 빈 맵으로 ConfigMap을 업데이트하면 의미 없는 데이터가 저장되므로,
	// 정보가 없는 경우에는 현재 상태를 유지하는 것이 더 안전합니다.
	if len(idcNodePodMap) == 0 {
		klog.Warningf("[YBS] No IDC-Node-Pod information collected for tenant %s/%s. Keeping existing ConfigMap if any.",
			tenant.Namespace, tenant.Name)
		return nil
	}

	// ConfigMap 업데이트
	err = c.UpsertNodePodMapConfigMap(ctx, tenant.Namespace, idcNodePodMap)
	if err != nil {
		klog.Errorf("[YBS] Failed to update IDC-Node-Pod ConfigMap for tenant %s/%s: %v",
			tenant.Namespace, tenant.Name, err)
		return err
	}

	klog.Infof("[YBS] Successfully reconciled IDC-Node-Pod map for tenant %s/%s", tenant.Namespace, tenant.Name)
	return nil
}

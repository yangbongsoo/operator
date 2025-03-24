package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"

	miniov2 "github.com/minio/operator/pkg/apis/minio.min.io/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetNodePodMapConfigMap(t *testing.T) {
	ctx := context.Background()
	namespace := "minio-tenant"

	testMap := IDCNodePodMap{
		"idc1": []NodePodStatus{
			{
				Node:       "node1",
				Pod:        "pod1",
				NodeStatus: "online",
				PodStatus:  "Ready",
			},
		},
	}
	yamlData, _ := yaml.Marshal(testMap)

	existingConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IDCNodePodMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			IDCNodePodMapKey: string(yamlData),
		},
	}

	fakeClient := fake.NewSimpleClientset(existingConfigMap)
	controller := &Controller{
		kubeClientSet: fakeClient,
	}

	configMap, err := controller.GetNodePodMapConfigMap(ctx, namespace)
	assert.NoError(t, err)
	assert.NotNil(t, configMap)
	assert.Equal(t, IDCNodePodMapName, configMap.Name)
	assert.Equal(t, string(yamlData), configMap.Data[IDCNodePodMapKey])

	configMap, err = controller.GetNodePodMapConfigMap(ctx, "non-existent-namespace")
	assert.NoError(t, err)
	assert.Nil(t, configMap)
}

func TestCreateNodePodMapConfigMap(t *testing.T) {
	ctx := context.Background()
	namespace := "minio-tenant"

	testMap := IDCNodePodMap{
		"idc1": []NodePodStatus{
			{
				Node:       "node1",
				Pod:        "pod1",
				NodeStatus: "online",
				PodStatus:  "Ready",
			},
		},
	}

	fakeClient := fake.NewSimpleClientset()

	controller := &Controller{
		kubeClientSet: fakeClient,
	}

	err := controller.CreateNodePodMapConfigMap(ctx, namespace, testMap)
	assert.NoError(t, err)

	configMap, err := fakeClient.CoreV1().ConfigMaps(namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, configMap)
	assert.Equal(t, IDCNodePodMapName, configMap.Name)

	var retrievedMap IDCNodePodMap
	err = yaml.Unmarshal([]byte(configMap.Data[IDCNodePodMapKey]), &retrievedMap)
	assert.NoError(t, err)
	assert.Equal(t, testMap, retrievedMap)
}

func TestUpsertNodePodMapConfigMap(t *testing.T) {
	ctx := context.Background()
	namespace := "minio-tenant"

	testCases := []struct {
		name        string
		existingMap IDCNodePodMap
		newMap      IDCNodePodMap
	}{
		{
			name:        "ConfigMap이 존재하지 않을 때 생성",
			existingMap: nil,
			newMap: IDCNodePodMap{
				"idc1": []NodePodStatus{
					{
						Node:       "node1",
						Pod:        "pod1",
						NodeStatus: "online",
						PodStatus:  "Ready",
					},
				},
			},
		},
		{
			name: "기존 ConfigMap 업데이트",
			existingMap: IDCNodePodMap{
				"idc1": []NodePodStatus{
					{
						Node:       "node1",
						Pod:        "pod1",
						NodeStatus: "online",
						PodStatus:  "Ready",
					},
				},
			},
			newMap: IDCNodePodMap{
				"idc1": []NodePodStatus{
					{
						Node:       "node1",
						Pod:        "pod1",
						NodeStatus: "online",
						PodStatus:  "Ready",
					},
				},
				"idc2": []NodePodStatus{
					{
						Node:       "node2",
						Pod:        "pod2",
						NodeStatus: "online",
						PodStatus:  "Ready",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var existingConfigMap *corev1.ConfigMap
			var fakeClient *fake.Clientset

			if tc.existingMap != nil {
				yamlData, _ := yaml.Marshal(tc.existingMap)
				existingConfigMap = &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      IDCNodePodMapName,
						Namespace: namespace,
					},
					Data: map[string]string{
						IDCNodePodMapKey: string(yamlData),
					},
				}
				fakeClient = fake.NewSimpleClientset(existingConfigMap)
			} else {
				fakeClient = fake.NewSimpleClientset()
			}

			controller := &Controller{
				kubeClientSet: fakeClient,
			}

			err := controller.UpsertNodePodMapConfigMap(ctx, namespace, tc.newMap)
			assert.NoError(t, err)

			configMap, err := fakeClient.CoreV1().ConfigMaps(namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, configMap)

			var retrievedMap IDCNodePodMap
			err = yaml.Unmarshal([]byte(configMap.Data[IDCNodePodMapKey]), &retrievedMap)
			assert.NoError(t, err)
			assert.Equal(t, tc.newMap, retrievedMap)
		})
	}
}

func TestCollectIDCNodePodInfo(t *testing.T) {
	ctx := context.Background()
	tenant := &miniov2.Tenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-tenant",
			Namespace: "minio-tenant",
		},
		Status: miniov2.TenantStatus{
			CurrentState: "Initialized",
			HealthStatus: miniov2.HealthStatusGreen,
		},
	}

	// 테스트 노드 생성
	nodes := []corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					TopologyZoneLabel: "idc1",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{
						Type:   corev1.NodeReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					TopologyZoneLabel: "idc1",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{
						Type:   corev1.NodeReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node3",
				Labels: map[string]string{
					TopologyZoneLabel: "idc2",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{
						Type:   corev1.NodeReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
	}

	// 테스트 파드 생성
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "minio-pod-1",
				Namespace: tenant.Namespace,
				Labels: map[string]string{
					miniov2.TenantLabel: tenant.Name,
					AppLabel:            MinIOAppName,
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "minio-pod-3",
				Namespace: tenant.Namespace,
				Labels: map[string]string{
					miniov2.TenantLabel: tenant.Name,
					AppLabel:            MinIOAppName,
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node3",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		},
	}

	// 가짜 Kubernetes 클라이언트 생성
	fakeClient := fake.NewSimpleClientset()

	// 노드와 파드 추가
	for _, node := range nodes {
		_, err := fakeClient.CoreV1().Nodes().Create(ctx, &node, metav1.CreateOptions{})
		assert.NoError(t, err)
	}

	for _, pod := range pods {
		_, err := fakeClient.CoreV1().Pods(tenant.Namespace).Create(ctx, &pod, metav1.CreateOptions{})
		assert.NoError(t, err)
	}

	// 테스트를 위한 컨트롤러 생성
	controller := &Controller{
		kubeClientSet: fakeClient,
	}

	// CollectIDCNodePodInfo 테스트
	idcNodePodMap, err := controller.CollectIDCNodePodInfo(ctx, tenant)
	assert.NoError(t, err)
	assert.NotNil(t, idcNodePodMap)

	// 결과 확인
	assert.Contains(t, idcNodePodMap, "idc1")
	assert.Contains(t, idcNodePodMap, "idc2")

	assert.Len(t, idcNodePodMap["idc1"], 2)
	assert.Len(t, idcNodePodMap["idc2"], 1)

	// idc1의 첫 번째 노드 검증 (node1)
	node1Found := false
	for _, nodePodStatus := range idcNodePodMap["idc1"] {
		if nodePodStatus.Node == "node1" {
			node1Found = true
			assert.Equal(t, "online", nodePodStatus.NodeStatus)
			assert.Equal(t, "minio-pod-1", nodePodStatus.Pod)
			assert.Equal(t, "Ready", nodePodStatus.PodStatus)
			break
		}
	}
	assert.True(t, node1Found, "node1 정보를 찾음")

	// idc1의 두 번째 노드 검증 (node2)
	node2Found := false
	for _, nodePodStatus := range idcNodePodMap["idc1"] {
		if nodePodStatus.Node == "node2" {
			node2Found = true
			assert.Equal(t, "offline", nodePodStatus.NodeStatus)
			assert.Equal(t, "", nodePodStatus.Pod)
			assert.Equal(t, "NotFound", nodePodStatus.PodStatus)
			break
		}
	}
	assert.True(t, node2Found, "node2 정보를 찾음")

	// zone2의 노드 검증 (node3)
	assert.Equal(t, "node3", idcNodePodMap["idc2"][0].Node)
	assert.Equal(t, "online", idcNodePodMap["idc2"][0].NodeStatus)
	assert.Equal(t, "minio-pod-3", idcNodePodMap["idc2"][0].Pod)
	assert.Equal(t, "NotReady", idcNodePodMap["idc2"][0].PodStatus)
}

func TestReconcileNodePodMap(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		tenantState   string
		healthStatus  miniov2.HealthStatus
		expectSuccess bool
	}{
		{
			name:          "정상 테넌트",
			tenantState:   "Initialized",
			healthStatus:  miniov2.HealthStatusGreen,
			expectSuccess: true,
		},
		{
			name:          "초기화되지 않은 테넌트",
			tenantState:   "NotInitialized",
			healthStatus:  miniov2.HealthStatusRed,
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tenant := &miniov2.Tenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-tenant",
					Namespace: "minio-tenant",
				},
				Status: miniov2.TenantStatus{
					CurrentState: tc.tenantState,
					HealthStatus: tc.healthStatus,
				},
			}

			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
					Labels: map[string]string{
						TopologyZoneLabel: "idc1",
					},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:   corev1.NodeReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
			}

			pod := corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "minio-pod-1",
					Namespace: tenant.Namespace,
					Labels: map[string]string{
						miniov2.TenantLabel: tenant.Name,
						AppLabel:            MinIOAppName,
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node1",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
			}

			// 가짜 Kubernetes 클라이언트 생성
			fakeClient := fake.NewSimpleClientset()

			// 노드와 파드 추가
			_, err := fakeClient.CoreV1().Nodes().Create(ctx, &node, metav1.CreateOptions{})
			assert.NoError(t, err)

			_, err = fakeClient.CoreV1().Pods(tenant.Namespace).Create(ctx, &pod, metav1.CreateOptions{})
			assert.NoError(t, err)

			// 테스트를 위한 컨트롤러 생성
			controller := &Controller{
				kubeClientSet: fakeClient,
			}

			// ReconcileNodePodMap 테스트
			err = controller.ReconcileNodePodMap(ctx, tenant)

			if tc.expectSuccess {
				assert.NoError(t, err)

				// ConfigMap이 생성되었는지 확인
				configMap, err := fakeClient.CoreV1().ConfigMaps(tenant.Namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotNil(t, configMap)

				// ConfigMap 데이터 확인
				var retrievedMap IDCNodePodMap
				err = yaml.Unmarshal([]byte(configMap.Data[IDCNodePodMapKey]), &retrievedMap)
				assert.NoError(t, err)
				assert.Contains(t, retrievedMap, "idc1")
			} else {
				// 초기화되지 않은 테넌트는 조기 반환되어 ConfigMap이 생성되지 않음
				assert.NoError(t, err) // 에러는 없지만 작업이 수행되지 않음

				// ConfigMap이 생성되지 않았는지 확인
				_, err := fakeClient.CoreV1().ConfigMaps(tenant.Namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
				assert.Error(t, err) // ConfigMap을 찾을 수 없어야 함
			}
		})
	}
}

func TestEmptyIDCNodePodMap(t *testing.T) {
	// 테스트 컨텍스트
	ctx := context.Background()
	tenant := &miniov2.Tenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-tenant",
			Namespace: "minio-tenant",
		},
		Status: miniov2.TenantStatus{
			CurrentState: "Initialized",
			HealthStatus: miniov2.HealthStatusGreen,
		},
	}

	// 가짜 Kubernetes 클라이언트 생성
	// 노드나 파드를 추가하지 않아 빈 맵이 반환됨
	fakeClient := fake.NewSimpleClientset()

	// 테스트를 위한 컨트롤러 생성
	controller := &Controller{
		kubeClientSet: fakeClient,
	}

	// 기존 ConfigMap 생성
	testMap := IDCNodePodMap{
		"idc1": []NodePodStatus{
			{
				Node:       "node1",
				Pod:        "pod1",
				NodeStatus: "online",
				PodStatus:  "Ready",
			},
		},
	}
	yamlData, _ := yaml.Marshal(testMap)
	existingConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IDCNodePodMapName,
			Namespace: tenant.Namespace,
		},
		Data: map[string]string{
			IDCNodePodMapKey: string(yamlData),
		},
	}
	_, err := fakeClient.CoreV1().ConfigMaps(tenant.Namespace).Create(ctx, existingConfigMap, metav1.CreateOptions{})
	assert.NoError(t, err)

	// ReconcileNodePodMap 테스트
	err = controller.ReconcileNodePodMap(ctx, tenant)
	assert.NoError(t, err)

	// 기존 ConfigMap이 그대로 유지되는지 확인
	configMap, err := fakeClient.CoreV1().ConfigMaps(tenant.Namespace).Get(ctx, IDCNodePodMapName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, configMap)
	assert.Equal(t, string(yamlData), configMap.Data[IDCNodePodMapKey])
}

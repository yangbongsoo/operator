package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type IDCTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              IDCTopologySpec `json:"spec"`
}

// +k8s:deepcopy-gen=true
type IDCTopologySpec struct {
	IDCs []IDC `json:"idcs"`
}

// +k8s:deepcopy-gen=true
type IDC struct {
	IDCName string `json:"idcName"`
	Nodes   []Node `json:"nodes"`
}

// +k8s:deepcopy-gen=true
type Node struct {
	Node       string `json:"node"`
	Pod        string `json:"pod"`
	NodeStatus string `json:"nodeStatus"`
	PodStatus  string `json:"podStatus"`
}

// This file is part of MinIO Operator
// Copyright (c) 2024 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/minio/operator/sidecar/pkg"

	"github.com/minio/cli"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/trie"
	"github.com/minio/pkg/words"
)

// Help template for Operator.
var operatorHelpTemplate = `NAME:
 {{.Name}} - {{.Usage}}

DESCRIPTION:
 {{.Description}}

USAGE:
 {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}COMMAND{{if .VisibleFlags}}{{end}} [ARGS...]

COMMANDS:
 {{range .VisibleCommands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
 {{end}}{{if .VisibleFlags}}
FLAGS:
 {{range .VisibleFlags}}{{.}}
 {{end}}{{end}}
VERSION:
 {{.Version}}
`

// NodeInfo for sidecar informer
type NodeInfo struct {
	IDC        string `json:"idc"`
	Node       string `json:"node"`
	Pod        string `json:"pod"`
	NodeStatus string `json:"nodeStatus"`
	PodStatus  string `json:"podStatus"`
}

// IDCTopology struct (Data to be saved as JSON file)
type IDCTopology map[string][]Node

// Node struct (Mapping with Node in IDCTopology CRD)
type Node struct {
	Node       string `json:"node"`
	NodeStatus string `json:"nodeStatus"`
	Pod        string `json:"pod"`
	PodStatus  string `json:"podStatus"`
}

func newApp(name string) *cli.App {
	// Collection of console commands currently supported are.
	var commands []cli.Command

	// Collection of console commands currently supported in a tree.
	commandsTree := trie.NewTrie()

	// registerCommand registers a cli command.
	registerCommand := func(command cli.Command) {
		commands = append(commands, command)
		commandsTree.Insert(command.Name)
	}

	// register commands
	for _, cmd := range appCmds {
		registerCommand(cmd)
	}

	findClosestCommands := func(command string) []string {
		var closestCommands []string
		closestCommands = append(closestCommands, commandsTree.PrefixMatch(command)...)

		sort.Strings(closestCommands)
		// Suggest other close commands - allow missed, wrongly added and
		// even transposed characters
		for _, value := range commandsTree.Walk(commandsTree.Root()) {
			if sort.SearchStrings(closestCommands, value) < len(closestCommands) {
				continue
			}
			// 2 is arbitrary and represents the max
			// allowed number of typed errors
			if words.DamerauLevenshteinDistance(command, value) < 2 {
				closestCommands = append(closestCommands, value)
			}
		}

		return closestCommands
	}

	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, h",
		Usage: "show help",
	}

	app := cli.NewApp()
	app.Name = name
	app.Version = pkg.Version + " - " + pkg.ShortCommitID
	app.Author = "MinIO, Inc."
	app.Usage = "MinIO Operator Sidecar"
	app.Description = `MinIO Operator automates the orchestration of MinIO Tenants on Kubernetes.`
	app.Copyright = "(c) 2024 MinIO, Inc."
	app.Compiled, _ = time.Parse(time.RFC3339, pkg.ReleaseTime)
	app.Commands = commands
	app.HideHelpCommand = true // Hide `help, h` command, we already have `minio --help`.
	app.CustomAppHelpTemplate = operatorHelpTemplate
	app.CommandNotFound = func(_ *cli.Context, command string) {
		console.Printf("'%s' is not a console sub-command. See 'console --help'.\n", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			console.Println()
			console.Println("Did you mean one of these?")
			for _, cmd := range closestCommands {
				console.Printf("\t'%s'\n", cmd)
			}
		}
		os.Exit(1)
	}

	return app
}

func main() {
	args := os.Args
	// Set the orchestrator app name.
	appName := filepath.Base(args[0])

	config, err := rest.InClusterConfig()
	if err != nil {
		console.Fatalf("[YBS] Failed to load config: %v\n", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		console.Fatalf("[YBS] Failed to create clientset: %v\n", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		console.Fatalf("[YBS] Failed to create dynamic client: %v\n", err)
	}
	operatorURL := "http://operator.minio-operator.svc.cluster.local:4221/report"
	go reportNodePodStatus(clientset, operatorURL)
	go watchIDCTopology(dynamicClient)

	// Run the app - exit on error.
	if err := newApp(appName).Run(args); err != nil {
		os.Exit(1)
	}
}

func watchIDCTopology(dynamicClient dynamic.Interface) {
	console.Println("[YBS] Setting up IDCTopology informer...")

	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	gvr := schema.GroupVersionResource{
		Group:    "topology.xai",
		Version:  "v1alpha1",
		Resource: "idctopologies",
	}

	console.Printf("[YBS] Configured to watch resource: %s\n", gvr.String())

	informer := informerFactory.ForResource(gvr).Informer()
	console.Println("[YBS] Created informer for IDCTopology")

	console.Println("[YBS] Registering event handlers...")
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				console.Printf("[YBS] Error: Added object is not Unstructured: %T\n", obj)
				return
			}
			console.Printf("[YBS] IDCTopology added: %s\n", u.GetName())
			processIDCTopologyFromUnstructured(u)
		},
		UpdateFunc: func(_, newObj interface{}) {
			u, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				console.Printf("[YBS] Error: Updated object is not Unstructured: %T\n", newObj)
				return
			}
			console.Printf("[YBS] IDCTopology updated: %s\n", u.GetName())
			processIDCTopologyFromUnstructured(u)
		},
		DeleteFunc: func(obj interface{}) {
			// DeleteFunc might receive a DeletedFinalStateUnknown instead of an Unstructured
			deleteObj, ok := obj.(cache.DeletedFinalStateUnknown)
			if ok {
				obj = deleteObj.Obj
			}

			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				console.Printf("[YBS] Error: Deleted object is not Unstructured: %T\n", obj)
				return
			}
			console.Printf("[YBS] IDCTopology deleted: %s\n", u.GetName())
			clearTopologyFile()
		},
	})

	console.Println("[YBS] Starting IDCTopology informer...")
	stopCh := make(chan struct{})
	informerFactory.Start(stopCh)

	console.Println("[YBS] Waiting for IDCTopology cache sync...")
	synced := informerFactory.WaitForCacheSync(stopCh)
	console.Printf("[YBS] IDCTopology cache sync completed: %v\n", synced)

	// Keep the goroutine alive
	console.Println("[YBS] IDCTopology informer is now running")
}

func processIDCTopologyFromUnstructured(u *unstructured.Unstructured) {
	console.Printf("[YBS] Processing IDCTopology: %s\n", u.GetName())

	idcTopologyData := make(IDCTopology)

	spec, found, err := unstructured.NestedMap(u.Object, "spec")
	if err != nil {
		console.Printf("[YBS] Error getting spec from IDCTopology: %v\n", err)
		return
	}
	if !found {
		console.Printf("[YBS] Spec not found in IDCTopology\n")
		return
	}
	console.Printf("[YBS] Found spec in IDCTopology\n")

	idcs, found, err := unstructured.NestedSlice(spec, "idcs")
	if err != nil {
		console.Printf("[YBS] Error getting IDCs from spec: %v\n", err)
		return
	}
	if !found {
		console.Printf("[YBS] IDCs field not found in spec\n")
		return
	}
	console.Printf("[YBS] Found %d IDCs in spec\n", len(idcs))

	for i, idcObj := range idcs {
		idc, ok := idcObj.(map[string]interface{})
		if !ok {
			console.Printf("[YBS] IDC at index %d is not a map\n", i)
			continue
		}

		idcName, found, err := unstructured.NestedString(idc, "idcName")
		if err != nil {
			console.Printf("[YBS] Error getting idcName: %v\n", err)
			continue
		}
		if !found {
			console.Printf("[YBS] idcName not found for IDC at index %d\n", i)
			continue
		}

		nodesObj, found, err := unstructured.NestedSlice(idc, "nodes")
		if err != nil {
			console.Printf("[YBS] Error getting nodes for IDC %s: %v\n", idcName, err)
			continue
		}
		if !found {
			console.Printf("[YBS] nodes not found for IDC %s\n", idcName)
			continue
		}
		console.Printf("[YBS] Found %d nodes for IDC %s\n", len(nodesObj), idcName)

		var nodes []Node
		for j, nodeObj := range nodesObj {
			node, ok := nodeObj.(map[string]interface{})
			if !ok {
				console.Printf("[YBS] Node at index %d for IDC %s is not a map\n", j, idcName)
				continue
			}

			var nodeName, nodeStatus, pod, podStatus string

			if val, found, _ := unstructured.NestedString(node, "node"); found {
				nodeName = val
			} else {
				console.Printf("[YBS] node name not found for node at index %d in IDC %s\n", j, idcName)
			}

			if val, found, _ := unstructured.NestedString(node, "nodeStatus"); found {
				nodeStatus = val
			}

			if val, found, _ := unstructured.NestedString(node, "pod"); found {
				pod = val
			}

			if val, found, _ := unstructured.NestedString(node, "podStatus"); found {
				podStatus = val
			}

			nodes = append(nodes, Node{
				Node:       nodeName,
				NodeStatus: nodeStatus,
				Pod:        pod,
				PodStatus:  podStatus,
			})
		}

		idcTopologyData[idcName] = nodes
	}

	if len(idcTopologyData) == 0 {
		console.Printf("[YBS] No valid IDC data found in IDCTopology %s\n", u.GetName())
		return
	}

	console.Println("[YBS] IDCTopology data to be shared:")
	for idcName, nodes := range idcTopologyData {
		console.Printf("[YBS] IDC: %s\n", idcName)
		for i, node := range nodes {
			console.Printf("[YBS]   Node[%d]: {Node: %s, NodeStatus: %s, Pod: %s, PodStatus: %s}\n",
				i, node.Node, node.NodeStatus, node.Pod, node.PodStatus)
		}
	}

	// Wrap shareDataWithMinIO in a defer-recover to catch any panics
	defer func() {
		if r := recover(); r != nil {
			console.Printf("[YBS] Panic during IDCTopology processing: %v\n", r)
		}
	}()

	// Share data with MinIO
	shareDataWithMinIO(idcTopologyData)
	console.Printf("[YBS] Completed processing IDCTopology: %s\n", u.GetName())
}

func shareDataWithMinIO(idcTopologyData IDCTopology) {
	console.Printf("[YBS] Starting to share data with MinIO, data size: %d IDCs\n", len(idcTopologyData))

	data, err := json.MarshalIndent(idcTopologyData, "", "  ")
	if err != nil {
		console.Printf("[YBS] Failed to marshal IDCTopology data: %v\n", err)
		return
	}
	console.Printf("[YBS] Marshaled IDCTopology data, size: %d bytes\n", len(data))

	dir := "/tmp/minio/topology"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		console.Printf("[YBS] Directory %s does not exist, creating\n", dir)
		err = os.MkdirAll(dir, 0o755)
		if err != nil {
			console.Printf("[YBS] Failed to create directory %s: %v\n", dir, err)
			return
		}
		console.Printf("[YBS] Created directory: %s\n", dir)
	} else if err != nil {
		console.Printf("[YBS] Error checking directory %s: %v\n", dir, err)
		return
	} else {
		console.Printf("[YBS] Directory %s already exists\n", dir)
	}

	tempFile := "/tmp/minio/topology/idc-topology.json.tmp"
	finalFile := "/tmp/minio/topology/idc-topology.json"

	console.Printf("[YBS] Writing to temp file: %s\n", tempFile)
	err = os.WriteFile(tempFile, data, 0o644)
	if err != nil {
		console.Printf("[YBS] Failed to write IDCTopology data to temp file: %v\n", err)
		return
	}
	console.Println("[YBS] Successfully wrote to temp file")

	console.Printf("[YBS] Renaming temp file to: %s\n", finalFile)
	err = os.Rename(tempFile, finalFile)
	if err != nil {
		console.Printf("[YBS] Failed to rename temp file: %v\n", err)
		return
	}

	console.Printf("[YBS] Successfully updated IDCTopology file: %s\n", finalFile)
}

func clearTopologyFile() {
	finalFile := "/tmp/minio/topology/idc-topology.json"
	err := os.Remove(finalFile)
	if err != nil && !os.IsNotExist(err) {
		console.Printf("[YBS] Failed to remove IDCTopology file: %v\n", err)
		return
	}
	console.Printf("[YBS] Successfully cleared IDCTopology file: %s\n", finalFile)
}

func reportNodePodStatus(clientset *kubernetes.Clientset, operatorURL string) {
	namespace := "minio-tenant"
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		console.Println("[YBS] Failed to get POD_NAME")
		return
	}
	console.Printf("[YBS] podName is %s\n", podName)
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	nodeInformer := factory.Core().V1().Nodes().Informer()
	podInformer := factory.Core().V1().Pods().Informer()

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, newObj interface{}) {
			node := newObj.(*corev1.Node)
			pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
			if err != nil {
				console.Printf("[YBS] Failed to get pod %s: %v\n", podName, err)
				return
			}
			if pod.Spec.NodeName != node.Name {
				return
			}
			idc := node.Labels["topology.kubernetes.io/zone"]
			if idc == "" {
				idc = "unknown-idc"
			}
			nodeInfo := NodeInfo{
				IDC:        idc,
				Node:       node.Name,
				NodeStatus: getNodeStatus(node),
				Pod:        pod.Name,
				PodStatus:  string(pod.Status.Phase),
			}
			sendToOperator(nodeInfo, operatorURL)
		},
	})

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, newObj interface{}) {
			pod := newObj.(*corev1.Pod)
			if pod.Name != podName {
				return
			}
			node, err := clientset.CoreV1().Nodes().Get(context.TODO(), pod.Spec.NodeName, metav1.GetOptions{})
			if err != nil {
				console.Printf("[YBS] Failed to get node %s: %v\n", pod.Spec.NodeName, err)
				return
			}
			idc := node.Labels["topology.kubernetes.io/zone"]
			if idc == "" {
				idc = "unknown-idc"
			}
			nodeInfo := NodeInfo{
				IDC:        idc,
				Node:       pod.Spec.NodeName,
				NodeStatus: getNodeStatus(node),
				Pod:        pod.Name,
				PodStatus:  string(pod.Status.Phase),
			}
			sendToOperator(nodeInfo, operatorURL)
		},
	})

	console.Println("[YBS] Starting node and pod informers...")
	go nodeInformer.Run(make(chan struct{}))
	go podInformer.Run(make(chan struct{}))
	console.Println("[YBS] Node and pod informers are now running")
	select {} // infinite wait
}

func sendToOperator(nodeInfo NodeInfo, operatorURL string) {
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		console.Printf("[YBS] Failed to marshal nodeInfo: %v\n", err)
		return
	}
	resp, err := http.Post(operatorURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		console.Printf("[YBS] Failed to send nodeInfo to Operator: %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		console.Printf("[YBS] Operator responded with nodeInfo: %s\n", resp.Status)
	}
}

func getNodeStatus(node *corev1.Node) string {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			if cond.Status == corev1.ConditionTrue {
				return "Ready"
			}
			return "NotReady"
		}
	}
	return "NotReady"
}

// Copyright (C) 2023, MinIO, Inc.
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

package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/minio/cli"
	"github.com/minio/operator/pkg"
	topologyv1alpha1 "github.com/minio/operator/pkg/apis/topology.xai/v1alpha1"
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

type NodeInfo struct {
	IDC        string `json:"idc"`
	Node       string `json:"node"`
	Pod        string `json:"pod"`
	NodeStatus string `json:"nodeStatus"`
	PodStatus  string `json:"podStatus"`
}

func newApp(name string) *cli.App {
	// Collection of console commands currently supported are.
	var commands []cli.Command

	// Collection of console commands currently supported in a trie tree.
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
	app.Usage = "MinIO Operator"
	app.Description = `MinIO Operator automates the orchestration of MinIO Tenants on Kubernetes.`
	app.Copyright = "(c) 2023 MinIO, Inc."
	app.Compiled, _ = time.Parse(time.RFC3339, pkg.ReleaseTime)
	app.Commands = commands
	app.HideHelpCommand = true // Hide `help, h` command, we already have `minio --help`.
	app.CustomAppHelpTemplate = operatorHelpTemplate
	app.CommandNotFound = func(_ *cli.Context, command string) {
		console.Printf("‘%s’ is not a console sub-command. See ‘console --help’.\n", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			console.Println()
			console.Println("Did you mean one of these?")
			for _, cmd := range closestCommands {
				console.Printf("\t‘%s’\n", cmd)
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
		log.Fatalf("[YBS] Failed to load config: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("[YBS] Failed to create dynamic client: %v", err)
	}

	http.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		reportHandler(w, r, dynamicClient)
	})
	go func() {
		log.Fatal(http.ListenAndServe(":4221", nil))
	}()
	// Run the app - exit on error.
	if err := newApp(appName).Run(args); err != nil {
		os.Exit(1)
	}
}

func reportHandler(w http.ResponseWriter, r *http.Request, dynamicClient dynamic.Interface) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		console.Printf("[YBS] Failed to read request body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var nodeInfo NodeInfo
	if err := json.Unmarshal(body, &nodeInfo); err != nil {
		console.Printf("[YBS] Failed to unmarshal nodeInfo: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	console.Printf("[YBS] Received nodeInfo: Node=%s, NodeStatus=%s, Pod=%s, PodStatus=%s\n",
		nodeInfo.Node, nodeInfo.NodeStatus, nodeInfo.Pod, nodeInfo.PodStatus)

	updateOrCreateIDCTopology(dynamicClient, nodeInfo)

	w.WriteHeader(http.StatusOK)
}

func updateOrCreateIDCTopology(dynamicClient dynamic.Interface, nodeInfo NodeInfo) {
	idcTopologyRes := schema.GroupVersionResource{
		Group:    "topology.xai",
		Version:  "v1alpha1",
		Resource: "idctopologies",
	}

	idcTopology, err := dynamicClient.Resource(idcTopologyRes).Namespace("default").Get(context.TODO(), "minio-topology", metav1.GetOptions{})
	if err != nil {
		createIDCTopology(dynamicClient, nodeInfo)
		return
	}

	newIDCTopology := &topologyv1alpha1.IDCTopology{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(idcTopology.UnstructuredContent(), newIDCTopology); err != nil {
		log.Printf("[YBS] Failed to convert from unstructured: %v", err)
		return
	}

	updateIDCTopologySpec(&newIDCTopology.Spec, nodeInfo)

	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newIDCTopology)
	if err != nil {
		log.Printf("[YBS] Failed to convert to unstructured: %v", err)
		return
	}
	_, err = dynamicClient.Resource(idcTopologyRes).Namespace("default").Update(context.TODO(), &unstructured.Unstructured{Object: unstructuredObj}, metav1.UpdateOptions{})
	if err != nil {
		log.Printf("[YBS] Failed to update IDCTopology: %v", err)
	} else {
		log.Println("[YBS] Updated IDCTopology")
	}
}

func createIDCTopology(dynamicClient dynamic.Interface, nodeInfo NodeInfo) {
	newIDCTopology := &topologyv1alpha1.IDCTopology{
		TypeMeta: metav1.TypeMeta{
			Kind:       "IDCTopology",
			APIVersion: "topology.xai/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-topology",
			Namespace: "default",
		},
		Spec: topologyv1alpha1.IDCTopologySpec{},
	}
	updateIDCTopologySpec(&newIDCTopology.Spec, nodeInfo)
	idcTopologyRes := schema.GroupVersionResource{
		Group:    "topology.xai",
		Version:  "v1alpha1",
		Resource: "idctopologies",
	}
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newIDCTopology)
	if err != nil {
		log.Printf("[YBS] Failed to convert to unstructured: %v", err)
		return
	}
	_, err = dynamicClient.Resource(idcTopologyRes).Namespace("default").Create(context.TODO(), &unstructured.Unstructured{Object: unstructuredObj}, metav1.CreateOptions{})
	if err != nil {
		log.Printf("[YBS] Failed to create IDCTopology: %v", err)
	} else {
		log.Println("[YBS] Created IDCTopology")
	}
}

func updateIDCTopologySpec(spec *topologyv1alpha1.IDCTopologySpec, nodeInfo NodeInfo) {
	idcName := nodeInfo.IDC
	node := topologyv1alpha1.Node{
		Node:       nodeInfo.Node,
		Pod:        nodeInfo.Pod,
		NodeStatus: nodeInfo.NodeStatus,
		PodStatus:  nodeInfo.PodStatus,
	}

	found := false
	for i, idc := range spec.IDCs {
		if idc.IDCName == idcName {
			nodeUpdated := false
			for j, existingNode := range idc.Nodes {
				if existingNode.Pod == nodeInfo.Pod {
					spec.IDCs[i].Nodes[j] = node
					nodeUpdated = true
					break
				}
			}
			if !nodeUpdated {
				spec.IDCs[i].Nodes = append(spec.IDCs[i].Nodes, node)
			}
			found = true
			break
		}
	}

	if !found {
		spec.IDCs = append(spec.IDCs, topologyv1alpha1.IDC{
			IDCName: idcName,
			Nodes:   []topologyv1alpha1.Node{node},
		})
	}
}

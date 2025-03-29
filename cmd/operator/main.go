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
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("[YBS] Failed to create clientset: %v", err)
	}
	createIDCTopology(clientset)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("[YBS] updateIDCTopology")
			updateIDCTopology(clientset)
		}
	}()
	// Run the app - exit on error.
	if err := newApp(appName).Run(args); err != nil {
		os.Exit(1)
	}
}

func createIDCTopology(clientset *kubernetes.Clientset) {
	newIDCTopology := &topologyv1alpha1.IDCTopology{
		TypeMeta: metav1.TypeMeta{
			Kind:       "IDCTopology",
			APIVersion: "topology.xai/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-topology",
			Namespace: "default",
		},
	}

	newIDCTopology.Spec = initialIDCTopologySpec(clientset)
	_, err := clientset.RESTClient().
		Post().
		AbsPath("/apis/topology.xai/v1alpha1/namespaces/default/idctopologies").
		Body(newIDCTopology).
		Do(context.TODO()).
		Get()
	if err != nil {
		log.Printf("[YBS] Failed to create IDCTopology: %v", err)
	} else {
		log.Println("[YBS] Created IDCTopology")
	}
}

func updateIDCTopology(clientset *kubernetes.Clientset) {
	idcTopology, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/topology.xai/v1alpha1/namespaces/default/idctopologies/minio-topology").
		Do(context.TODO()).
		Get()

	if err != nil {
		log.Printf("[YBS] Failed to get IDCTopology for update: %v", err)
		return
	}

	newIDCTopology := idcTopology.(*topologyv1alpha1.IDCTopology)
	newIDCTopology.Spec = initialIDCTopologySpec(clientset)
	_, err = clientset.RESTClient().
		Put().
		AbsPath("/apis/topology.xai/v1alpha1/namespaces/default/idctopologies/minio-topology").
		Body(newIDCTopology).
		Do(context.TODO()).
		Get()
	if err != nil {
		log.Printf("[YBS] Failed to update IDCTopology: %v", err)
	} else {
		log.Println("[YBS] Updated IDCTopology")
	}
}

func initialIDCTopologySpec(clientset *kubernetes.Clientset) topologyv1alpha1.IDCTopologySpec {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Printf("[YBS] Failed to list nodes: %v", err)
		return topologyv1alpha1.IDCTopologySpec{}
	}

	idcNodeMap := make(map[string][]string)
	for _, node := range nodes.Items {
		idc := node.Labels["topology.kubernetes.io/zone"]
		if idc == "" {
			continue
		}
		idcNodeMap[idc] = append(idcNodeMap[idc], node.Name)
	}

	idcTopologySpec := topologyv1alpha1.IDCTopologySpec{}
	for idc, nodeNames := range idcNodeMap {
		idcEntry := topologyv1alpha1.IDC{IDCName: idc}
		for _, nodeName := range nodeNames {
			nodeInfo, err := clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
			if err != nil {
				log.Printf("[YBS] Failed to get nodeInfo: %v", err)
				continue
			}
			podInfo := findPodForNode(clientset, nodeName)
			if podInfo.Name == "" {
				log.Printf("[YBS] Failed to get podInfo: %v", err)
				continue
			}
			idcEntry.Nodes = append(idcEntry.Nodes, topologyv1alpha1.Node{
				Node:       nodeName,
				Pod:        podInfo.Name,
				NodeStatus: getNodeStatus(nodeInfo),
				PodStatus:  string(podInfo.Status.Phase),
			})
		}
		if len(idcEntry.Nodes) > 0 {
			idcTopologySpec.IDCs = append(idcTopologySpec.IDCs, idcEntry)
		}
	}
	return idcTopologySpec
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

func findPodForNode(clientset *kubernetes.Clientset, nodeName string) *corev1.Pod {
	pods, err := clientset.CoreV1().Pods("minio-tenant").List(context.TODO(), metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return &corev1.Pod{}
	}
	for _, pod := range pods.Items {
		if pod.Labels["app"] == "myminio" {
			return &pod
		}
	}
	return &corev1.Pod{}
}

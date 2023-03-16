package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	hostlist "github.com/abrekhov/hostlist"
	tfe "github.com/hashicorp/go-tfe"
)

func main() {
	config := &tfe.Config{
		Token: os.Getenv("TFE_TOKEN"),
	}

	client, err := tfe.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	workspace, err := client.Workspaces.ReadByID(ctx, os.Getenv("TFE_WORKSPACE"))
	if err != nil {
		log.Fatal(err)
	}

	var_list, _ := client.Variables.List(ctx, workspace.ID, &tfe.VariableListOptions{})
	var tfe_pool *tfe.Variable
	for _, s := range var_list.Items {
		if s.Key == "pool" {
			tfe_pool = s
		}
	}

	variable, err := client.Variables.Read(ctx, workspace.ID, tfe_pool.ID)
	if err != nil {
		log.Fatal(err)
	}

	var pool []string
	err = json.Unmarshal([]byte(variable.Value), &pool)
	if err != nil {
		log.Fatal(err)
	}
	var pool_set = make(map[string]bool)
	for _, s := range pool {
		pool_set[s] = true
	}

	// Translate hostlist to list of nodes
	nodes := hostlist.ExpandNodeList(os.Args[2])
	if os.Args[1] == "resume" {
		for _, s := range nodes {
			pool_set[s] = true
		}
	} else if os.Args[1] == "suspend" {
		for _, s := range nodes {
			delete(pool_set, s)
		}
	}

	keys := make([]string, len(pool_set))
	i := 0
	for k := range pool_set {
		keys[i] = k
		i++
	}

	pool_json, err := json.Marshal(keys)
	value := string(pool_json)
	tfe_pool2, err := client.Variables.Update(ctx, workspace.ID, tfe_pool.ID, tfe.VariableUpdateOptions{Value: &value})

	if tfe_pool.Value != tfe_pool2.Value {
		log.Println("Updating pool ", tfe_pool.Value, "->", tfe_pool2.Value)
	} else {
		log.Println("no change")
	}

	if err != nil {
		log.Fatal(err)
	}
}

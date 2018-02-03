package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	"google.golang.org/grpc"
)

type Root struct {
	Me []Person `json:"me"`
}

type Person struct {
	uid  string `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
	Age  int    `json:"age,omitempty"`
}

func throw(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func newClient() *client.Dgraph {
	// client uses gprc connections to connect to dgraph database
	d, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure()) // This starts a single dgraph connection
	throw(err)
	return client.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func main() {
	d := newClient()

	// Alter
	op := &api.Operation{}

	op.Schema = `
	name: string @index(term) .
	age: int .`

	ctx := context.Background()
	err := d.Alter(ctx, op)
	throw(err)

	// Mutation
	p1 := Person{
		Name: "Dhriti",
		Age:  24,
	}

	m1 := &api.Mutation{
		CommitNow: true,
	}

	pb, err := json.Marshal(p1)
	throw(err)

	m1.SetJson = pb
	assigned, err := d.NewTxn().Mutate(ctx, m1)
	throw(err)

	// Query
	variables := map[string]string{
		"$id": assigned.Uids["blank-0"],
	}

	q1 := `query Me($id: string) {
		me(func: uid($id)) {
			uid
			name
			age
		}
	}`

	resp, err := d.NewTxn().QueryWithVars(ctx, q1, variables)
	throw(err)

	var r Root
	err = json.Unmarshal(resp.Json, &r)
	throw(err)

	fmt.Println(r.Me)
}

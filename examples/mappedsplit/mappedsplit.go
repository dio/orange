// mappedsplit demonstrates producing and consuming Cherry mapped-split
// snapshots through Orange's mapped-split SnapshotService API.
package main

import (
	"fmt"
	"os"
)

const (
	defaultScopeKind = "workspace"
	defaultScopeID   = "prod"
	defaultScope     = "prod"
	defaultSlug      = "slug:default"

	defaultServerURL = "http://127.0.0.1:8090"
	defaultPrincipal = "slug:alice"
	defaultModel     = "gpt-4o-mini"
	defaultMCPPath   = "profile-dev-tools"
	defaultLane      = "lane-a"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "server":
		err = runServer(os.Args[2:])
	case "client":
		err = runClient(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: go run ./examples/mappedsplit <server|client> [flags]")
}

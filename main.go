package main

import (
	"context"

	"github.com/dio/orange/server"
)

func main() {
	_ = server.Run(context.Background())
}

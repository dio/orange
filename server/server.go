package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/dio/orange/vtprotocodec"
)

func Run(ctx context.Context) error {
	_ = connect.WithCodec(vtprotocodec.Codec{})

	return nil
}

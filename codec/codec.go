// Package vtprotocodec provides a Connect codec that uses vtproto's
// MarshalVT/UnmarshalVT fast-path when available, falling back to the
// standard google.golang.org/protobuf encoding.
package vtprotocodec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

type vtMarshaler interface{ MarshalVT() ([]byte, error) }
type vtUnmarshaler interface{ UnmarshalVT([]byte) error }

// Codec implements connect.Codec with the wire name "proto".
type Codec struct{}

func (Codec) Name() string { return "proto" }

func (Codec) Marshal(msg any) ([]byte, error) {
	if m, ok := msg.(vtMarshaler); ok {
		return m.MarshalVT()
	}
	pm, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("expected proto.Message, got %T", msg)
	}
	return proto.Marshal(pm)
}

func (Codec) Unmarshal(data []byte, msg any) error {
	if m, ok := msg.(vtUnmarshaler); ok {
		return m.UnmarshalVT(data)
	}
	pm, ok := msg.(proto.Message)
	if !ok {
		return fmt.Errorf("expected proto.Message, got %T", msg)
	}
	return proto.Unmarshal(data, pm)
}

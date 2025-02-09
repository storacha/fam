package remote

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Remote interface {
	// Address is the network address of the remote.
	Address(ctx context.Context) (peer.AddrInfo, error)
	// Push local state to the remote.
	Push(ctx context.Context) error
	// Pull remote state from the remote.
	Pull(ctx context.Context) error
}

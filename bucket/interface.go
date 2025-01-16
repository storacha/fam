package bucket

import (
	"context"
	"iter"

	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
)

var ErrNotFound = pail.ErrNotFound

type EntriesOption = pail.EntriesOption
type Entry[T any] struct {
	Key   string
	Value T
}

var (
	WithKeyPrefix             = pail.WithKeyPrefix
	WithKeyGreaterThan        = pail.WithKeyGreaterThan
	WithKeyGreaterThanOrEqual = pail.WithKeyGreaterThanOrEqual
	WithKeyLessThan           = pail.WithKeyLessThan
	WithKeyLessThanOrEqual    = pail.WithKeyLessThanOrEqual
)

type Blockstore interface {
	block.Fetcher
	Put(ctx context.Context, block block.Block) error
	Del(ctx context.Context, link ipld.Link) error
}

type Bucket[T any] interface {
	// Root returns the current root CID of the bucket.
	Root(ctx context.Context) (ipld.Link, error)
	Get(ctx context.Context, key string) (T, error)
	Put(ctx context.Context, key string, value T) error
	Del(ctx context.Context, key string) error
	Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error]
}

// Clock is a merkle clock.
type Clock interface {
	Head(ctx context.Context) ([]ipld.Link, error)
	Advance(ctx context.Context, event block.Block) ([]ipld.Link, error)
}

// ClockBucket is a bucket backed by a merkle clock.
type ClockBucket[T any] interface {
	Clock
	Bucket[T]
}

// Networker allows for syncing state with remote servers.
type Networker interface {
	// Remotes retrieves the list of configured remotes.
	Remotes(ctx context.Context) (Bucket[Remote], error)
}

type Remote interface {
	// Address is the network address of the remote.
	Address() peer.AddrInfo
	// Push local state to the remote.
	Push(ctx context.Context) error
	// Pull remote state from the remote.
	Pull(ctx context.Context) error
}

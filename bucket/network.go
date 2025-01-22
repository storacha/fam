package bucket

import (
	"context"
	"iter"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
)

type ClockDsBucket[T any] struct {
	bucket  Bucket[T]
	remotes Bucket[Remote]
}

func (cb *ClockDsBucket[T]) Remotes(ctx context.Context) (Bucket[Remote], error) {
	return cb.remotes, nil
}

func (cb *ClockDsBucket[T]) Root(ctx context.Context) (ipld.Link, error) {
	return cb.bucket.Root(ctx)
}

func (cb *ClockDsBucket[T]) Get(ctx context.Context, key string) (T, error) {
	return cb.bucket.Get(ctx, key)
}

func (cb *ClockDsBucket[T]) Put(ctx context.Context, key string, value T) error {
	return cb.bucket.Put(ctx, key, value)
}

func (cb *ClockDsBucket[T]) Del(ctx context.Context, key string) error {
	return cb.bucket.Del(ctx, key)
}

func (cb *ClockDsBucket[T]) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error] {
	return cb.bucket.Entries(ctx, opts...)
}

// NewClockDsBucket creates a new [ClockBucket[T]] backed by a [datastore.Datastore].
func NewClockDsBucket[T any](bucket ClockBucket[T], blocks Blockstore, dstore datastore.Datastore) (*ClockDsBucket[T], error) {
	remotes, err := NewRemoteDsBucket(bucket, blocks, namespace.Wrap(dstore, datastore.NewKey("remotes/")))
	if err != nil {
		return nil, err
	}
	return &ClockDsBucket[T]{bucket, remotes}, nil
}

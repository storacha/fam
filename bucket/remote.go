package bucket

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/fam/bucket"
)

type ClockDsRemoteBucket struct {
	clock  Clock
	bucket Bucket[ipld.Link]
	values datastore.Datastore
}

func (rb *ClockDsRemoteBucket) Root(ctx context.Context) (ipld.Link, error) {
	return rb.bucket.Root(ctx)
}

func (rb *ClockDsRemoteBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[Remote], error] {
	return func(yield func(Entry[Remote], error) bool) {
		for entry, err := range rb.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[Remote]{}, err)
				return
			}
			keyBytes, err := rb.values.Get(ctx, datastore.NewKey(entry.Value.String()))
			if err != nil {
				yield(Entry[Remote]{}, err)
				return
			}
			s, err := decodeKey(keyBytes)
			if err != nil {
				yield(Entry[Remote]{}, err)
				return
			}
			if !yield(Entry[Remote]{entry.Key, s}, err) {
				return
			}
		}
	}
}

func (rb *ClockDsRemoteBucket) Get(ctx context.Context, key string) (Remote, error) {
	link, err := rb.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting key link: %w", err)
	}

	keyBytes, err := rb.values.Get(ctx, datastore.NewKey(link.String()))
	if err != nil {
		return nil, fmt.Errorf("getting key: %w", err)
	}

	return decodeKey(keyBytes)
}

func (rb *ClockDsRemoteBucket) Put(ctx context.Context, key string, remote Remote) error {
	keyBytes := signer.Encode()

	c, err := cid.Prefix{
		Version:  1,
		Codec:    signer.Code(),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(keyBytes)
	if err != nil {
		return fmt.Errorf("hashing key bytes: %w", err)
	}

	err = rb.values.Put(ctx, datastore.NewKey(c.String()), keyBytes)
	if err != nil {
		return fmt.Errorf("putting key: %w", err)
	}

	return rb.bucket.Put(ctx, key, cidlink.Link{Cid: c})
}

func (rb *ClockDsRemoteBucket) Del(ctx context.Context, key string) error {
	return rb.bucket.Del(ctx, key)
}

func (rb *ClockDsRemoteBucket) Push(ctx context.Context) error {
	return errors.New("not implemented")
}

func (rb *ClockDsRemoteBucket) Pull(ctx context.Context) error {
	return errors.New("not implemented")
}

func NewClockDsRemoteBucket(clock Clock, blocks bucket.Blockstore, dstore datastore.Datastore) (*ClockDsRemoteBucket, error) {
	bucket, err := NewDsBucket(blocks, namespace.Wrap(dstore, datastore.NewKey("shards/")))
	if err != nil {
		return nil, err
	}

	return &ClockDsRemoteBucket{
		clock,
		bucket,
		namespace.Wrap(dstore, datastore.NewKey("values/")),
	}, nil
}

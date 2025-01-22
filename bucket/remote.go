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
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/fam/bucket"
)

type RemoteDsBucket struct {
	clock  Clock
	bucket Bucket[ipld.Link]
	values datastore.Datastore
}

func (rb *RemoteDsBucket) Root(ctx context.Context) (ipld.Link, error) {
	return rb.bucket.Root(ctx)
}

func (rb *RemoteDsBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[Remote], error] {
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

func (rb *RemoteDsBucket) Get(ctx context.Context, key string) (Remote, error) {
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

func (rb *RemoteDsBucket) Put(ctx context.Context, key string, remote Remote) error {
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

func (rb *RemoteDsBucket) Del(ctx context.Context, key string) error {
	return rb.bucket.Del(ctx, key)
}

type DsRemote struct {
}

func (r *DsRemote) Address(ctx context.Context) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, errors.New("not implemented")
}

func (r *DsRemote) Push(ctx context.Context) error {
	return errors.New("not implemented")
}

func (r *DsRemote) Pull(ctx context.Context) error {
	return errors.New("not implemented")
}

// TODO: split into remotes and remote buckets

// NewRemoteDsBucket creates a new bucket that stores remotes.
func NewRemoteDsBucket(clock Clock, blocks bucket.Blockstore, dstore datastore.Datastore) (*RemoteDsBucket, error) {
	bucket, err := NewDsBucket(blocks, namespace.Wrap(dstore, datastore.NewKey("shards/")))
	if err != nil {
		return nil, err
	}

	return &RemoteDsBucket{
		clock,
		bucket,
		namespace.Wrap(dstore, datastore.NewKey("values/")),
	}, nil
}

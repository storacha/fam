package bucket

import (
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	pail "github.com/storacha/go-pail"
	"github.com/storacha/go-ucanto/core/delegation"
)

type DelegationBucket struct {
	bucket Bucket[ipld.Link]
	values datastore.Datastore
}

func (db *DelegationBucket) Entries(ctx context.Context, opts ...pail.EntriesOption) iter.Seq2[Entry[delegation.Delegation], error] {
	return func(yield func(Entry[delegation.Delegation], error) bool) {
		for entry, err := range db.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[delegation.Delegation]{}, err)
				return
			}
			dlgBytes, err := db.values.Get(ctx, datastore.NewKey(entry.Value.String()))
			if err != nil {
				yield(Entry[delegation.Delegation]{}, err)
				return
			}
			dlg, err := delegation.Extract(dlgBytes)
			if err != nil {
				yield(Entry[delegation.Delegation]{}, err)
				return
			}
			if !yield(Entry[delegation.Delegation]{entry.Key, dlg}, err) {
				return
			}
		}
	}
}

func (db *DelegationBucket) Get(ctx context.Context, key string) (delegation.Delegation, error) {
	link, err := db.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting delegation link: %w", err)
	}

	dlgBytes, err := db.values.Get(ctx, datastore.NewKey(link.String()))
	if err != nil {
		return nil, fmt.Errorf("getting delegation: %w", err)
	}

	return delegation.Extract(dlgBytes)
}

func (db *DelegationBucket) Put(ctx context.Context, key string, dlg delegation.Delegation) error {
	dlgBytes, err := io.ReadAll(dlg.Archive())
	if err != nil {
		return fmt.Errorf("archiving delegation: %w", err)
	}

	err = db.values.Put(ctx, datastore.NewKey(dlg.Link().String()), dlgBytes)
	if err != nil {
		return fmt.Errorf("putting delegation: %w", err)
	}

	c, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Car),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(dlgBytes)
	if err != nil {
		return fmt.Errorf("hashing delegation archive: %w", err)
	}

	return db.bucket.Put(ctx, key, cidlink.Link{Cid: c})
}

func (db *DelegationBucket) Del(ctx context.Context, key string) error {
	return db.bucket.Del(ctx, key)
}

func NewDelegationBucket(dstore datastore.Datastore) (*DelegationBucket, error) {
	bucket, err := NewDsBucket(namespace.Wrap(dstore, datastore.NewKey("shards/")))
	if err != nil {
		return nil, err
	}
	return &DelegationBucket{
		bucket,
		namespace.Wrap(dstore, datastore.NewKey("values/")),
	}, nil
}

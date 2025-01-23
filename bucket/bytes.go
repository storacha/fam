package bucket

import (
	"context"
	"fmt"
	"iter"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type DsBytesBucket struct {
	codec  multicodec.Code
	bucket Bucket[ipld.Link]
	values datastore.Datastore
}

func (bk *DsBytesBucket) Root(ctx context.Context) (ipld.Link, error) {
	return bk.bucket.Root(ctx)
}

func (bk *DsBytesBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[[]byte], error] {
	return func(yield func(Entry[[]byte], error) bool) {
		for entry, err := range bk.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[[]byte]{}, err)
				return
			}
			b, err := bk.values.Get(ctx, datastore.NewKey(entry.Value.String()))
			if err != nil {
				yield(Entry[[]byte]{}, err)
				return
			}
			if !yield(Entry[[]byte]{entry.Key, b}, err) {
				return
			}
		}
	}
}

func (bk *DsBytesBucket) Get(ctx context.Context, key string) ([]byte, error) {
	link, err := bk.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting key link: %w", err)
	}
	return bk.values.Get(ctx, datastore.NewKey(link.String()))
}

func (bk *DsBytesBucket) Put(ctx context.Context, key string, value []byte) error {
	c, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(bk.codec),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(value)
	if err != nil {
		return fmt.Errorf("hashing key bytes: %w", err)
	}

	err = bk.values.Put(ctx, datastore.NewKey(c.String()), value)
	if err != nil {
		return fmt.Errorf("putting key: %w", err)
	}

	return bk.bucket.Put(ctx, key, cidlink.Link{Cid: c})
}

func (bk *DsBytesBucket) Del(ctx context.Context, key string) error {
	return bk.bucket.Del(ctx, key)
}

// NewDsBytesBucket is a bucket that stores values as bytes in a [datastore.Datastore].
func NewDsBytesBucket(bucket Bucket[ipld.Link], dstore datastore.Datastore, codec multicodec.Code) *DsBytesBucket {
	return &DsBytesBucket{codec, bucket, dstore}
}

// NewIdentityBytesBucket creates a bucket that stores values as bytes in an identity CID.
func NewIdentityBytesBucket(bucket Bucket[ipld.Link]) Bucket[[]byte] {
	return NewIdentityBucket(bucket, id, id)
}

func id(b []byte) ([]byte, error) {
	return b, nil
}

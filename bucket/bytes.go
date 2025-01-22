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

type BytesEncoder[T any] func(value T) ([]byte, error)
type BytesDecoder[T any] func(bytes []byte) (T, error)

type IdentityBytesBucket[T any] struct {
	bucket Bucket[ipld.Link]
	encode BytesEncoder[T]
	decode BytesDecoder[T]
}

func (bk *IdentityBytesBucket[T]) Root(ctx context.Context) (ipld.Link, error) {
	return bk.bucket.Root(ctx)
}

func (bk *IdentityBytesBucket[T]) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error] {
	return func(yield func(Entry[T], error) bool) {
		for entry, err := range bk.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			cid, err := cid.Cast([]byte(entry.Value.Binary()))
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			dmh, err := multihash.Decode(cid.Hash())
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			v, err := bk.decode(dmh.Digest)
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			if !yield(Entry[T]{entry.Key, v}, err) {
				return
			}
		}
	}
}

func (bk *IdentityBytesBucket[T]) Get(ctx context.Context, key string) (T, error) {
	var value T
	link, err := bk.bucket.Get(ctx, key)
	if err != nil {
		return value, fmt.Errorf("getting key link: %w", err)
	}
	cid, err := cid.Cast([]byte(link.Binary()))
	if err != nil {
		return value, err
	}
	dmh, err := multihash.Decode(cid.Hash())
	if err != nil {
		return value, err
	}
	return bk.decode(dmh.Digest)
}

func (bk *IdentityBytesBucket[T]) Put(ctx context.Context, key string, value T) error {
	b, err := bk.encode(value)
	if err != nil {
		return err
	}
	c, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Identity),
		MhType:   multihash.IDENTITY,
		MhLength: -1,
	}.Sum(b)
	if err != nil {
		return fmt.Errorf("hashing key bytes: %w", err)
	}
	return bk.bucket.Put(ctx, key, cidlink.Link{Cid: c})
}

func (bk *IdentityBytesBucket[T]) Del(ctx context.Context, key string) error {
	return bk.bucket.Del(ctx, key)
}

// NewIdentityBytesBucket creates a bytes bucket whose values are stored as the identity CID.
func NewIdentityBytesBucket[T any](bucket Bucket[ipld.Link], encode BytesEncoder[T], decode BytesDecoder[T]) *IdentityBytesBucket[T] {
	return &IdentityBytesBucket[T]{bucket, encode, decode}
}

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

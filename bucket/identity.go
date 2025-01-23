package bucket

import (
	"context"
	"fmt"
	"iter"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type BytesEncoder[T any] func(value T) ([]byte, error)
type BytesDecoder[T any] func(bytes []byte) (T, error)

type IdentityBucket[T any] struct {
	bucket Bucket[ipld.Link]
	encode BytesEncoder[T]
	decode BytesDecoder[T]
}

func (bk *IdentityBucket[T]) Root(ctx context.Context) (ipld.Link, error) {
	return bk.bucket.Root(ctx)
}

func (bk *IdentityBucket[T]) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error] {
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

func (bk *IdentityBucket[T]) Get(ctx context.Context, key string) (T, error) {
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

func (bk *IdentityBucket[T]) Put(ctx context.Context, key string, value T) error {
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

func (bk *IdentityBucket[T]) Del(ctx context.Context, key string) error {
	return bk.bucket.Del(ctx, key)
}

// NewIdentityBucket creates a bucket whose values are stored in an identity CID.
func NewIdentityBucket[T any](bucket Bucket[ipld.Link], encode BytesEncoder[T], decode BytesDecoder[T]) *IdentityBucket[T] {
	return &IdentityBucket[T]{bucket, encode, decode}
}

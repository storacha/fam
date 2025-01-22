package bucket

import (
	"bytes"
	"context"
	"fmt"
	"iter"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/storacha/go-pail/ipld/node"
)

type IpldNodeBucket[T any] struct {
	bucket Bucket[ipld.Node]
	bind   node.BinderFunc[T]
	unbind node.UnbinderFunc[T]
}

func (bk *IpldNodeBucket[T]) Root(ctx context.Context) (ipld.Link, error) {
	return bk.bucket.Root(ctx)
}

func (bk *IpldNodeBucket[T]) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error] {
	return func(yield func(Entry[T], error) bool) {
		for entry, err := range bk.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			value, err := bk.bind(entry.Value)
			if err != nil {
				yield(Entry[T]{}, err)
				return
			}
			if !yield(Entry[T]{entry.Key, value}, err) {
				return
			}
		}
	}
}

func (bk *IpldNodeBucket[T]) Get(ctx context.Context, key string) (T, error) {
	var value T

	nd, err := bk.bucket.Get(ctx, key)
	if err != nil {
		return value, fmt.Errorf("getting key link: %w", err)
	}

	return bk.bind(nd)
}

func (bk *IpldNodeBucket[T]) Put(ctx context.Context, key string, value T) error {
	nd, err := bk.unbind(value)
	if err != nil {
		return fmt.Errorf("unbinding value: %w", err)
	}

	err = bk.bucket.Put(ctx, key, nd)
	if err != nil {
		return fmt.Errorf("putting key: %w", err)
	}

	return err
}

func (bk *IpldNodeBucket[T]) Del(ctx context.Context, key string) error {
	return bk.bucket.Del(ctx, key)
}

// NewIpldNodeBucket is a bucket that stores IPLD nodes.
func NewIpldNodeBucket[T any](bucket Bucket[ipld.Node], bind node.BinderFunc[T], unbind node.UnbinderFunc[T]) *IpldNodeBucket[T] {
	return &IpldNodeBucket[T]{
		bucket,
		bind,
		unbind,
	}
}

type IpldBytesBucket struct {
	bucket Bucket[[]byte]
	encode codec.Encoder
	decode codec.Decoder
}

func (bk *IpldBytesBucket) Root(ctx context.Context) (ipld.Link, error) {
	return bk.bucket.Root(ctx)
}

func (bk *IpldBytesBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[ipld.Node], error] {
	return func(yield func(Entry[ipld.Node], error) bool) {
		for entry, err := range bk.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[ipld.Node]{}, err)
				return
			}

			np := basicnode.Prototype.Any
			nb := np.NewBuilder()
			err = dagcbor.Decode(nb, bytes.NewReader(entry.Value))
			if err != nil {
				yield(Entry[ipld.Node]{}, err)
				return
			}
			if !yield(Entry[ipld.Node]{entry.Key, nb.Build()}, err) {
				return
			}
		}
	}
}

func (bk *IpldBytesBucket) Get(ctx context.Context, key string) (ipld.Node, error) {
	b, err := bk.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting key link: %w", err)
	}

	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	err = dagcbor.Decode(nb, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("CBOR decoding: %w", err)
	}
	return nb.Build(), nil
}

func (bk *IpldBytesBucket) Put(ctx context.Context, key string, value ipld.Node) error {
	buf := bytes.NewBuffer([]byte{})
	err := bk.encode(value, buf)
	if err != nil {
		return fmt.Errorf("CBOR encoding: %w", err)
	}

	err = bk.bucket.Put(ctx, key, buf.Bytes())
	if err != nil {
		return fmt.Errorf("putting key: %w", err)
	}

	return err
}

func (bk *IpldBytesBucket) Del(ctx context.Context, key string) error {
	return bk.bucket.Del(ctx, key)
}

// NewIpldBytesBucket is a bucket that stores IPLD encoded bytes.
func NewIpldBytesBucket(bucket Bucket[[]byte], encode codec.Encoder, decode codec.Decoder) *IpldBytesBucket {
	return &IpldBytesBucket{bucket, encode, decode}
}

// c, err := cid.Prefix{
// 	Version:  1,
// 	Codec:    uint64(multicodec.DagCbor),
// 	MhType:   multihash.SHA2_256,
// 	MhLength: -1,
// }.Sum(buf.Bytes())
// if err != nil {
// 	return fmt.Errorf("hashing bytes: %w", err)
// }

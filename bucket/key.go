package bucket

import (
	"context"
	"fmt"
	"iter"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	rsa "github.com/storacha/go-ucanto/principal/rsa/signer"
)

type KeyBucket struct {
	bucket Bucket[ipld.Link]
	values datastore.Datastore
}

func (kb *KeyBucket) Root(ctx context.Context) (ipld.Link, error) {
	return kb.bucket.Root(ctx)
}

func (kb *KeyBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[principal.Signer], error] {
	return func(yield func(Entry[principal.Signer], error) bool) {
		for entry, err := range kb.bucket.Entries(ctx, opts...) {
			if err != nil {
				yield(Entry[principal.Signer]{}, err)
				return
			}
			keyBytes, err := kb.values.Get(ctx, datastore.NewKey(entry.Value.String()))
			if err != nil {
				yield(Entry[principal.Signer]{}, err)
				return
			}
			s, err := decodeKey(keyBytes)
			if err != nil {
				yield(Entry[principal.Signer]{}, err)
				return
			}
			if !yield(Entry[principal.Signer]{entry.Key, s}, err) {
				return
			}
		}
	}
}

func (kb *KeyBucket) Get(ctx context.Context, key string) (principal.Signer, error) {
	link, err := kb.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting key link: %w", err)
	}

	keyBytes, err := kb.values.Get(ctx, datastore.NewKey(link.String()))
	if err != nil {
		return nil, fmt.Errorf("getting key: %w", err)
	}

	return decodeKey(keyBytes)
}

func (kb *KeyBucket) Put(ctx context.Context, key string, signer principal.Signer) error {
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

	err = kb.values.Put(ctx, datastore.NewKey(c.String()), keyBytes)
	if err != nil {
		return fmt.Errorf("putting key: %w", err)
	}

	return kb.bucket.Put(ctx, key, cidlink.Link{Cid: c})
}

func (kb *KeyBucket) Del(ctx context.Context, key string) error {
	return kb.bucket.Del(ctx, key)
}

func NewKeyBucket(blocks Blockstore, dstore datastore.Datastore) (*KeyBucket, error) {
	bucket, err := NewDsBucket(blocks, namespace.Wrap(dstore, datastore.NewKey("shards/")))
	if err != nil {
		return nil, err
	}
	return &KeyBucket{
		bucket,
		namespace.Wrap(dstore, datastore.NewKey("values/")),
	}, nil
}

func decodeKey(b []byte) (principal.Signer, error) {
	s, err := ed25519.Decode(b)
	if err != nil {
		s, err = rsa.Decode(b)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

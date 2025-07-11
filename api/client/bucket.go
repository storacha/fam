package client

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/fam/bucket"
	bucketcaps "github.com/storacha/fam/bucket/capabilities"
	"github.com/storacha/fam/capabilities/util"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

func (c *Client) Bucket(ctx context.Context, id did.DID) (*Bucket, error) {
	ids, err := c.Buckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing buckets: %w", err)
	}
	if !slices.Contains(ids, id) {
		return nil, bucket.ErrNotFound
	}
	return &Bucket{id, c.signer, c.conn, c.Proofs}, nil
}

type Bucket struct {
	id     did.DID
	signer ucan.Signer
	conn   client.Connection
	Proofs ProofProviderFunc
}

func (b *Bucket) Del(ctx context.Context, key string) error {
	panic("unimplemented")
}

func (b *Bucket) Entries(ctx context.Context, opts ...bucket.EntriesOption) iter.Seq2[bucket.Entry[ipld.Link], error] {
	return func(yield func(bucket.Entry[ipld.Link], error) bool) {
		root, err := b.Root(ctx)
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, err)
			return
		}
		proofs, err := b.Proofs(bucketcaps.EntriesAbility)
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, err)
			return
		}
		inv, err := bucketcaps.Entries.Invoke(
			b.signer,
			b.conn.ID(),
			b.signer.DID().String(),
			bucketcaps.EntriesCaveats{
				ID:   b.id,
				Root: root,
			},
			delegation.WithProof(proofs...),
		)
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, err)
			return
		}
		res, err := client.Execute(ctx, []invocation.Invocation{inv}, b.conn)
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, fmt.Errorf("executing %s invocation: %w", bucketcaps.RootAbility, err))
			return
		}
		rcptLink, ok := res.Get(inv.Link())
		if !ok {
			yield(bucket.Entry[ipld.Link]{}, fmt.Errorf("missing receipt for invocation: %s", inv.Link()))
			return
		}
		rcptReader, err := bucketcaps.NewEntriesReceiptReader()
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, err)
			return
		}
		rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
		if err != nil {
			yield(bucket.Entry[ipld.Link]{}, fmt.Errorf("reading receipt: %w", err))
			return
		}
		o, x := result.Unwrap(rcpt.Out())
		if x != nil {
			f, err := util.BindFailure(x)
			if err != nil {
				yield(bucket.Entry[ipld.Link]{}, err)
				return
			}
			yield(bucket.Entry[ipld.Link]{}, fmt.Errorf("invocation failure: %+v", f))
			return
		}
		for _, entry := range o.Results {
			if !yield(bucket.Entry[ipld.Link](entry), nil) {
				return
			}
		}
	}
}

func (b *Bucket) Get(ctx context.Context, key string) (ipld.Link, error) {
	panic("unimplemented")
}

func (b *Bucket) Put(ctx context.Context, key string, value ipld.Link) error {
	panic("unimplemented")
}

func (b *Bucket) Root(ctx context.Context) (ipld.Link, error) {
	proofs, err := b.Proofs(bucketcaps.RootAbility)
	if err != nil {
		return nil, err
	}
	inv, err := bucketcaps.Root.Invoke(
		b.signer,
		b.conn.ID(),
		b.signer.DID().String(),
		bucketcaps.RootCaveats{
			ID:        b.id,
			Timestamp: time.Now(),
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, err
	}
	res, err := client.Execute(ctx, []invocation.Invocation{inv}, b.conn)
	if err != nil {
		return nil, fmt.Errorf("executing %s invocation: %w", bucketcaps.RootAbility, err)
	}
	rcptLink, ok := res.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("missing receipt for invocation: %s", inv.Link())
	}
	rcptReader, err := bucketcaps.NewRootReceiptReader()
	if err != nil {
		return nil, err
	}
	rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}
	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("invocation failure: %+v", f)
	}
	return o.Root, nil
}

var _ bucket.Bucket[ipld.Link] = (*Bucket)(nil)

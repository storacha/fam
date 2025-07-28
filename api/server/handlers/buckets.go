package handlers

import (
	"context"

	buckets "github.com/storacha/fam/buckets/capabilities"
	"github.com/storacha/fam/store"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
)

func Buckets(userdata *store.UserDataStore) []server.Option {
	return []server.Option{
		server.WithServiceMethod(
			buckets.AddAbility,
			server.Provide(buckets.Add, func(ctx context.Context, cap ucan.Capability[buckets.AddCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[buckets.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(inv.Blocks()))
				if err != nil {
					return nil, nil, err
				}
				proof, err := delegation.NewDelegationView(cap.Nb().Proof, bs)
				if err != nil {
					return nil, nil, err
				}
				id, err := userdata.AddBucket(ctx, proof)
				if err != nil {
					return nil, nil, err
				}
				return result.Ok[buckets.AddOk, failure.IPLDBuilderFailure](buckets.AddOk{ID: id}), nil, nil
			}),
		),
		server.WithServiceMethod(
			buckets.ListAbility,
			server.Provide(buckets.List, func(ctx context.Context, cap ucan.Capability[buckets.ListCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[buckets.ListOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				bks, err := userdata.Buckets(ctx)
				if err != nil {
					return nil, nil, err
				}
				var results []buckets.Bucket
				for id := range bks {
					results = append(results, buckets.Bucket{ID: id})
				}
				return result.Ok[buckets.ListOk, failure.IPLDBuilderFailure](buckets.ListOk{Results: results}), nil, nil
			}),
		),
	}
}

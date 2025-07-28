package handlers

import (
	"context"
	"errors"

	"github.com/storacha/fam/bucket"
	bucketcaps "github.com/storacha/fam/bucket/capabilities"
	"github.com/storacha/fam/store"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
)

func Bucket(userdata *store.UserDataStore) []server.Option {
	return []server.Option{
		server.WithServiceMethod(
			bucketcaps.EntriesAbility,
			server.Provide(bucketcaps.Entries, func(ctx context.Context, cap ucan.Capability[bucketcaps.EntriesCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[bucketcaps.EntriesOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				nb := cap.Nb()
				bk, err := userdata.Bucket(ctx, nb.ID)
				if err != nil {
					return nil, nil, err
				}
				root, err := bk.Root(ctx)
				if err != nil {
					return nil, nil, err
				}
				if root.String() != cap.Nb().Root.String() {
					return result.Error[bucketcaps.EntriesOk, failure.IPLDBuilderFailure](NewRootConflictError(cap.Nb().Root, root)), nil, nil
				}

				opts := []bucket.EntriesOption{}
				if nb.Prefix != nil {
					opts = append(opts, bucket.WithKeyPrefix(*nb.Prefix))
				}
				if nb.GreaterThan != nil {
					opts = append(opts, bucket.WithKeyGreaterThan(*nb.GreaterThan))
				}
				if nb.GreaterThanEqual != nil {
					opts = append(opts, bucket.WithKeyGreaterThanOrEqual(*nb.GreaterThanEqual))
				}
				if nb.LessThan != nil {
					opts = append(opts, bucket.WithKeyLessThan(*nb.LessThan))
				}
				if nb.LessThanEqual != nil {
					opts = append(opts, bucket.WithKeyLessThanOrEqual(*nb.LessThanEqual))
				}
				limit := 1000
				if nb.Limit != nil {
					limit = *nb.Limit
				}

				i := 0
				var entries []bucketcaps.Entry
				for entry, err := range bk.Entries(ctx, opts...) {
					if err != nil {
						return nil, nil, err
					}
					entries = append(entries, bucketcaps.Entry(entry))
					i++
					if i >= limit {
						break
					}
				}

				return result.Ok[bucketcaps.EntriesOk, failure.IPLDBuilderFailure](bucketcaps.EntriesOk{Results: entries}), nil, nil
			}),
		),
		server.WithServiceMethod(
			bucketcaps.GetAbility,
			server.Provide(bucketcaps.Get, func(ctx context.Context, cap ucan.Capability[bucketcaps.GetCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[bucketcaps.GetOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				bk, err := userdata.Bucket(ctx, cap.Nb().ID)
				if err != nil {
					return nil, nil, err
				}
				root, err := bk.Root(ctx)
				if err != nil {
					return nil, nil, err
				}
				if root.String() != cap.Nb().Root.String() {
					return result.Error[bucketcaps.GetOk, failure.IPLDBuilderFailure](NewRootConflictError(cap.Nb().Root, root)), nil, nil
				}
				val, err := bk.Get(ctx, cap.Nb().Key)
				if err != nil {
					if errors.Is(err, bucket.ErrNotFound) {
						return result.Error[bucketcaps.GetOk, failure.IPLDBuilderFailure](NewNotFoundError()), nil, nil
					}
					return nil, nil, err
				}
				return result.Ok[bucketcaps.GetOk, failure.IPLDBuilderFailure](bucketcaps.GetOk{Value: val}), nil, nil
			}),
		),
		server.WithServiceMethod(
			bucketcaps.PutAbility,
			server.Provide(bucketcaps.Put, func(ctx context.Context, cap ucan.Capability[bucketcaps.PutCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[bucketcaps.PutOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				bk, err := userdata.Bucket(ctx, cap.Nb().ID)
				if err != nil {
					return nil, nil, err
				}
				root, err := bk.Root(ctx)
				if err != nil {
					return nil, nil, err
				}
				if root.String() != cap.Nb().Root.String() {
					return result.Error[bucketcaps.PutOk, failure.IPLDBuilderFailure](NewRootConflictError(cap.Nb().Root, root)), nil, nil
				}
				err = bk.Put(ctx, cap.Nb().Key, cap.Nb().Value)
				if err != nil {
					return nil, nil, err
				}
				root, err = bk.Root(ctx)
				if err != nil {
					return nil, nil, err
				}
				return result.Ok[bucketcaps.PutOk, failure.IPLDBuilderFailure](bucketcaps.PutOk{Root: root}), nil, nil
			}),
		),
		server.WithServiceMethod(
			bucketcaps.RootAbility,
			server.Provide(bucketcaps.Root, func(ctx context.Context, cap ucan.Capability[bucketcaps.RootCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[bucketcaps.RootOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				bk, err := userdata.Bucket(ctx, cap.Nb().ID)
				if err != nil {
					return nil, nil, err
				}
				root, err := bk.Root(ctx)
				if err != nil {
					return nil, nil, err
				}
				return result.Ok[bucketcaps.RootOk, failure.IPLDBuilderFailure](bucketcaps.RootOk{Root: root}), nil, nil
			}),
		),
	}
}

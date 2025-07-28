package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/fam/clock"
	clockcaps "github.com/storacha/fam/clock/capabilities"
	"github.com/storacha/fam/store"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/rsa/verifier"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
)

func Clock(userdata *store.UserDataStore, host host.Host) []server.Option {
	return []server.Option{
		server.WithServiceMethod(
			clockcaps.HeadAbility,
			server.Provide(clockcaps.Head, func(ctx context.Context, cap ucan.Capability[clockcaps.HeadCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[clockcaps.HeadOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return nil, nil, err
				}

				b, err := userdata.Bucket(ctx, id)
				if err != nil {
					return nil, nil, err
				}

				c, ok := b.(clock.Clock)
				if !ok {
					return nil, nil, errors.New("bucket does not implement clock")
				}

				head, err := c.Head(ctx)
				if err != nil {
					return nil, nil, err
				}

				return result.Ok[clockcaps.HeadOk, failure.IPLDBuilderFailure](clockcaps.HeadOk{Head: head}), nil, nil
			}),
		),
		server.WithServiceMethod(
			clockcaps.AdvanceAbility,
			server.Provide(clockcaps.Advance, func(ctx context.Context, cap ucan.Capability[clockcaps.AdvanceCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[clockcaps.AdvanceOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return nil, nil, err
				}

				b, err := userdata.Bucket(ctx, id)
				if err != nil {
					return nil, nil, err
				}

				c, ok := b.(clock.Clock)
				if !ok {
					return nil, nil, errors.New("bucket does not implement clock")
				}

				v, err := verifier.Parse(inv.Issuer().DID().String())
				if err != nil {
					return nil, nil, err
				}

				pk, err := crypto.UnmarshalEd25519PublicKey(v.Raw())
				if err != nil {
					return nil, nil, err
				}

				peerID, err := peer.IDFromPublicKey(pk)
				if err != nil {
					return nil, nil, err
				}

				client := http.Client{Transport: trustlessgateway.NewP2PTransport(host)}
				url := fmt.Sprintf("libp2p://%s", peerID)
				fetcher := trustlessgateway.NewClient(url, &client)

				head, err := c.Advance(ctx, cap.Nb().Event, clock.WithBlockFetcher(fetcher))
				if err != nil {
					return nil, nil, err
				}

				return result.Ok[clockcaps.AdvanceOk, failure.IPLDBuilderFailure](clockcaps.AdvanceOk{Head: head}), nil, nil
			}),
		),
	}
}

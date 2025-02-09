package clock

import (
	"context"
	"fmt"
	"net/http"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
)

func NewServer(agent principal.Signer, clocks ClockStore, host host.Host) (server.ServerView, error) {
	return server.NewServer(
		agent,
		server.WithServiceMethod(
			HeadAbility,
			server.Provide(Head, func(cap ucan.Capability[HeadCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (Ok, fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return Ok{}, nil, err
				}

				c, err := clocks.Get(context.TODO(), id)
				if err != nil {
					return Ok{}, nil, err
				}

				head, err := c.Head(context.TODO())
				if err != nil {
					return Ok{}, nil, err
				}

				return Ok{Head: head}, nil, nil
			}),
		),
		server.WithServiceMethod(
			AdvanceAbility,
			server.Provide(Advance, func(cap ucan.Capability[AdvanceCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (Ok, fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return Ok{}, nil, err
				}

				c, err := clocks.Get(context.TODO(), id)
				if err != nil {
					return Ok{}, nil, err
				}

				v, err := verifier.Parse(inv.Issuer().DID().String())
				if err != nil {
					return Ok{}, nil, err
				}

				pk, err := crypto.UnmarshalEd25519PublicKey(v.Raw())
				if err != nil {
					return Ok{}, nil, err
				}

				peerID, err := peer.IDFromPublicKey(pk)
				if err != nil {
					return Ok{}, nil, err
				}

				client := http.Client{Transport: trustlessgateway.NewP2PTransport(host)}
				url := fmt.Sprintf("libp2p://%s", peerID)
				fetcher := trustlessgateway.NewClient(url, &client)

				head, err := c.Advance(context.TODO(), cap.Nb().Event, WithBlockFetcher(fetcher))
				if err != nil {
					return Ok{}, nil, err
				}

				return Ok{Head: head}, nil, nil
			}),
		),
	)
}

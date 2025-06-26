package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/fam/clock"
	clockcaps "github.com/storacha/fam/clock/capabilities"
	ping "github.com/storacha/fam/ping/capabilities"
	"github.com/storacha/fam/store"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/server"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
)

var log = logging.Logger("api")

func NewServeMux(server server.ServerView) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "🔥 fam UCAN RPC %s\n", "v0.0.0") // TODO: add build version
		fmt.Fprint(w, "- https://github.com/storacha/fam\n")
		fmt.Fprintf(w, "- %s", server.ID().DID())
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		res, err := server.Request(r.Context(), ucanhttp.NewHTTPRequest(r.Body, r.Header))
		if err != nil {
			log.Errorf("handling UCAN request: %w", err)
			http.Error(w, "failed to handle UCAN request", http.StatusInternalServerError)
			return
		}

		for key, vals := range res.Headers() {
			for _, v := range vals {
				w.Header().Add(key, v)
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		_, err = io.Copy(w, res.Body())
		if err != nil {
			log.Errorf("sending UCAN response: %w", err)
			http.Error(w, "failed to send UCAN response", http.StatusInternalServerError)
			return
		}
	})
	return mux, nil
}

func NewHandler(server server.ServerView) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := server.Request(r.Context(), ucanhttp.NewHTTPRequest(r.Body, r.Header))
		if err != nil {
			log.Errorf("handling UCAN request: %w", err)
			http.Error(w, "failed to handle UCAN request", http.StatusInternalServerError)
			return
		}

		for key, vals := range res.Headers() {
			for _, v := range vals {
				w.Header().Add(key, v)
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		_, err = io.Copy(w, res.Body())
		if err != nil {
			log.Errorf("sending UCAN response: %w", err)
			http.Error(w, "failed to send UCAN response", http.StatusInternalServerError)
			return
		}
	})
}

// NewServer creates a new UCAN RPC server.
func NewServer(agent principal.Signer, userdata *store.UserDataStore, host host.Host) (server.ServerView, error) {
	return server.NewServer(
		agent,
		server.WithServiceMethod(
			ping.SendAbility,
			server.Provide(ping.Send, func(ctx context.Context, cap ucan.Capability[ping.SendCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (ping.SendOk, fx.Effects, error) {
				return ping.SendOk{Message: cap.Nb().Message}, nil, nil
			}),
		),
		server.WithServiceMethod(
			clockcaps.HeadAbility,
			server.Provide(clockcaps.Head, func(ctx context.Context, cap ucan.Capability[clockcaps.HeadCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (clockcaps.HeadOk, fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return clockcaps.HeadOk{}, nil, err
				}

				b, err := userdata.Bucket(ctx, id)
				if err != nil {
					return clockcaps.HeadOk{}, nil, err
				}

				c, ok := b.(clock.Clock)
				if !ok {
					return clockcaps.HeadOk{}, nil, errors.New("bucket does not implement clock")
				}

				head, err := c.Head(ctx)
				if err != nil {
					return clockcaps.HeadOk{}, nil, err
				}

				return clockcaps.HeadOk{Head: head}, nil, nil
			}),
		),
		server.WithServiceMethod(
			clockcaps.AdvanceAbility,
			server.Provide(clockcaps.Advance, func(ctx context.Context, cap ucan.Capability[clockcaps.AdvanceCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (clockcaps.AdvanceOk, fx.Effects, error) {
				id, err := did.Parse(cap.With())
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				b, err := userdata.Bucket(ctx, id)
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				c, ok := b.(clock.Clock)
				if !ok {
					return clockcaps.AdvanceOk{}, nil, errors.New("bucket does not implement clock")
				}

				v, err := verifier.Parse(inv.Issuer().DID().String())
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				pk, err := crypto.UnmarshalEd25519PublicKey(v.Raw())
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				peerID, err := peer.IDFromPublicKey(pk)
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				client := http.Client{Transport: trustlessgateway.NewP2PTransport(host)}
				url := fmt.Sprintf("libp2p://%s", peerID)
				fetcher := trustlessgateway.NewClient(url, &client)

				head, err := c.Advance(ctx, cap.Nb().Event, clock.WithBlockFetcher(fetcher))
				if err != nil {
					return clockcaps.AdvanceOk{}, nil, err
				}

				return clockcaps.AdvanceOk{Head: head}, nil, nil
			}),
		),
	)
}

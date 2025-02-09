package remote

import (
	"context"
	"fmt"
	"net/http"

	"github.com/alanshaw/ucanp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/fam/clock"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/ucan"
)

type ClockRemote struct {
	agent  principal.Signer
	bucket ucan.Principal
	proof  delegation.Proof
	clock  clock.Clock
	remote peer.AddrInfo
	host   host.Host
}

func (r *ClockRemote) Address(ctx context.Context) (peer.AddrInfo, error) {
	return r.remote, nil
}

func (r *ClockRemote) Push(ctx context.Context) error {
	pk, err := r.remote.ID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extracting public key: %w", err)
	}
	raw, err := pk.Raw()
	if err != nil {
		return fmt.Errorf("extracting raw public key: %w", err)
	}
	remotePrincipal, err := verifier.FromRaw(raw)
	if err != nil {
		return fmt.Errorf("creating verifier from raw public key: %w", err)
	}
	channel := ucanp2p.NewHTTPChannel(r.host, r.remote, "/")
	conn, err := client.NewConnection(remotePrincipal, channel)
	if err != nil {
		return fmt.Errorf("opening connection: %w", err)
	}

	head, err := r.clock.Head(ctx)
	if err != nil {
		return fmt.Errorf("getting clock head: %w", err)
	}

	var invs []invocation.Invocation
	for _, h := range head {
		nb := clock.AdvanceCaveats{Event: h}
		inv, err := clock.Advance.Invoke(r.agent, remotePrincipal, r.bucket.DID().String(), nb, delegation.WithProof(r.proof))
		if err != nil {
			return fmt.Errorf("issuing invocation: %w", err)
		}
		invs = append(invs, inv)
	}

	resp, err := client.Execute(invs, conn)
	if err != nil {
		return fmt.Errorf("executing invocations: %w", err)
	}

	rcptReader, err := receipt.NewReceiptReaderFromTypes[clock.Ok, clock.Err](clock.OkType(), clock.ErrType())
	if err != nil {
		return fmt.Errorf("creating receipt reader: %w", err)
	}

	for _, inv := range invs {
		rcptLink, ok := resp.Get(inv.Link())
		if !ok {
			return fmt.Errorf("missing receipt for invocation: %s", inv.Link())
		}

		rcpt, err := rcptReader.Read(rcptLink, resp.Blocks())
		if err != nil {
			return fmt.Errorf("reading receipt: %w", err)
		}

		_, x := result.Unwrap(rcpt.Out())
		if x != nil {
			f, err := clock.BindFailure(x)
			if err != nil {
				return err
			}
			return fmt.Errorf("invocation failure: %+v", f)
		}
	}

	return nil
}

func (r *ClockRemote) Pull(ctx context.Context) error {
	pk, err := r.remote.ID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extracting public key: %w", err)
	}
	raw, err := pk.Raw()
	if err != nil {
		return fmt.Errorf("extracting raw public key: %w", err)
	}
	remotePrincipal, err := verifier.FromRaw(raw)
	if err != nil {
		return fmt.Errorf("creating verifier from raw public key: %w", err)
	}
	channel := ucanp2p.NewHTTPChannel(r.host, r.remote, "/")
	conn, err := client.NewConnection(remotePrincipal, channel)
	if err != nil {
		return fmt.Errorf("opening connection: %w", err)
	}

	nb := clock.HeadCaveats{}
	inv, err := clock.Head.Invoke(r.agent, remotePrincipal, r.bucket.DID().String(), nb, delegation.WithProof(r.proof))
	if err != nil {
		return fmt.Errorf("issuing invocation: %w", err)
	}

	resp, err := client.Execute([]invocation.Invocation{inv}, conn)
	if err != nil {
		return fmt.Errorf("executing invocations: %w", err)
	}

	rcptReader, err := receipt.NewReceiptReaderFromTypes[clock.Ok, clock.Err](clock.OkType(), clock.ErrType())
	if err != nil {
		return fmt.Errorf("creating receipt reader: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return fmt.Errorf("missing receipt for invocation: %s", inv.Link())
	}

	rcpt, err := rcptReader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return fmt.Errorf("reading receipt: %w", err)
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := clock.BindFailure(x)
		if err != nil {
			return err
		}
		return fmt.Errorf("invocation failure: %+v", f)
	}

	client := http.Client{Transport: trustlessgateway.NewP2PTransport(r.host)}
	url := fmt.Sprintf("libp2p://%s", r.remote.ID)
	fetcher := trustlessgateway.NewClient(url, &client)

	for _, event := range o.Head {
		_, err := r.clock.Advance(ctx, event, clock.WithBlockFetcher(fetcher))
		if err != nil {
			return fmt.Errorf("advancing clock: %w", err)
		}
	}

	return nil
}

func NewClockRemote(
	agent principal.Signer,
	bucket ucan.Principal,
	proof delegation.Proof,
	clock clock.Clock,
	remoteAddr peer.AddrInfo,
	host host.Host,
) *ClockRemote {
	return &ClockRemote{agent, bucket, proof, clock, remoteAddr, host}
}

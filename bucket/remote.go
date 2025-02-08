package bucket

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/alanshaw/ucanp2p"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/storacha/fam/capabilities/clock"
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
	agentID    principal.Signer
	bucketID   ucan.Principal
	proof      delegation.Proof
	clock      Clock
	remoteAddr peer.AddrInfo
	host       host.Host
}

func (r *ClockRemote) Address(ctx context.Context) (peer.AddrInfo, error) {
	return r.remoteAddr, nil
}

func (r *ClockRemote) Push(ctx context.Context) error {
	pk, err := r.remoteAddr.ID.ExtractPublicKey()
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
	channel := ucanp2p.NewHTTPChannel(r.host, r.remoteAddr, "/")
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
		inv, err := clock.Advance.Invoke(r.agentID, remotePrincipal, r.bucketID.DID().String(), nb, delegation.WithProof(r.proof))
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
	pk, err := r.remoteAddr.ID.ExtractPublicKey()
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
	channel := ucanp2p.NewHTTPChannel(r.host, r.remoteAddr, "/")
	conn, err := client.NewConnection(remotePrincipal, channel)
	if err != nil {
		return fmt.Errorf("opening connection: %w", err)
	}

	nb := clock.HeadCaveats{}
	inv, err := clock.Head.Invoke(r.agentID, remotePrincipal, r.bucketID.DID().String(), nb, delegation.WithProof(r.proof))
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
	url := fmt.Sprintf("libp2p://%s", r.remoteAddr.ID)
	fetcher := trustlessgateway.NewClient(url, &client)

	for _, event := range o.Head {
		_, err := r.clock.Advance(ctx, event, WithBlockFetcher(fetcher))
		if err != nil {
			return fmt.Errorf("advancing clock: %w", err)
		}
	}

	return nil
}

// NewRemoteBucket creates a new bucket that stores remote address info.
func NewRemoteBucket(clock Clock, bucket Bucket[ipld.Link]) Bucket[peer.AddrInfo] {
	idbb := NewIdentityBytesBucket(bucket)
	ipbb := NewCborBucket(idbb)
	return NewIpldNodeBucket(ipbb, func(n ipld.Node) (peer.AddrInfo, error) {
		return bindAddrInfo(n)
	}, func(info peer.AddrInfo) (ipld.Node, error) {
		return unbindAddrInfo(info)
	})
}

func bindAddrInfo(n ipld.Node) (peer.AddrInfo, error) {
	var addr peer.AddrInfo
	idn, err := n.LookupByString("id")
	if err != nil {
		return addr, fmt.Errorf("looking up peer id: %w", err)
	}
	idb, err := idn.AsBytes()
	if err != nil {
		return addr, fmt.Errorf("decoding peer ID as bytes: %w", err)
	}
	id, err := peer.IDFromBytes(idb)
	if err != nil {
		return addr, fmt.Errorf("creating peer ID: %w", err)
	}
	addr.ID = id

	addrsn, err := n.LookupByString("addrs")
	if err != nil {
		return addr, fmt.Errorf("looking up peer addresses: %w", err)
	}
	addrs := addrsn.ListIterator()
	if addrs == nil {
		return addr, errors.New("peer addresses is not a list")
	}
	for {
		if addrs.Done() {
			break
		}
		_, n, err := addrs.Next()
		if err != nil {
			return addr, fmt.Errorf("iterating address: %w", err)
		}
		b, err := n.AsBytes()
		if err != nil {
			return addr, fmt.Errorf("decoding multiaddr as bytes: %w", err)
		}
		ma, err := multiaddr.NewMultiaddrBytes(b)
		if err != nil {
			return addr, fmt.Errorf("creating multiaddr: %w", err)
		}
		addr.Addrs = append(addr.Addrs, ma)
	}
	return addr, nil
}

func unbindAddrInfo(addr peer.AddrInfo) (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, err := nb.BeginMap(2)
	if err != nil {
		return nil, fmt.Errorf("beginning map: %w", err)
	}
	idb, err := addr.ID.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshalling peer ID: %w", err)
	}
	err = ma.AssembleKey().AssignString("id")
	if err != nil {
		return nil, fmt.Errorf("assembling peer ID key: %w", err)
	}
	err = ma.AssembleValue().AssignBytes(idb)
	if err != nil {
		return nil, fmt.Errorf("assembling peer ID value: %w", err)
	}
	nb2 := np.NewBuilder()
	la, err := nb2.BeginList(int64(len(addr.Addrs)))
	if err != nil {
		return nil, fmt.Errorf("beginning value list: %w", err)
	}
	for _, a := range addr.Addrs {
		err = la.AssembleValue().AssignBytes(a.Bytes())
		if err != nil {
			return nil, fmt.Errorf("assembling multiaddr bytes: %w", err)
		}
	}
	err = la.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing addresses list: %w", err)
	}
	err = ma.AssembleKey().AssignString("addrs")
	if err != nil {
		return nil, fmt.Errorf("assembling addrs key: %w", err)
	}
	err = ma.AssembleValue().AssignNode(nb2.Build())
	if err != nil {
		return nil, fmt.Errorf("assembling addrs value: %w", err)
	}
	err = ma.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing map: %w", err)
	}
	return nb.Build(), nil
}

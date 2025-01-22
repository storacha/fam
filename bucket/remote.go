package bucket

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type ClockRemote struct {
	clock Clock
	addr  peer.AddrInfo
}

func (r *ClockRemote) Address(ctx context.Context) (peer.AddrInfo, error) {
	return r.addr, nil
}

func (r *ClockRemote) Push(ctx context.Context) error {
	return errors.New("not implemented")
}

func (r *ClockRemote) Pull(ctx context.Context) error {
	return errors.New("not implemented")
}

// NewRemoteBucket creates a new bucket that stores remotes.
func NewRemoteBucket(clock Clock, bucket Bucket[ipld.Link]) Bucket[Remote] {
	idbb := NewIdentityBytesBucket(bucket)
	ipbb := NewCborBucket(idbb)
	return NewIpldNodeBucket(ipbb, func(n ipld.Node) (Remote, error) {
		addr, err := bindAddrInfo(n)
		if err != nil {
			return nil, err
		}
		return &ClockRemote{clock, addr}, nil
	}, func(r Remote) (ipld.Node, error) {
		addr, err := r.Address(context.Background())
		if err != nil {
			return nil, err
		}
		return unbindAddrInfo(addr)
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

	la, err := nb.BeginList(int64(len(addr.Addrs)))
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
	err = ma.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing map: %w", err)
	}
	return nb.Build(), nil
}

package store

import (
	"context"
	"errors"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/storacha/fam/block"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/ucan"
)

var log = logging.Logger("userdata")

var DefaultKeyName = "default"

var (
	DefaultRemoteName = "origin"
	DefaultRemoteID   = "did:key:z6MkjonsDH66hn1zkLH1j7u3NBpsF8NpbpkMFAKtXGgumsyr"
	DefaultRemoteAddr = "/dns/clock.web3.storage/https"
)

type UserDataStore struct {
	id          principal.Signer
	dstore      ds.Datastore
	grants      bucket.Bucket[delegation.Delegation]
	blockstores map[did.DID]block.Blockstore
	buckets     map[did.DID]bucket.Bucket[ipld.Link]
	host        host.Host
}

func (userdata *UserDataStore) AddBucket(ctx context.Context, proof delegation.Delegation) (did.DID, error) {
	bucketID := did.Undef
	var canMutateClock bool
	var canUpload bool
	for _, c := range proof.Capabilities() {
		if bucketID == did.Undef {
			id, err := did.Parse(c.With())
			if err != nil {
				return did.Undef, err
			}
			bucketID = id
		} else {
			id, err := did.Parse(c.With())
			if err != nil {
				return did.Undef, err
			}
			if id != bucketID {
				return did.Undef, errors.New("capabilities do not reference the same resource")
			}
		}

		if c.Can() == "*" {
			canMutateClock = true
			canUpload = true
		}
		if c.Can() == "clock/*" || c.Can() == "clock/advance" {
			canMutateClock = true
		}
		if c.Can() == "space/*" || c.Can() == "space/blob/*" || c.Can() == "space/blob/add" {
			canUpload = true
		}
	}

	if !canMutateClock {
		return did.Undef, errors.New("missing capability to mutate merkle clock")
	}
	if !canUpload {
		return did.Undef, errors.New("missing capability to upload data")
	}

	err := userdata.grants.Put(ctx, bucketID.String(), proof)
	if err != nil {
		return did.Undef, err
	}

	return bucketID, nil
}

func (userdata *UserDataStore) RemoveBucket(ctx context.Context, id did.DID) error {
	err := userdata.grants.Del(ctx, id.String())
	if err != nil {
		return err
	}
	delete(userdata.buckets, id)
	// TODO: clean data
	return nil
}

func (userdata *UserDataStore) ShareBucket(ctx context.Context, bucket did.DID, audience did.DID) (delegation.Delegation, error) {
	proof, err := userdata.grants.Get(ctx, bucket.String())
	if err != nil {
		return nil, err
	}
	return delegation.Delegate(
		userdata.id,
		audience,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("clock/*", bucket.String(), ucan.NoCaveats{}),
			ucan.NewCapability("space/blob/*", bucket.String(), ucan.NoCaveats{}),
		},
		delegation.WithNoExpiration(),
		delegation.WithProof(delegation.FromDelegation(proof)),
	)
}

// Buckets retrieves the list of buckets (and their corresponding delegations).
func (userdata *UserDataStore) Buckets(ctx context.Context) (map[did.DID]delegation.Delegation, error) {
	buckets := map[did.DID]delegation.Delegation{}
	for entry, err := range userdata.grants.Entries(ctx) {
		if err != nil {
			return nil, err
		}
		id, err := did.Parse(entry.Key)
		if err != nil {
			return nil, err
		}
		buckets[id] = entry.Value
	}
	return buckets, nil
}

func (userdata *UserDataStore) Blocks(bucketID did.DID) block.Blockstore {
	bs, ok := userdata.blockstores[bucketID]
	if ok {
		return bs
	}
	pfx := ds.NewKey(fmt.Sprintf("bucket/%s", bucketID.String()))
	bs = block.NewDsBlockstore(namespace.Wrap(userdata.dstore, pfx.ChildString("blocks")))
	userdata.blockstores[bucketID] = bs
	return bs
}

// Bucket retrieves a specific user bucket by it's DID.
func (userdata *UserDataStore) Bucket(ctx context.Context, bucketID did.DID) (bucket.Bucket[ipld.Link], error) {
	if bucket, ok := userdata.buckets[bucketID]; ok {
		return bucket, nil
	}

	agentID := userdata.id

	// ensure it exists
	grant, err := userdata.grants.Get(ctx, bucketID.String())
	if err != nil {
		return nil, err
	}
	// TODO: verify delegation is still valid

	proof := delegation.FromDelegation(grant)

	// TODO: storacha blockstore?
	// TODO: tiered blockstore local, remote

	pfx := ds.NewKey(fmt.Sprintf("bucket/%s", bucketID.String()))
	bk, err := bucket.NewDsClockBucket(
		userdata.Blocks(bucketID),
		namespace.Wrap(userdata.dstore, pfx.ChildString("shards")),
	)
	if err != nil {
		return nil, err
	}

	pfx = pfx.ChildString("remotes")
	rbk, err := bucket.NewDsClockBucket(
		block.NewDsBlockstore(namespace.Wrap(userdata.dstore, pfx.ChildString("blocks"))),
		namespace.Wrap(userdata.dstore, pfx.ChildString("shards")),
	)
	if err != nil {
		return nil, err
	}

	rems := bucket.NewRemoteBucket(bk, rbk)
	_, err = rems.Get(ctx, DefaultRemoteName)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			pcl, err := verifier.Parse(DefaultRemoteID)
			if err != nil {
				return nil, err
			}
			pk, err := crypto.UnmarshalEd25519PublicKey(pcl.Raw())
			if err != nil {
				return nil, err
			}
			id, err := peer.IDFromPublicKey(pk)
			if err != nil {
				return nil, err
			}
			addr, err := multiaddr.NewMultiaddr(DefaultRemoteAddr)
			if err != nil {
				return nil, err
			}
			err = rems.Put(ctx, DefaultRemoteName, peer.AddrInfo{
				ID:    id,
				Addrs: []multiaddr.Multiaddr{addr},
			})
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	nbk, err := bucket.NewNetworkClockBucket(agentID, bucketID, proof, bk, userdata.host, rems)
	if err != nil {
		return nil, err
	}

	userdata.buckets[bucketID] = nbk
	return nbk, nil
}

func (userdata *UserDataStore) Close() error {
	herr := userdata.host.Close()
	derr := userdata.dstore.Close()
	return errors.Join(herr, derr)
}

func NewUserDataStore(ctx context.Context, id principal.Signer, dstore ds.Datastore, host host.Host) (*UserDataStore, error) {
	log.Debugln("creating grants bucket...")
	grantshards, err := bucket.NewDsClockBucket(
		block.NewDsBlockstore(namespace.Wrap(dstore, ds.NewKey("grants/blocks/"))),
		namespace.Wrap(dstore, ds.NewKey("grants/shards/")),
	)
	if err != nil {
		return nil, err
	}
	grants := bucket.NewDelegationBucket(grantshards)

	blockstores := map[did.DID]block.Blockstore{}
	buckets := map[did.DID]bucket.Bucket[ipld.Link]{}

	return &UserDataStore{
		id:          id,
		dstore:      dstore,
		grants:      grants,
		blockstores: blockstores,
		buckets:     buckets,
		host:        host,
	}, nil
}

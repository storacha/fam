package store

import (
	"context"
	"errors"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

var log = logging.Logger("userdata")

var DefaultKey = "default"

type UserDataStore struct {
	dstore  ds.Datastore
	keys    bucket.Bucket[principal.Signer]
	proofs  bucket.Bucket[delegation.Delegation]
	buckets map[did.DID]bucket.Bucket[ipld.Link]
}

// ID retrieves the named private key (signer) of the agent.
func (userdata *UserDataStore) ID(ctx context.Context) (principal.Signer, error) {
	return userdata.keys.Get(ctx, DefaultKey)
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

	err := userdata.proofs.Put(ctx, bucketID.String(), proof)
	if err != nil {
		return did.Undef, err
	}

	return bucketID, nil
}

func (userdata *UserDataStore) RemoveBucket(ctx context.Context, id did.DID) error {
	err := userdata.proofs.Del(ctx, id.String())
	if err != nil {
		return err
	}
	delete(userdata.buckets, id)
	return nil
}

// Buckets retrieves the list of buckets (and their corresponding delegations).
func (userdata *UserDataStore) Buckets(ctx context.Context) (map[did.DID]delegation.Delegation, error) {
	buckets := map[did.DID]delegation.Delegation{}
	for entry, err := range userdata.proofs.Entries(ctx) {
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

// Bucket retrieves a specific user bucket by it's DID.
func (userdata *UserDataStore) Bucket(ctx context.Context, id did.DID) (bucket.Bucket[ipld.Link], error) {
	if bucket, ok := userdata.buckets[id]; ok {
		return bucket, nil
	}
	// ensure it exists
	if _, err := userdata.proofs.Get(ctx, id.String()); err != nil {
		return nil, err
	}
	// TODO: verify delegation is still valid

	dstore := namespace.Wrap(userdata.dstore, ds.NewKey(fmt.Sprintf("bucket/%s/", id.String())))
	bucket, err := bucket.NewDsBucket(dstore)
	if err != nil {
		return nil, err
	}

	userdata.buckets[id] = bucket
	return bucket, nil
}

func (userdata *UserDataStore) Close() error {
	return userdata.dstore.Close()
}

func NewUserDataStore(ctx context.Context, dstore ds.Datastore) (*UserDataStore, error) {
	log.Debugln("creating key bucket...")
	keys, err := bucket.NewKeyBucket(namespace.Wrap(dstore, ds.NewKey("keys/")))
	if err != nil {
		return nil, err
	}

	id, err := keys.Get(ctx, DefaultKey)
	if errors.Is(err, bucket.ErrNotFound) {
		log.Warnln("default signing key not found, generating a new ed25519 key")

		id, err = ed25519.Generate()
		if err != nil {
			return nil, err
		}

		err = keys.Put(ctx, DefaultKey, id)
		if err != nil {
			return nil, err
		}
	}
	log.Infof("agent ID: %s", id.DID().String())

	log.Debugln("creating proofs bucket...")
	grants, err := bucket.NewDelegationBucket(namespace.Wrap(dstore, ds.NewKey("proofs/")))
	if err != nil {
		return nil, err
	}

	return &UserDataStore{dstore, keys, grants, map[did.DID]bucket.Bucket[ipld.Link]{}}, nil
}

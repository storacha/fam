package bucket

import (
	"context"
	"iter"

	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/fam/remote"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
)

type NetworkClockBucket[T any] struct {
	agentID  principal.Signer
	bucketID ucan.Principal
	proof    delegation.Proof
	bucket   ClockBucket[T]
	remotes  Bucket[peer.AddrInfo]
	host     host.Host
}

func (cb *NetworkClockBucket[T]) Remotes(ctx context.Context) (Bucket[peer.AddrInfo], error) {
	return cb.remotes, nil
}

func (cb *NetworkClockBucket[T]) Remote(ctx context.Context, name string) (remote.Remote, error) {
	remotes, err := cb.Remotes(ctx)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := remotes.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return remote.NewClockRemote(cb.agentID, cb.bucketID, cb.proof, cb.bucket, remoteAddr, cb.host), nil
}

func (cb *NetworkClockBucket[T]) Head(ctx context.Context) ([]ipld.Link, error) {
	return cb.bucket.Head(ctx)
}

func (cb *NetworkClockBucket[T]) Advance(ctx context.Context, evt ipld.Link) ([]ipld.Link, error) {
	return cb.bucket.Advance(ctx, evt)
}

func (cb *NetworkClockBucket[T]) Root(ctx context.Context) (ipld.Link, error) {
	return cb.bucket.Root(ctx)
}

func (cb *NetworkClockBucket[T]) Get(ctx context.Context, key string) (T, error) {
	return cb.bucket.Get(ctx, key)
}

func (cb *NetworkClockBucket[T]) Put(ctx context.Context, key string, value T) error {
	return cb.bucket.Put(ctx, key, value)
}

func (cb *NetworkClockBucket[T]) Del(ctx context.Context, key string) error {
	return cb.bucket.Del(ctx, key)
}

func (cb *NetworkClockBucket[T]) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error] {
	return cb.bucket.Entries(ctx, opts...)
}

// NewNetworkClockBucket creates a new [ClockBucket[T]] that is also a [Networker].
func NewNetworkClockBucket[T any](agentID principal.Signer, bucketID ucan.Principal, proof delegation.Proof, bucket ClockBucket[T], host host.Host, remotes Bucket[peer.AddrInfo]) (*NetworkClockBucket[T], error) {
	return &NetworkClockBucket[T]{agentID, bucketID, proof, bucket, remotes, host}, nil
}

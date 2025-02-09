package clock

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/fam/block"
	"github.com/storacha/go-ucanto/did"
)

// Clock is a merkle clock.
type Clock interface {
	Head(ctx context.Context) ([]ipld.Link, error)
	Advance(ctx context.Context, event ipld.Link, opts ...AdvanceOption) ([]ipld.Link, error)
}

type AdvanceOption func(*AdvanceOptions)

type AdvanceOptions struct {
	Fetcher block.Fetcher
}

func WithBlockFetcher(f block.Fetcher) AdvanceOption {
	return func(opts *AdvanceOptions) {
		opts.Fetcher = f
	}
}

type ClockStore interface {
	// Get retrieves a clock by it's ID.
	Get(context.Context, did.DID) (Clock, error)
}

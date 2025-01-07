package bucket

import (
	"context"
	"iter"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail"
)

var ErrNotFound = pail.ErrNotFound

type EntriesOption = pail.EntriesOption
type Entry[T any] struct {
	Key   string
	Value T
}

var (
	WithKeyPrefix             = pail.WithKeyPrefix
	WithKeyGreaterThan        = pail.WithKeyGreaterThan
	WithKeyGreaterThanOrEqual = pail.WithKeyGreaterThanOrEqual
	WithKeyLessThan           = pail.WithKeyLessThan
	WithKeyLessThanOrEqual    = pail.WithKeyLessThanOrEqual
)

type Bucket[T any] interface {
	// Root returns the current root CID of the bucket.
	Root() ipld.Link
	Get(ctx context.Context, key string) (T, error)
	Put(ctx context.Context, key string, value T) error
	Del(ctx context.Context, key string) error
	Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[T], error]
}

package bucket

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

var log = logging.Logger("datastore")

var rootKey = datastore.NewKey("root")

type DsBlockFetcher struct {
	dstore datastore.Datastore
}

func (f *DsBlockFetcher) Get(ctx context.Context, link ipld.Link) (block.Block, error) {
	b, err := f.dstore.Get(ctx, datastore.NewKey(link.String()))
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, fmt.Errorf("getting block: %s: %w", link, ErrNotFound)
		}
		return nil, fmt.Errorf("getting block: %s: %w", link, err)
	}
	return block.New(link, b), nil
}

func NewDsBlockFetcher(dstore datastore.Datastore) *DsBlockFetcher {
	return &DsBlockFetcher{dstore}
}

type DsBucket struct {
	mutex  sync.RWMutex
	root   ipld.Link
	dstore datastore.Datastore
	blocks block.Fetcher
}

func (bucket *DsBucket) Root() ipld.Link {
	return bucket.root
}

func (bucket *DsBucket) Put(ctx context.Context, key string, value ipld.Link) error {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	r, diff, err := pail.Put(ctx, bucket.blocks, bucket.root, key, value)
	if err != nil {
		return fmt.Errorf("putting %s: %w", key, err)
	}

	for _, b := range diff.Additions {
		log.Debugf("putting put diff addition: %s", b.Link())
		err = bucket.dstore.Put(ctx, datastore.NewKey(b.Link().String()), b.Bytes())
		if err != nil {
			return fmt.Errorf("putting diff addition: %w", err)
		}
	}

	err = bucket.dstore.Put(context.Background(), rootKey, []byte(r.Binary()))
	if err != nil {
		return fmt.Errorf("updating root: %w", err)
	}
	bucket.root = r

	for _, b := range diff.Removals {
		log.Debugf("deleting put diff removal: %s", b.Link())
		err = bucket.dstore.Delete(ctx, datastore.NewKey(b.Link().String()))
		if err != nil {
			return fmt.Errorf("deleting diff removal: %w", err)
		}
	}

	return nil
}

func (bucket *DsBucket) Get(ctx context.Context, key string) (ipld.Link, error) {
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()

	value, err := pail.Get(ctx, bucket.blocks, bucket.root, key)
	if err != nil {
		return nil, fmt.Errorf("getting %s: %w", key, err)
	}
	return value, nil
}

func (bucket *DsBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[ipld.Link], error] {
	return func(yield func(Entry[ipld.Link], error) bool) {
		bucket.mutex.RLock()
		defer bucket.mutex.RUnlock()

		for e, err := range pail.Entries(ctx, bucket.blocks, bucket.root, opts...) {
			if err != nil {
				yield(Entry[ipld.Link]{}, err)
				return
			}
			if !yield(Entry[ipld.Link]{e.Key, e.Value}, nil) {
				return
			}
		}
	}
}

func (bucket *DsBucket) Del(ctx context.Context, key string) error {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	r, diff, err := pail.Del(ctx, bucket.blocks, bucket.root, key)
	if err != nil {
		return fmt.Errorf("deleting %s: %w", key, err)
	}

	for _, b := range diff.Additions {
		log.Debugf("putting delete diff addition: %s", b.Link())
		err = bucket.dstore.Put(ctx, datastore.NewKey(b.Link().String()), b.Bytes())
		if err != nil {
			return fmt.Errorf("putting diff addition: %w", err)
		}
	}

	err = bucket.dstore.Put(context.Background(), rootKey, []byte(r.Binary()))
	if err != nil {
		return fmt.Errorf("updating root: %w", err)
	}
	bucket.root = r

	for _, b := range diff.Removals {
		log.Debugf("deleting delete diff removal: %s", b.Link())
		err = bucket.dstore.Delete(ctx, datastore.NewKey(b.Link().String()))
		if err != nil {
			return fmt.Errorf("deleting diff removal: %w", err)
		}
	}

	return nil
}

func NewDsBucket(dstore datastore.Datastore) (*DsBucket, error) {
	var root ipld.Link
	b, err := dstore.Get(context.Background(), rootKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			log.Warnln("bucket root not found, creating new bucket...")

			rs := shard.NewRoot(nil)
			rb, err := shard.MarshalBlock(rs)
			if err != nil {
				return nil, fmt.Errorf("marshalling pail root: %w", err)
			}
			err = dstore.Put(context.Background(), datastore.NewKey(rb.Link().String()), rb.Bytes())
			if err != nil {
				return nil, fmt.Errorf("putting pail root block: %w", err)
			}
			err = dstore.Put(context.Background(), rootKey, []byte(rb.Link().Binary()))
			if err != nil {
				return nil, fmt.Errorf("putting pail root: %w", err)
			}
			root = rb.Link()
		} else {
			return nil, fmt.Errorf("getting root: %w", err)
		}
	} else {
		c, err := cid.Cast(b)
		if err != nil {
			return nil, fmt.Errorf("decoding root: %w", err)
		}
		root = cidlink.Link{Cid: c}
	}
	log.Debugf("loading bucket with root: %s", root)
	blocks := NewDsBlockFetcher(dstore)
	return &DsBucket{root: root, dstore: dstore, blocks: blocks}, nil
}

package bucket

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/fam/bucket/head"
	pail "github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/clock"
	"github.com/storacha/go-pail/crdt"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/ipld/node"
)

var log = logging.Logger("datastore")

var headKey = datastore.NewKey("head")

type DsBlockstore struct {
	data datastore.Datastore
}

func (bs *DsBlockstore) Get(ctx context.Context, link ipld.Link) (block.Block, error) {
	b, err := bs.data.Get(ctx, datastore.NewKey(link.String()))
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, fmt.Errorf("getting block: %s: %w", link, ErrNotFound)
		}
		return nil, fmt.Errorf("getting block: %s: %w", link, err)
	}
	return block.New(link, b), nil
}

func (bs *DsBlockstore) Put(ctx context.Context, block block.Block) error {
	err := bs.data.Put(ctx, datastore.NewKey(block.Link().String()), block.Bytes())
	if err != nil {
		return fmt.Errorf("putting block: %w", err)
	}
	return nil
}

func (bs *DsBlockstore) PutBatch(ctx context.Context, blocks []block.Block) error {
	if bds, ok := bs.data.(datastore.Batching); ok {
		batch, err := bds.Batch(ctx)
		if err != nil {
			return fmt.Errorf("creating batch: %w", err)
		}
		for _, b := range blocks {
			err := batch.Put(ctx, datastore.NewKey(b.Link().String()), b.Bytes())
			if err != nil {
				return err
			}
		}
		err = batch.Commit(ctx)
		if err != nil {
			return fmt.Errorf("comitting batch: %w", err)
		}
	} else {
		for _, b := range blocks {
			err := bs.Put(ctx, b)
			if err != nil {
				return fmt.Errorf("putting block: %w", err)
			}
		}
	}
	return nil
}

func (bs *DsBlockstore) Del(ctx context.Context, link ipld.Link) error {
	err := bs.data.Delete(ctx, datastore.NewKey(link.String()))
	if err != nil {
		return fmt.Errorf("deleting block: %w", err)
	}
	return nil
}

func NewDsBlockstore(dstore datastore.Datastore) *DsBlockstore {
	return &DsBlockstore{dstore}
}

type DsClockBucket struct {
	mutex  sync.RWMutex
	head   []ipld.Link
	data   datastore.Datastore
	blocks Blockstore
}

func (bucket *DsClockBucket) Head(ctx context.Context) ([]ipld.Link, error) {
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()
	return bucket.head, nil
}

func (bucket *DsClockBucket) Advance(ctx context.Context, evt block.Block) ([]ipld.Link, error) {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	for _, l := range bucket.head {
		if l == evt.Link() {
			return bucket.head, nil
		}
	}

	mblocks := block.NewMapBlockstore()
	_ = mblocks.Put(ctx, evt)

	hd, err := clock.Advance(ctx, block.NewTieredBlockFetcher(mblocks, bucket.blocks), node.BinderFunc[operation.Operation](operation.Bind), bucket.head, evt.Link())
	if err != nil {
		return nil, fmt.Errorf("advancing merkle clock: %w", err)
	}

	// permanently write the new event block
	err = bucket.blocks.Put(ctx, evt)
	if err != nil {
		return nil, fmt.Errorf("putting merkle clock event: %w", err)
	}

	hbytes, err := head.Marshal(hd)
	if err != nil {
		return nil, fmt.Errorf("marshalling head: %w", err)
	}

	err = bucket.data.Put(ctx, headKey, hbytes)
	if err != nil {
		return nil, fmt.Errorf("updating head: %w", err)
	}

	bucket.head = hd
	return hd, nil
}

func (bucket *DsClockBucket) Root(ctx context.Context) (ipld.Link, error) {
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()

	if len(bucket.head) == 0 {
		b, err := pail.New()
		if err != nil {
			return nil, fmt.Errorf("creating pail: %w", err)
		}
		return b.Link(), nil
	}

	root, _, err := crdt.Root(ctx, bucket.blocks, bucket.head)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func (bucket *DsClockBucket) Put(ctx context.Context, key string, value ipld.Link) error {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	res, err := crdt.Put(ctx, bucket.blocks, bucket.head, key, value)
	if err != nil {
		return fmt.Errorf("putting %s: %w", key, err)
	}

	var additions []block.Block
	if res.Event != nil {
		additions = append(additions, res.Event)
	}
	for _, b := range res.Additions {
		additions = append(additions, b)
	}
	err = bucket.blocks.PutBatch(ctx, additions)
	if err != nil {
		return fmt.Errorf("putting diff addition: %w", err)
	}

	hbytes, err := head.Marshal(res.Head)
	if err != nil {
		return fmt.Errorf("marshalling head: %w", err)
	}

	err = bucket.data.Put(ctx, headKey, hbytes)
	if err != nil {
		return fmt.Errorf("updating head: %w", err)
	}
	bucket.head = res.Head

	for _, b := range res.Removals {
		log.Debugf("deleting put diff removal: %s", b.Link())
		err = bucket.blocks.Del(ctx, b.Link())
		if err != nil {
			return fmt.Errorf("deleting diff removal: %w", err)
		}
	}

	return nil
}

func (bucket *DsClockBucket) Get(ctx context.Context, key string) (ipld.Link, error) {
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()

	value, err := crdt.Get(ctx, bucket.blocks, bucket.head, key)
	if err != nil {
		return nil, fmt.Errorf("getting %s: %w", key, err)
	}
	return value, nil
}

func (bucket *DsClockBucket) Entries(ctx context.Context, opts ...EntriesOption) iter.Seq2[Entry[ipld.Link], error] {
	return func(yield func(Entry[ipld.Link], error) bool) {
		bucket.mutex.RLock()
		defer bucket.mutex.RUnlock()

		for e, err := range crdt.Entries(ctx, bucket.blocks, bucket.head, opts...) {
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

func (bucket *DsClockBucket) Del(ctx context.Context, key string) error {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	res, err := crdt.Del(ctx, bucket.blocks, bucket.head, key)
	if err != nil {
		return fmt.Errorf("deleting %s: %w", key, err)
	}

	var additions []block.Block
	if res.Event != nil {
		additions = append(additions, res.Event)
	}
	for _, b := range res.Additions {
		additions = append(additions, b)
	}
	err = bucket.blocks.PutBatch(ctx, additions)
	if err != nil {
		return fmt.Errorf("putting diff addition: %w", err)
	}

	hbytes, err := head.Marshal(res.Head)
	if err != nil {
		return fmt.Errorf("marshalling head: %w", err)
	}

	err = bucket.data.Put(ctx, headKey, hbytes)
	if err != nil {
		return fmt.Errorf("updating head: %w", err)
	}
	bucket.head = res.Head

	for _, b := range res.Removals {
		log.Debugf("deleting delete diff removal: %s", b.Link())
		err = bucket.blocks.Del(ctx, b.Link())
		if err != nil {
			return fmt.Errorf("deleting diff removal: %w", err)
		}
	}

	return nil
}

func NewDsClockBucket(blocks Blockstore, dstore datastore.Datastore) (*DsClockBucket, error) {
	var hd []ipld.Link
	b, err := dstore.Get(context.Background(), headKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			log.Warnln("bucket head not found, creating new bucket...")
		} else {
			return nil, fmt.Errorf("getting root: %w", err)
		}
	} else {
		hd, err = head.Unmarshal(b)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling head: %w", err)
		}
	}
	log.Debugf("loading bucket with head: %s", hd)
	return &DsClockBucket{head: hd, data: dstore, blocks: blocks}, nil
}

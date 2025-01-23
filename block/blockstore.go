package block

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
)

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

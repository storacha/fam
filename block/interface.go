package block

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
)

var ErrNotFound = pail.ErrNotFound

type Block = block.Block
type Fetcher = block.Fetcher

var NewMapBlockstore = block.NewMapBlockstore
var NewTieredBlockFetcher = block.NewTieredBlockFetcher

type Putter interface {
	Put(ctx context.Context, block Block) error
}

type Blockstore interface {
	block.Fetcher
	Putter
	PutBatch(ctx context.Context, blocks []Block) error
	Del(ctx context.Context, link ipld.Link) error
}

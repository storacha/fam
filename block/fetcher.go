package block

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-pail/block"
)

var log = logging.Logger("block")

type TrustlessGatewayBlockFetcher struct {
	endpoint string
	client   *http.Client
}

func (tf *TrustlessGatewayBlockFetcher) Get(ctx context.Context, link ipld.Link) (Block, error) {
	reqDigest, err := ToDigest(link)
	if err != nil {
		return nil, fmt.Errorf("extracting digest from link: %w", err)
	}

	url, err := url.JoinPath(tf.endpoint, "ipfs", link.String())
	if err != nil {
		return nil, fmt.Errorf("constructing URL: %w", err)
	}

	log.Debugf("fetching block: %s", url)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ipld.raw")

	res, err := tf.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	resDigest, err := multihash.Sum(body, multihash.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing block: %w", err)
	}

	if !bytes.Equal(reqDigest, resDigest) {
		return nil, fmt.Errorf("consistency check failure: z%s != z%s", reqDigest.B58String(), resDigest.B58String())
	}

	return block.New(link, body), nil
}

func NewTrustlessGatewayBlockFetcher(endpoint string, client *http.Client) *TrustlessGatewayBlockFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &TrustlessGatewayBlockFetcher{endpoint, client}
}

func ToDigest(link ipld.Link) (multihash.Multihash, error) {
	c, err := cid.Parse(link.String())
	if err != nil {
		return nil, fmt.Errorf("decoding CID: %w", err)
	}
	return c.Hash(), nil
}

type CachingBlockFetcher struct {
	fetcher Fetcher
	cache   Putter
}

func (cf *CachingBlockFetcher) Get(ctx context.Context, link ipld.Link) (Block, error) {
	b, err := cf.fetcher.Get(ctx, link)
	if err != nil {
		return nil, err
	}
	err = cf.cache.Put(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("caching block: %w", err)
	}
	return b, nil
}

// NewCachingFetcher creates a block fetcher that writes every block it fetches
// to a cache.
func NewCachingFetcher(fetcher Fetcher, cache Putter) Fetcher {
	return &CachingBlockFetcher{fetcher, cache}
}

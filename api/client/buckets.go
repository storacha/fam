package client

import (
	"context"
	"fmt"

	buckets "github.com/storacha/fam/buckets/capabilities"
	"github.com/storacha/fam/capabilities/util"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

func (c *Client) Buckets(ctx context.Context) ([]did.DID, error) {
	proofs, err := c.Proofs(buckets.ListAbility)
	if err != nil {
		return nil, err
	}
	inv, err := buckets.List.Invoke(
		c.signer,
		c.conn.ID(),
		c.signer.DID().String(),
		buckets.ListCaveats{},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, err
	}
	res, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	if err != nil {
		return nil, fmt.Errorf("executing %s invocation: %w", buckets.ListAbility, err)
	}
	rcptLink, ok := res.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("missing receipt for invocation: %s", inv.Link())
	}
	rcptReader, err := buckets.NewListReceiptReader()
	if err != nil {
		return nil, err
	}
	rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}
	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("invocation failure: %+v", f)
	}
	var ids []did.DID
	for _, r := range o.Results {
		ids = append(ids, r.ID)
	}
	return ids, nil
}

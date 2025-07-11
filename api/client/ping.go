package client

import (
	"context"
	"fmt"
	"time"

	"github.com/storacha/fam/capabilities/util"
	ping "github.com/storacha/fam/ping/capabilities"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/result"
)

// Ping the service sending the specified message. Returns the RTT.
func (c *Client) Ping(ctx context.Context, msg string) (time.Duration, error) {
	now := time.Now()
	proofs, err := c.Proofs(ping.SendAbility)
	if err != nil {
		return time.Since(now), err
	}
	inv, err := ping.Send.Invoke(
		c.signer,
		c.conn.ID(),
		c.signer.DID().String(),
		ping.SendCaveats{Message: msg},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return time.Since(now), err
	}
	res, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	rtt := time.Since(now)
	if err != nil {
		return rtt, fmt.Errorf("executing %s invocation: %w", ping.SendAbility, err)
	}
	rcptLink, ok := res.Get(inv.Link())
	if !ok {
		return rtt, fmt.Errorf("missing receipt for invocation: %s", inv.Link())
	}
	rcptReader, err := ping.NewSendReceiptReader()
	if err != nil {
		return rtt, err
	}
	rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
	if err != nil {
		return rtt, fmt.Errorf("reading receipt: %w", err)
	}
	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return rtt, err
		}
		return rtt, fmt.Errorf("invocation failure: %+v", f)
	}
	if o.Message != msg {
		return rtt, fmt.Errorf("pong message mismatch, expected: %s, received: %s", msg, o.Message)
	}
	return rtt, nil
}

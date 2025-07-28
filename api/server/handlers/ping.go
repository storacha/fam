package handlers

import (
	"context"

	ping "github.com/storacha/fam/ping/capabilities"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
)

func Ping() []server.Option {
	return []server.Option{
		server.WithServiceMethod(
			ping.SendAbility,
			server.Provide(ping.Send, func(ctx context.Context, cap ucan.Capability[ping.SendCaveats], inv invocation.Invocation, iCtx server.InvocationContext) (result.Result[ping.SendOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				return result.Ok[ping.SendOk, failure.IPLDBuilderFailure](ping.SendOk{Message: cap.Nb().Message}), nil, nil
			}),
		),
	}
}

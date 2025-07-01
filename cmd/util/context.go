package util

import (
	"context"
	"fmt"

	"github.com/storacha/fam/api"
	famclient "github.com/storacha/fam/api/client"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/urfave/cli/v2"
)

type Context struct {
	Context    context.Context
	DataDir    string
	ID         principal.Signer
	Connection client.Connection
	Client     *famclient.Client
}

func WithContext(ctx context.Context, h func(cctx *cli.Context, fctx *Context) error) func(cctx *cli.Context) error {
	return func(cctx *cli.Context) error {
		datadir := EnsureDataDir(cctx.String("datadir"))
		srv, err := api.New(datadir)
		if err != nil {
			log.Fatal(err)
		}
		id, err := AgentSigner(AgentPrivateKeyPath(datadir))
		if err != nil {
			log.Fatal(err)
		}
		conn, err := srv.Connect(ctx)
		if err != nil {
			log.Fatal(err)
		}
		serviceID, err := ServiceSigner(ServicePrivateKeyPath(datadir))
		if err != nil {
			log.Fatal(err)
		}
		getProofs := func(can ucan.Ability) ([]delegation.Proof, error) {
			dlg, err := delegation.Delegate(
				serviceID,
				id,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(can, serviceID.DID().String(), ucan.NoCaveats{}),
				},
			)
			if err != nil {
				return nil, fmt.Errorf("delegating capabilities: %w", err)
			}
			return []delegation.Proof{delegation.FromDelegation(dlg)}, nil
		}
		client := famclient.New(id, conn, getProofs)

		return h(cctx, &Context{ctx, datadir, id, conn, client})
	}
}

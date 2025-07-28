package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/storacha/fam/cmd/util"
	"github.com/urfave/cli/v2"
)

func Ping(ctx context.Context) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "ping",
			Usage: "Ping the service",
			Action: util.WithContext(ctx, func(cctx *cli.Context, fctx *util.Context) error {
				msg := time.Now().Format(time.RFC3339)
				fmt.Printf("PING %s: %d data bytes\n", fctx.Connection.ID().DID(), len(msg))
				rtt, err := fctx.Client.Ping(ctx, msg)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("%d bytes from %s: time=%s\n", len(msg), fctx.Connection.ID().DID(), rtt.String())
				return nil
			}),
		},
	}
}

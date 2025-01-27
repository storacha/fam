package main

import (
	"context"
	"fmt"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/fam/cmd/bucket"
	"github.com/storacha/fam/cmd/util"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("cmd")

func main() {
	logging.SetLogLevel("*", "error")

	app := &cli.App{
		Name:  "fam",
		Usage: "Manage a family size chicken bucket.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "datadir",
				Aliases: []string{"d"},
				Usage:   "path to store application data",
				EnvVars: []string{"FAM_DATA_DIR"},
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "whodis",
				Usage: "Print your agent DID",
				Action: func(cCtx *cli.Context) error {
					datadir := util.EnsureDataDir(cCtx.String("datadir"))
					userdata := util.UserDataStore(context.Background(), datadir)
					id, err := userdata.ID(context.Background())
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(id.DID().String())
					return nil
				},
			},
			bucket.Command,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

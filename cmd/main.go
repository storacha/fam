package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	logging "github.com/ipfs/go-log/v2"
	fbucket "github.com/storacha/fam/bucket"
	"github.com/storacha/fam/cmd/bucket"
	"github.com/storacha/fam/cmd/bucket/remote"
	"github.com/storacha/fam/cmd/util"
	"github.com/storacha/fam/store"
	"github.com/storacha/go-ucanto/did"
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
			{
				Name:      "pull",
				Usage:     "Pull changes from a remote",
				Args:      true,
				ArgsUsage: "[remote]",
				Action: func(cCtx *cli.Context) error {
					datadir := util.EnsureDataDir(cCtx.String("datadir"))
					userdata := util.UserDataStore(context.Background(), datadir)
					curr := util.GetCurrent(datadir)
					if curr == did.Undef {
						return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
					}
					bk, err := userdata.Bucket(context.Background(), curr)
					if err != nil {
						log.Fatal(err)
					}
					if nbk, ok := bk.(fbucket.Networker); ok {
						name := cCtx.Args().Get(0)
						if name == "" {
							name = store.DefaultRemoteName
						}
						remote, err := nbk.Remote(context.Background(), name)
						if err != nil {
							if errors.Is(err, fbucket.ErrNotFound) {
								return fmt.Errorf("remote not found: %s", name)
							}
							log.Fatal(err)
						}
						err = remote.Pull(context.Background())
						if err != nil {
							log.Fatal(err)
						}
						root, err := bk.Root(context.Background())
						if err != nil {
							log.Fatal(err)
						}
						fmt.Println(root.String())
					} else {
						return fmt.Errorf("bucket is not a networker")
					}
					return nil
				},
			},
			{
				Name:      "push",
				Usage:     "Push local changes to a remote",
				Args:      true,
				ArgsUsage: "[remote]",
				Action: func(cCtx *cli.Context) error {
					datadir := util.EnsureDataDir(cCtx.String("datadir"))
					userdata := util.UserDataStore(context.Background(), datadir)
					curr := util.GetCurrent(datadir)
					if curr == did.Undef {
						return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
					}
					bk, err := userdata.Bucket(context.Background(), curr)
					if err != nil {
						log.Fatal(err)
					}
					if nbk, ok := bk.(fbucket.Networker); ok {
						name := cCtx.Args().Get(0)
						if name == "" {
							name = store.DefaultRemoteName
						}
						remote, err := nbk.Remote(context.Background(), name)
						if err != nil {
							if errors.Is(err, fbucket.ErrNotFound) {
								return fmt.Errorf("remote not found: %s", name)
							}
							log.Fatal(err)
						}
						err = remote.Push(context.Background())
						if err != nil {
							log.Fatal(err)
						}
						root, err := bk.Root(context.Background())
						if err != nil {
							log.Fatal(err)
						}
						fmt.Println(root.String())
					} else {
						return fmt.Errorf("bucket is not a networker")
					}
					return nil
				},
			},
			remote.Command,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

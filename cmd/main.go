package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	fbucket "github.com/storacha/fam/bucket"
	"github.com/storacha/fam/cmd/bucket"
	"github.com/storacha/fam/cmd/remote"
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
				Name:      "del",
				Aliases:   []string{"delete"},
				Usage:     "Delete an entry from a bucket",
				Args:      true,
				ArgsUsage: "<key>",
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
					key := cCtx.Args().Get(0)
					if key == "" {
						return fmt.Errorf("missing key")
					}
					err = bk.Del(context.Background(), key)
					if err != nil {
						log.Fatal(err)
					}
					root, err := bk.Root(context.Background())
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(root.String())
					return nil
				},
			},
			{
				Name:    "ls",
				Aliases: []string{"list"},
				Usage:   "List bucket entries",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "pfx",
						Aliases: []string{"p"},
						Usage:   "filter entries by key prefix",
					},
					&cli.StringFlag{
						Name:  "gt",
						Usage: "filter entries by key greater than",
					},
					&cli.StringFlag{
						Name:  "gte",
						Usage: "filter entries by key greater than or equal",
					},
					&cli.StringFlag{
						Name:  "lt",
						Usage: "filter entries by key greater than",
					},
					&cli.StringFlag{
						Name:  "lte",
						Usage: "filter entries by key less than or equal",
					},
					&cli.IntFlag{
						Name:    "limit",
						Aliases: []string{"l"},
						Usage:   "limit the number of entries printed",
					},
				},
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
					opts := []fbucket.EntriesOption{}
					if cCtx.String("pfx") != "" {
						opts = append(opts, fbucket.WithKeyPrefix(cCtx.String("pfx")))
					}
					if cCtx.String("gt") != "" {
						opts = append(opts, fbucket.WithKeyGreaterThan(cCtx.String("gt")))
					}
					if cCtx.String("gte") != "" {
						opts = append(opts, fbucket.WithKeyGreaterThanOrEqual(cCtx.String("gte")))
					}
					if cCtx.String("lt") != "" {
						opts = append(opts, fbucket.WithKeyLessThan(cCtx.String("lt")))
					}
					if cCtx.String("lte") != "" {
						opts = append(opts, fbucket.WithKeyLessThanOrEqual(cCtx.String("lte")))
					}
					count := 0
					limit := cCtx.Int("limit")
					for entry, err := range bk.Entries(context.Background(), opts...) {
						if err != nil {
							log.Fatal(err)
						}
						fmt.Printf("%s\t%s\n", entry.Key, entry.Value)
						count++
						if limit > 0 && count >= limit {
							break
						}
					}
					fmt.Printf("%d total\n", count)
					return nil
				},
			},
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
			{
				Name:      "put",
				Usage:     "Put a value to the bucket",
				Args:      true,
				ArgsUsage: "<key> <value>",
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
					key := cCtx.Args().Get(0)
					if key == "" {
						return fmt.Errorf("missing key")
					}
					value, err := cid.Parse(cCtx.Args().Get(1))
					if err != nil {
						return fmt.Errorf("invalid value: %w", err)
					}
					err = bk.Put(context.Background(), key, cidlink.Link{Cid: value})
					if err != nil {
						log.Fatal(err)
					}
					root, err := bk.Root(context.Background())
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(root.String())
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

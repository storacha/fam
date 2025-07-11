package bucket

import (
	"context"
	"errors"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/fam/cmd/util"
	"github.com/storacha/go-ucanto/did"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("bucket")

func Commands(ctx context.Context) []*cli.Command {
	return []*cli.Command{
		{
			Name:      "ls",
			Aliases:   []string{"list", "entries"},
			Usage:     "List bucket entries",
			Args:      true,
			ArgsUsage: "<id>",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "prefix",
					Aliases: []string{"p"},
					Usage:   "filter entries by key prefix",
				},
				&cli.StringFlag{
					Name:    "greater-than",
					Aliases: []string{"gt"},
					Usage:   "filter entries by key greater than",
				},
				&cli.StringFlag{
					Name:    "greater-than-equal",
					Aliases: []string{"gte"},
					Usage:   "filter entries by key greater than or equal",
				},
				&cli.StringFlag{
					Name:    "less-than",
					Aliases: []string{"lt"},
					Usage:   "filter entries by key greater than",
				},
				&cli.StringFlag{
					Name:    "less-than-equal",
					Aliases: []string{"lte"},
					Usage:   "filter entries by key less than or equal",
				},
				&cli.IntFlag{
					Name:    "limit",
					Aliases: []string{"l"},
					Usage:   "limit the number of entries printed",
				},
			},
			Action: util.WithContext(ctx, func(cctx *cli.Context, fctx *util.Context) error {
				if cctx.Args().First() == "" {
					log.Fatal(errors.New("missing bucket DID"))
				}
				id, err := did.Parse(cctx.Args().First())
				if err != nil {
					log.Fatal(err)
				}
				bk, err := fctx.Client.Bucket(ctx, id)
				if err != nil {
					log.Fatal(err)
				}

				opts := []bucket.EntriesOption{}
				if cctx.String("pfx") != "" {
					opts = append(opts, bucket.WithKeyPrefix(cctx.String("prefix")))
				}
				if cctx.String("gt") != "" {
					opts = append(opts, bucket.WithKeyGreaterThan(cctx.String("greater-than")))
				}
				if cctx.String("gte") != "" {
					opts = append(opts, bucket.WithKeyGreaterThanOrEqual(cctx.String("greater-than-equal")))
				}
				if cctx.String("lt") != "" {
					opts = append(opts, bucket.WithKeyLessThan(cctx.String("less-than")))
				}
				if cctx.String("lte") != "" {
					opts = append(opts, bucket.WithKeyLessThanOrEqual(cctx.String("less-than-equal")))
				}
				count := 0
				limit := cctx.Int("limit")
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
			}),
		},
		{
			Name:      "root",
			Usage:     "Derive the current root CID for the bucket.",
			Args:      true,
			ArgsUsage: "<id>",
			Action: util.WithContext(ctx, func(cctx *cli.Context, fctx *util.Context) error {
				id, err := did.Parse(cctx.Args().First())
				if err != nil {
					log.Fatal(err)
				}
				bk, err := fctx.Client.Bucket(ctx, id)
				if err != nil {
					log.Fatal(err)
				}
				root, err := bk.Root(ctx)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("%s\n", root)
				return nil
			}),
		},
		// 	{
		// 		Name:      "del",
		// 		Aliases:   []string{"delete"},
		// 		Usage:     "Delete an entry from a bucket",
		// 		Args:      true,
		// 		ArgsUsage: "<key>",
		// 		Action: func(cCtx *cli.Context) error {
		// 			datadir := util.EnsureDataDir(cCtx.String("datadir"))
		// 			userdata := util.UserDataStore(context.Background(), datadir)
		// 			curr := util.GetCurrent(datadir)
		// 			if curr == did.Undef {
		// 				return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
		// 			}
		// 			bk, err := userdata.Bucket(context.Background(), curr)
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			key := cCtx.Args().Get(0)
		// 			if key == "" {
		// 				return fmt.Errorf("missing key")
		// 			}
		// 			err = bk.Del(context.Background(), key)
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			root, err := bk.Root(context.Background())
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			fmt.Println(root.String())
		// 			return nil
		// 		},
		// 	},
		// 	{
		// 		Name:      "pull",
		// 		Usage:     "Pull changes from a remote",
		// 		Args:      true,
		// 		ArgsUsage: "[remote]",
		// 		Action: func(cCtx *cli.Context) error {
		// 			datadir := util.EnsureDataDir(cCtx.String("datadir"))
		// 			userdata := util.UserDataStore(context.Background(), datadir)
		// 			curr := util.GetCurrent(datadir)
		// 			if curr == did.Undef {
		// 				return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
		// 			}
		// 			bk, err := userdata.Bucket(context.Background(), curr)
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			if nbk, ok := bk.(fbucket.Networker); ok {
		// 				name := cCtx.Args().Get(0)
		// 				if name == "" {
		// 					name = store.DefaultRemoteName
		// 				}
		// 				remote, err := nbk.Remote(context.Background(), name)
		// 				if err != nil {
		// 					if errors.Is(err, fbucket.ErrNotFound) {
		// 						return fmt.Errorf("remote not found: %s", name)
		// 					}
		// 					log.Fatal(err)
		// 				}
		// 				err = remote.Pull(context.Background())
		// 				if err != nil {
		// 					log.Fatal(err)
		// 				}
		// 				root, err := bk.Root(context.Background())
		// 				if err != nil {
		// 					log.Fatal(err)
		// 				}
		// 				fmt.Println(root.String())
		// 			} else {
		// 				return fmt.Errorf("bucket is not a networker")
		// 			}
		// 			return nil
		// 		},
		// 	},
		// 	{
		// 		Name:      "push",
		// 		Usage:     "Push local changes to a remote",
		// 		Args:      true,
		// 		ArgsUsage: "[remote]",
		// 		Action: func(cCtx *cli.Context) error {
		// 			datadir := util.EnsureDataDir(cCtx.String("datadir"))
		// 			userdata := util.UserDataStore(context.Background(), datadir)
		// 			curr := util.GetCurrent(datadir)
		// 			if curr == did.Undef {
		// 				return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
		// 			}
		// 			bk, err := userdata.Bucket(context.Background(), curr)
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			if nbk, ok := bk.(fbucket.Networker); ok {
		// 				name := cCtx.Args().Get(0)
		// 				if name == "" {
		// 					name = store.DefaultRemoteName
		// 				}
		// 				remote, err := nbk.Remote(context.Background(), name)
		// 				if err != nil {
		// 					if errors.Is(err, fbucket.ErrNotFound) {
		// 						return fmt.Errorf("remote not found: %s", name)
		// 					}
		// 					log.Fatal(err)
		// 				}
		// 				err = remote.Push(context.Background())
		// 				if err != nil {
		// 					log.Fatal(err)
		// 				}
		// 				root, err := bk.Root(context.Background())
		// 				if err != nil {
		// 					log.Fatal(err)
		// 				}
		// 				fmt.Println(root.String())
		// 			} else {
		// 				return fmt.Errorf("bucket is not a networker")
		// 			}
		// 			return nil
		// 		},
		// 	},
		// 	{
		// 		Name:      "put",
		// 		Usage:     "Put a value to the bucket",
		// 		Args:      true,
		// 		ArgsUsage: "<key> <value>",
		// 		Action: func(cCtx *cli.Context) error {
		// 			datadir := util.EnsureDataDir(cCtx.String("datadir"))
		// 			userdata := util.UserDataStore(context.Background(), datadir)
		// 			curr := util.GetCurrent(datadir)
		// 			if curr == did.Undef {
		// 				return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
		// 			}
		// 			bk, err := userdata.Bucket(context.Background(), curr)
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			key := cCtx.Args().Get(0)
		// 			if key == "" {
		// 				return fmt.Errorf("missing key")
		// 			}
		// 			value, err := cid.Parse(cCtx.Args().Get(1))
		// 			if err != nil {
		// 				return fmt.Errorf("invalid value: %w", err)
		// 			}
		// 			err = bk.Put(context.Background(), key, cidlink.Link{Cid: value})
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			root, err := bk.Root(context.Background())
		// 			if err != nil {
		// 				log.Fatal(err)
		// 			}
		// 			fmt.Println(root.String())
		// 			return nil
		// 		},
		// 	},
		// 	remote.Command,
	}
}

package bucket

import (
	"context"
	"errors"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/fam/cmd/bucket/remote"
	"github.com/storacha/fam/cmd/util"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("bucket")

var Command = &cli.Command{
	Name:  "bucket",
	Usage: "Print buckets",
	Action: func(cCtx *cli.Context) error {
		datadir := util.EnsureDataDir(cCtx.String("datadir"))
		userdata := util.UserDataStore(context.Background(), datadir)
		buckets, err := userdata.Buckets(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		curr := util.GetCurrent(datadir)
		count := 0
		for id := range buckets {
			if id == curr {
				fmt.Printf("* %s\n", id)
			} else {
				fmt.Printf("  %s\n", id)
			}
			count++
		}
		fmt.Printf("%d total\n", count)
		return nil
	},
	Subcommands: []*cli.Command{
		{
			Name:      "import",
			Usage:     "Import a bucket",
			Args:      true,
			ArgsUsage: "<grant>",
			Action: func(cCtx *cli.Context) error {
				datadir := util.EnsureDataDir(cCtx.String("datadir"))
				userdata := util.UserDataStore(context.Background(), datadir)
				arg := cCtx.Args().Get(0)
				// TODO: remove
				if arg == "" {
					issuer, _ := signer.Generate()
					audience, _ := userdata.ID(context.Background())
					d, _ := delegation.Delegate(
						issuer,
						audience,
						[]ucan.Capability[ucan.NoCaveats]{
							ucan.NewCapability("space/blob/*", issuer.DID().String(), ucan.NoCaveats{}),
							ucan.NewCapability("clock/*", issuer.DID().String(), ucan.NoCaveats{}),
						},
					)
					arg, _ = delegation.Format(d)
				}
				proof, err := delegation.Parse(arg)
				if err != nil {
					log.Fatal(err)
				}
				id, err := userdata.AddBucket(context.Background(), proof)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(id)
				curr := util.GetCurrent(datadir)
				if curr == did.Undef {
					util.SetCurrent(datadir, id)
				}
				return nil
			},
		},
		{
			Name:      "pull",
			Usage:     "Pull changes from a remote",
			Args:      true,
			ArgsUsage: "<remote>",
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
				if nbk, ok := bk.(bucket.Networker); ok {
					name := cCtx.Args().Get(0)
					remote, err := nbk.Remote(context.Background(), name)
					if err != nil {
						if errors.Is(err, bucket.ErrNotFound) {
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
			Usage:     "Push local changes from a remote",
			Args:      true,
			ArgsUsage: "<remote>",
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
				if nbk, ok := bk.(bucket.Networker); ok {
					name := cCtx.Args().Get(0)
					remote, err := nbk.Remote(context.Background(), name)
					if err != nil {
						if errors.Is(err, bucket.ErrNotFound) {
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
		{
			Name:      "use",
			Usage:     "Use a bucket",
			Args:      true,
			ArgsUsage: "<id>",
			Action: func(cCtx *cli.Context) error {
				datadir := util.EnsureDataDir(cCtx.String("datadir"))
				userdata := util.UserDataStore(context.Background(), datadir)
				buckets, err := userdata.Buckets(context.Background())
				if err != nil {
					log.Fatal(err)
				}
				if len(buckets) == 0 {
					return fmt.Errorf("no buckets, use `fam bucket import`")
				}
				id, err := did.Parse(cCtx.Args().Get(0))
				if err != nil {
					return fmt.Errorf("parsing bucket DID: \"%s\"", cCtx.Args().Get(0))
				}
				if _, ok := buckets[id]; !ok {
					return fmt.Errorf("bucket not found: %s", id)
				}
				util.SetCurrent(datadir, id)
				fmt.Printf("* %s\n", id)
				return nil
			},
		},
	},
}

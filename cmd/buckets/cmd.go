package buckets

import (
	"context"
	"fmt"
	"slices"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/fam/cmd/util"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("buckets")

func Commands(ctx context.Context) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "bucket",
			Usage: "Manage buckets",
			Subcommands: []*cli.Command{
				// {
				// 	Name:      "import",
				// 	Usage:     "Import a shared bucket",
				// 	Args:      true,
				// 	ArgsUsage: "<grant>",
				// 	Action: func(cCtx *cli.Context) error {
				// 		datadir := util.EnsureDataDir(cCtx.String("datadir"))
				// 		userdata := util.UserDataStore(context.Background(), datadir)
				// 		arg := cCtx.Args().Get(0)
				// 		// TODO: remove
				// 		if arg == "" {
				// 			issuer, _ := signer.Generate()
				// 			audience, _ := userdata.ID(context.Background())
				// 			d, _ := delegation.Delegate(
				// 				issuer,
				// 				audience,
				// 				[]ucan.Capability[ucan.NoCaveats]{
				// 					ucan.NewCapability("space/blob/*", issuer.DID().String(), ucan.NoCaveats{}),
				// 					ucan.NewCapability("clock/*", issuer.DID().String(), ucan.NoCaveats{}),
				// 				},
				// 			)
				// 			arg, _ = delegation.Format(d)
				// 		}
				// 		proof, err := delegation.Parse(arg)
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		id, err := userdata.AddBucket(context.Background(), proof)
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		fmt.Println(id)
				// 		curr := util.GetCurrent(datadir)
				// 		if curr == did.Undef {
				// 			util.SetCurrent(datadir, id)
				// 		}
				// 		return nil
				// 	},
				// },
				{
					Name:    "ls",
					Usage:   "List buckets",
					Aliases: []string{"list"},
					Action: util.WithContext(ctx, func(cctx *cli.Context, fctx *util.Context) error {
						buckets, err := fctx.Client.Buckets(fctx.Context)
						if err != nil {
							log.Fatal(err)
						}
						var ids []string
						for _, id := range buckets {
							ids = append(ids, id.String())
						}
						slices.Sort(ids)
						for _, id := range ids {
							fmt.Printf("%s\n", id)
						}
						fmt.Printf("%d total\n", len(ids))
						return nil
					}),
				},
				// {
				// 	Name:      "rm",
				// 	Usage:     "Remove a bucket",
				// 	Aliases:   []string{"remove"},
				// 	Args:      true,
				// 	ArgsUsage: "<id>",
				// 	Action: func(cCtx *cli.Context) error {
				// 		datadir := util.EnsureDataDir(cCtx.String("datadir"))
				// 		userdata := util.UserDataStore(context.Background(), datadir)
				// 		id, err := did.Parse(cCtx.Args().Get(0))
				// 		if err != nil {
				// 			return fmt.Errorf("parsing bucket DID: %w", err)
				// 		}
				// 		err = userdata.RemoveBucket(context.Background(), id)
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		curr := util.GetCurrent(datadir)
				// 		if curr != did.Undef && curr == id {
				// 			util.SetCurrent(datadir, did.Undef)
				// 		}
				// 		return nil
				// 	},
				// },
				// {
				// 	Name:      "share",
				// 	Usage:     "Share a bucket",
				// 	Args:      true,
				// 	ArgsUsage: "<recipient>",
				// 	Action: func(cCtx *cli.Context) error {
				// 		datadir := util.EnsureDataDir(cCtx.String("datadir"))
				// 		userdata := util.UserDataStore(context.Background(), datadir)
				// 		buckets, err := userdata.Buckets(context.Background())
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		if len(buckets) == 0 {
				// 			return fmt.Errorf("no buckets, use `fam bucket import`")
				// 		}
				// 		audience, err := did.Parse(cCtx.Args().Get(0))
				// 		if err != nil {
				// 			return fmt.Errorf("parsing recipient DID: \"%s\"", cCtx.Args().Get(0))
				// 		}
				// 		curr := util.GetCurrent(datadir)
				// 		if curr == did.Undef {
				// 			return fmt.Errorf("no bucket selected, use `fam bucket use <did>`")
				// 		}
				// 		proof, ok := buckets[curr]
				// 		if !ok {
				// 			return fmt.Errorf("bucket not found: %s", curr)
				// 		}
				// 		issuer, err := userdata.ID(context.Background())
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		d, err := delegation.Delegate(
				// 			issuer,
				// 			audience,
				// 			[]ucan.Capability[ucan.NoCaveats]{
				// 				ucan.NewCapability("space/blob/*", curr.String(), ucan.NoCaveats{}),
				// 				ucan.NewCapability("clock/*", curr.String(), ucan.NoCaveats{}),
				// 			},
				// 			delegation.WithProof(delegation.FromDelegation(proof)),
				// 		)
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		s, err := delegation.Format(d)
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		fmt.Printf("%s\n", s)
				// 		return nil
				// 	},
				// },
				// {
				// 	Name:      "use",
				// 	Usage:     "Use a bucket",
				// 	Args:      true,
				// 	ArgsUsage: "<id>",
				// 	Action: func(cCtx *cli.Context) error {
				// 		datadir := util.EnsureDataDir(cCtx.String("datadir"))
				// 		userdata := util.UserDataStore(context.Background(), datadir)
				// 		buckets, err := userdata.Buckets(context.Background())
				// 		if err != nil {
				// 			log.Fatal(err)
				// 		}
				// 		if len(buckets) == 0 {
				// 			return fmt.Errorf("no buckets, use `fam bucket import`")
				// 		}
				// 		id, err := did.Parse(cCtx.Args().Get(0))
				// 		if err != nil {
				// 			return fmt.Errorf("parsing bucket DID: \"%s\"", cCtx.Args().Get(0))
				// 		}
				// 		if _, ok := buckets[id]; !ok {
				// 			return fmt.Errorf("bucket not found: %s", id)
				// 		}
				// 		util.SetCurrent(datadir, id)
				// 		fmt.Printf("* %s\n", id)
				// 		return nil
				// 	},
				// },
			},
		},
	}
}

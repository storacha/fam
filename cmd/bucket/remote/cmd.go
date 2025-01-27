package remote

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/fam/cmd/util"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/principal/multiformat"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("remote")

var Command = &cli.Command{
	Name:  "remote",
	Usage: "Print configured remotes",
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
			rems, err := nbk.Remotes(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			count := 0
			for entry, err := range rems.Entries(context.Background()) {
				if err != nil {
					log.Fatal(err)
				}
				count++
				fmt.Printf("%s\n", entry.Key)
				pk, err := entry.Value.ID.ExtractPublicKey()
				if err != nil {
					log.Fatal(err)
				}
				raw, err := pk.Raw()
				if err != nil {
					log.Fatal(err)
				}
				key, err := multibase.Encode(multibase.Base58BTC, multiformat.TagWith(verifier.Code, raw))
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("  ID:    did:key:%s\n", key)
				fmt.Println("  Addrs:")
				for _, a := range entry.Value.Addrs {
					fmt.Printf("    %s\n", a)
				}
				fmt.Println()
			}
			fmt.Printf("%d total\n", count)
		} else {
			return fmt.Errorf("bucket is not a networker")
		}
		return nil
	},
	Subcommands: []*cli.Command{
		{
			Name:      "add",
			Usage:     "Add a remote",
			Args:      true,
			ArgsUsage: "<name> <id> <address>",
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
					rems, err := nbk.Remotes(context.Background())
					if err != nil {
						log.Fatal(err)
					}
					name := cCtx.Args().Get(0)
					if name == "" {
						return fmt.Errorf("missing remote name")
					}
					id, err := verifier.Parse(cCtx.Args().Get(1))
					if err != nil {
						return fmt.Errorf("parsing remote DID: %w", err)
					}
					pubKey, err := crypto.UnmarshalEd25519PublicKey(id.Raw())
					if err != nil {
						return fmt.Errorf("unmarshalling Ed25519 public key: %w", err)
					}
					peerID, err := peer.IDFromPublicKey(pubKey)
					if err != nil {
						return fmt.Errorf("creating peer ID from public key: %w", err)
					}
					addr, err := multiaddr.NewMultiaddr(cCtx.Args().Get(2))
					if err != nil {
						return fmt.Errorf("parsing multiaddr: %w", err)
					}
					info := peer.AddrInfo{
						ID:    peerID,
						Addrs: []multiaddr.Multiaddr{addr},
					}
					err = rems.Put(context.Background(), name, info)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					return fmt.Errorf("bucket is not a networker")
				}
				return nil
			},
		},
		{
			Name:      "rm",
			Usage:     "Remove a remote",
			Aliases:   []string{"remove"},
			Args:      true,
			ArgsUsage: "<name>",
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
					rems, err := nbk.Remotes(context.Background())
					if err != nil {
						log.Fatal(err)
					}
					name := cCtx.Args().Get(0)
					if name == "" {
						return fmt.Errorf("missing remote name")
					}
					err = rems.Del(context.Background(), name)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					return fmt.Errorf("bucket is not a networker")
				}
				return nil
			},
		},
	},
}

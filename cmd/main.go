package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/fam/api"
	"github.com/storacha/fam/cmd/handlers"
	"github.com/storacha/fam/cmd/util"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("cmd")

func main() {
	if os.Getenv("DEBUG") != "" {
		logging.SetLogLevel(os.Getenv("DEBUG"), "debug")
	}
	ctx, cancel := context.WithCancel(context.Background())

	commands := []*cli.Command{
		{
			Name:    "whodis",
			Usage:   "Print your agent DID",
			Aliases: []string{"whoami"},
			Action: func(cCtx *cli.Context) error {
				datadir := util.EnsureDataDir(cCtx.String("datadir"))
				id, err := util.AgentSigner(util.AgentPrivateKeyPath(datadir))
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(id.DID().String())
				return nil
			},
		},
		{
			Name:  "start",
			Usage: "Start the daemon",
			Action: func(cCtx *cli.Context) error {
				datadir := util.EnsureDataDir(cCtx.String("datadir"))
				srv, err := api.New(datadir)
				if err != nil {
					log.Fatal(err)
				}
				err = srv.Start(ctx)
				if err != nil {
					log.Fatal(err)
				}
				return nil
			},
		},
	}
	commands = append(commands, handlers.Bucket(ctx)...)
	commands = append(commands, handlers.Buckets(ctx)...)
	commands = append(commands, handlers.Ping(ctx)...)

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
		Commands: commands,
	}

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			fmt.Println("Received interrupt signal, shutting down...")
			fmt.Println("(Hit ctrl-c again to force-shutdown the daemon.)")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

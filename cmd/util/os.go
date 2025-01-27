package util

import (
	"context"
	"fmt"
	"os"
	"path"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/storacha/fam/store"
)

func mkdirp(dirpath ...string) (string, error) {
	dir := path.Join(dirpath...)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", fmt.Errorf("creating directory: %s: %w", dir, err)
	}
	return dir, nil
}

func EnsureDataDir(dataDir string) string {
	if dataDir == "" {
		homedir, err := os.UserHomeDir()
		if err != nil {
			log.Fatalln("getting user home directory: %w", err)
		}
		dataDir = path.Join(homedir, ".fam")
	}
	_, err := mkdirp(dataDir)
	if err != nil {
		log.Fatalln("creating data directory: %w", err)
	}
	return dataDir
}

func UserDataStore(ctx context.Context, dataDir string) *store.UserDataStore {
	dstore, err := leveldb.NewDatastore(dataDir, nil)
	if err != nil {
		log.Fatalln("creating datastore: %w", err)
	}
	userdata, err := store.NewUserDataStore(ctx, dstore)
	if err != nil {
		log.Fatalln(err)
	}
	return userdata
}

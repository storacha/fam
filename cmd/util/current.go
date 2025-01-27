package util

import (
	"errors"
	"os"
	"path"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
)

var log = logging.Logger("util")

func GetCurrent(dataDir string) did.DID {
	cliDataDir, err := mkdirp(dataDir, "cli")
	if err != nil {
		log.Fatalln("creating CLI data directory: %w", err)
	}
	b, err := os.ReadFile(path.Join(cliDataDir, "current"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return did.Undef
		}
		log.Fatalln("creating CLI data directory: %w", err)
	}
	if len(b) == 0 {
		return did.Undef
	}
	id, err := did.Decode(b)
	if err != nil {
		log.Fatalln("decoding current bucket DID: %w", err)
	}
	return id
}

func SetCurrent(dataDir string, id did.DID) {
	cliDataDir, err := mkdirp(dataDir, "cli")
	if err != nil {
		log.Fatalln("creating CLI data directory: %w", err)
	}
	var bytes []byte
	if id.Defined() {
		bytes = id.Bytes()
	}
	err = os.WriteFile(path.Join(cliDataDir, "current"), bytes, 0644)
	if err != nil {
		log.Fatalln("creating CLI data directory: %w", err)
	}
}

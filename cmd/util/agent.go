package util

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/storacha/fam/principal/ed25519/pem"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

func AgentPrivateKeyPath(dataDir string) string {
	return path.Join(dataDir, "id.cli.pem")
}

// AgentSigner gets the stored agent identity or generates a new one.
func AgentSigner(pemPath string) (principal.Signer, error) {
	id, err := pem.ReadPrivateKey(pemPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s, err := ed25519.Generate()
			if err != nil {
				return nil, fmt.Errorf("generating private key: %w", err)
			}
			err = pem.WritePrivateKey(pemPath, s)
			if err != nil {
				return nil, fmt.Errorf("writing private key: %w", err)
			}
			id = s
		} else {
			return nil, fmt.Errorf("reading private key: %w", err)
		}
	}
	return id, nil
}

func ServicePrivateKeyPath(dataDir string) string {
	return path.Join(dataDir, "id.pem")
}

// ServiceSigner gets the stored service identity.
func ServiceSigner(pemPath string) (principal.Signer, error) {
	return pem.ReadPrivateKey(pemPath)
}

package pem

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/storacha/go-ucanto/principal"
	ucaned "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

func ReadPrivateKey(path string) (principal.Signer, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var privateKey *ed25519.PrivateKey
	rest := pemData

	// Loop until no more blocks
	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			// No more PEM blocks
			break
		}
		rest = remaining

		// Look for "PRIVATE KEY"
		if block.Type == "PRIVATE KEY" {
			parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parsing PKCS#8 private key: %w", err)
			}

			// We expect a ed25519 private key, cast it
			key, ok := parsedKey.(ed25519.PrivateKey)
			if !ok {
				return nil, errors.New("parsed key is not an ed25519 private key")
			}
			privateKey = &key
			break
		}
	}

	if privateKey == nil {
		return nil, errors.New("could not find a PRIVATE KEY block in the PEM file")
	}
	return ucaned.FromRaw(*privateKey)
}

func WritePrivateKey(path string, signer principal.Signer) error {
	privateKey := ed25519.PrivateKey(signer.Raw())

	// Marshal and encode the private key
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("marshaling PKCS8 private key: %w", err)
	}

	privateKeyBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privateKeyBytes,
	}

	buffer := new(bytes.Buffer)
	if err := pem.Encode(buffer, privateKeyBlock); err != nil {
		return fmt.Errorf("encoding PEM private key block: %w", err)
	}

	// Marshal and encode the public key
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(privateKey.Public())
	if err != nil {
		return fmt.Errorf("marshaling PKIX public key: %w", err)
	}

	publicKeyBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}

	// Append the public key block to the same file
	if err := pem.Encode(buffer, publicKeyBlock); err != nil {
		return fmt.Errorf("encoding PEM public key block: %w", err)
	}

	if err = os.WriteFile(path, buffer.Bytes(), 0600); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	return nil
}

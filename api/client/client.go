package client

import (
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
)

type Client struct {
	signer ucan.Signer
	conn   client.Connection
	Proofs ProofProviderFunc
}

type ProofProviderFunc = func(can ucan.Ability) ([]delegation.Proof, error)

func noProofs(can ucan.Ability) ([]delegation.Proof, error) {
	return []delegation.Proof{}, nil
}

func New(signer ucan.Signer, connection client.Connection, getProofs ProofProviderFunc) *Client {
	if getProofs == nil {
		getProofs = noProofs
	}
	return &Client{signer: signer, conn: connection, Proofs: getProofs}
}

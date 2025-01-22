package bucket

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	rsa "github.com/storacha/go-ucanto/principal/rsa/signer"
)

func NewKeyBucket(bucket Bucket[ipld.Link]) Bucket[principal.Signer] {
	return NewIdentityBucket(bucket, func(s principal.Signer) ([]byte, error) {
		return s.Encode(), nil
	}, func(b []byte) (principal.Signer, error) {
		s, err := ed25519.Decode(b)
		if err != nil {
			s, err = rsa.Decode(b)
			if err != nil {
				return nil, err
			}
		}
		return s, nil
	})
}

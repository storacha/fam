package bucket

import (
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-ucanto/core/delegation"
)

func NewDelegationBucket(bucket Bucket[ipld.Link]) Bucket[delegation.Delegation] {
	return NewIdentityBytesBucket(bucket, func(d delegation.Delegation) ([]byte, error) {
		return io.ReadAll(d.Archive())
	}, func(b []byte) (delegation.Delegation, error) {
		return delegation.Extract(b)
	})
}

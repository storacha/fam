package bucket

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multicodec"
)

func NewBytesBucket(b Bucket[ipld.Link], codec uint64) Bucket[[]byte] {
	if codec == 0 {
		codec = uint64(multicodec.Raw)
	}
	return nil
}

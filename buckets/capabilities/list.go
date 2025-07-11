package capabilities

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
)

const ListAbility = "buckets/list"

type ListCaveats struct{}

func (c ListCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, ListCaveatsType(), types.Converters...)
}

var ListCaveatsReader = schema.Struct[ListCaveats](ListCaveatsType(), nil, types.Converters...)

type ListOk struct {
	Results []Bucket
}

func (ok ListOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, ListOkType(), types.Converters...)
}

type Bucket struct {
	ID did.DID
}

func (r Bucket) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&r, BucketType(), types.Converters...)
}

type ListReceipt receipt.Receipt[ListOk, ipld.Node]
type ListReceiptReader receipt.ReceiptReader[ListOk, ipld.Node]

func NewListReceiptReader() (ListReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[ListOk, ipld.Node](ListOkType(), ListErrType(), types.Converters...)
}

var List = validator.NewCapability(
	ListAbility,
	schema.DIDString(),
	ListCaveatsReader,
	nil,
)

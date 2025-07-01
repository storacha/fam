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

const ListAbility = "bucket/list"

type ListCaveats struct{}

func (c ListCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, ListCaveatsType(), types.Converters...)
}

var ListCaveatsReader = schema.Struct[ListCaveats](ListCaveatsType(), nil, types.Converters...)

type ListOk struct {
	Results []ListResult
}

func (ok ListOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, ListOkType(), types.Converters...)
}

type ListResult struct {
	Id did.DID
}

func (r ListResult) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&r, ListResultType(), types.Converters...)
}

type ListReceipt receipt.Receipt[ListOk, ipld.Node]
type ListReceiptReader receipt.ReceiptReader[ListOk, ipld.Node]

func NewListReceiptReader() (ListReceiptReader, error) {
	return receipt.NewReceiptReader[ListOk, ipld.Node](schemaBytes, types.Converters...)
}

var List = validator.NewCapability(
	ListAbility,
	schema.DIDString(),
	ListCaveatsReader,
	nil,
)

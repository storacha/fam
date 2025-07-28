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

const GetAbility = "bucket/get"

type GetCaveats struct {
	ID   did.DID
	Root ipld.Link
	Key  string
}

func (c GetCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, GetCaveatsType(), types.Converters...)
}

var GetCaveatsReader = schema.Struct[GetCaveats](GetCaveatsType(), nil, types.Converters...)

type GetOk struct {
	Value ipld.Link
}

func (ok GetOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, GetOkType(), types.Converters...)
}

type GetReceipt receipt.Receipt[GetOk, ipld.Node]
type GetReceiptReader receipt.ReceiptReader[GetOk, ipld.Node]

func NewGetReceiptReader() (GetReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[GetOk, ipld.Node](GetOkType(), GetErrType(), types.Converters...)
}

var Get = validator.NewCapability(
	GetAbility,
	schema.DIDString(),
	GetCaveatsReader,
	nil,
)

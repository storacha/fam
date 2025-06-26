package capabilities

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/validator"
)

const SendAbility = "ping/send"

type SendCaveats struct {
	Message string
}

func (pc SendCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&pc, SendCaveatsType(), types.Converters...)
}

var SendCaveatsReader = schema.Struct[SendCaveats](SendCaveatsType(), nil, types.Converters...)

type SendOk struct {
	Message string
}

func (po SendOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&po, SendOkType(), types.Converters...)
}

type SendReceipt receipt.Receipt[SendOk, ipld.Node]
type SendReceiptReader receipt.ReceiptReader[SendOk, ipld.Node]

func NewSendReceiptReader() (SendReceiptReader, error) {
	return receipt.NewReceiptReader[SendOk, ipld.Node](schemaBytes)
}

var Send = validator.NewCapability(
	SendAbility,
	schema.DIDString(),
	SendCaveatsReader,
	nil,
)

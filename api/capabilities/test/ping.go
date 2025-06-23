package test

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/validator"
)

const PingAbility = "test/ping"

type PingCaveats struct {
	Message string
}

func (pc PingCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&pc, PingCaveatsType(), types.Converters...)
}

var PingCaveatsReader = schema.Struct[PingCaveats](PingCaveatsType(), nil, types.Converters...)

type PingOk struct {
	Message string
}

func (po PingOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&po, PingOkType(), types.Converters...)
}

type PingReceipt receipt.Receipt[PingOk, ipld.Node]
type PingReceiptReader receipt.ReceiptReader[PingOk, ipld.Node]

func NewPingReceiptReader() (PingReceiptReader, error) {
	return receipt.NewReceiptReader[PingOk, ipld.Node](schemaBytes)
}

var Ping = validator.NewCapability(
	PingAbility,
	schema.DIDString(),
	PingCaveatsReader,
	nil,
)

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

const DeleteAbility = "bucket/delete"

type DeleteCaveats struct {
	ID    did.DID
	Root  ipld.Link
	Key   string
	Value ipld.Link
}

func (c DeleteCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, DeleteCaveatsType(), types.Converters...)
}

var DeleteCaveatsReader = schema.Struct[DeleteCaveats](DeleteCaveatsType(), nil, types.Converters...)

type DeleteOk struct {
	Root ipld.Link
}

func (ok DeleteOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, DeleteOkType(), types.Converters...)
}

type DeleteReceipt receipt.Receipt[DeleteOk, ipld.Node]
type DeleteReceiptReader receipt.ReceiptReader[DeleteOk, ipld.Node]

func NewDeleteReceiptReader() (DeleteReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[DeleteOk, ipld.Node](DeleteOkType(), DeleteErrType(), types.Converters...)
}

var Delete = validator.NewCapability(
	DeleteAbility,
	schema.DIDString(),
	DeleteCaveatsReader,
	nil,
)

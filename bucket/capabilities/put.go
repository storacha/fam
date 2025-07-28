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

const PutAbility = "bucket/put"

type PutCaveats struct {
	ID    did.DID
	Root  ipld.Link
	Key   string
	Value ipld.Link
}

func (c PutCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, PutCaveatsType(), types.Converters...)
}

var PutCaveatsReader = schema.Struct[PutCaveats](PutCaveatsType(), nil, types.Converters...)

type PutOk struct {
	Root ipld.Link
}

func (ok PutOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, PutOkType(), types.Converters...)
}

type PutReceipt receipt.Receipt[PutOk, ipld.Node]
type PutReceiptReader receipt.ReceiptReader[PutOk, ipld.Node]

func NewPutReceiptReader() (PutReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[PutOk, ipld.Node](PutOkType(), PutErrType(), types.Converters...)
}

var Put = validator.NewCapability(
	PutAbility,
	schema.DIDString(),
	PutCaveatsReader,
	nil,
)

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

const AddAbility = "buckets/add"

type AddCaveats struct {
	Proof ipld.Link
}

func (c AddCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, AddCaveatsType(), types.Converters...)
}

var AddCaveatsReader = schema.Struct[AddCaveats](AddCaveatsType(), nil, types.Converters...)

type AddOk struct {
	ID did.DID
}

func (ok AddOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, AddOkType(), types.Converters...)
}

type AddReceipt receipt.Receipt[AddOk, ipld.Node]
type AddReceiptReader receipt.ReceiptReader[AddOk, ipld.Node]

func NewAddReceiptReader() (AddReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[AddOk, ipld.Node](AddOkType(), AddErrType(), types.Converters...)
}

var Add = validator.NewCapability(
	AddAbility,
	schema.DIDString(),
	AddCaveatsReader,
	nil,
)

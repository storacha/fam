package capabilities

import (
	"time"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
)

const RootAbility = "bucket/root"

type RootCaveats struct {
	ID        did.DID
	Timestamp time.Time
}

func (c RootCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, RootCaveatsType(), types.Converters...)
}

var RootCaveatsReader = schema.Struct[RootCaveats](RootCaveatsType(), nil, types.Converters...)

type RootOk struct {
	Root ipld.Link
}

func (ok RootOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, RootOkType(), types.Converters...)
}

type RootReceipt receipt.Receipt[RootOk, ipld.Node]
type RootReceiptReader receipt.ReceiptReader[RootOk, ipld.Node]

func NewRootReceiptReader() (RootReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[RootOk, ipld.Node](RootOkType(), RootErrType(), types.Converters...)
}

var Root = validator.NewCapability(
	RootAbility,
	schema.DIDString(),
	RootCaveatsReader,
	nil,
)

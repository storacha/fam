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

const EntriesAbility = "bucket/entries"

type EntriesCaveats struct {
	ID               did.DID
	Root             ipld.Link
	Prefix           *string
	GreaterThan      *string
	GreaterThanEqual *string
	LessThan         *string
	LessThanEqual    *string
	Limit            *int
}

func (c EntriesCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, EntriesCaveatsType(), types.Converters...)
}

var EntriesCaveatsReader = schema.Struct[EntriesCaveats](EntriesCaveatsType(), nil, types.Converters...)

type EntriesOk struct {
	Results []Entry
}

func (ok EntriesOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, EntriesOkType(), types.Converters...)
}

type Entry struct {
	Key   string
	Value ipld.Link
}

func (r Entry) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&r, EntryType(), types.Converters...)
}

type EntriesReceipt receipt.Receipt[EntriesOk, ipld.Node]
type EntriesReceiptReader receipt.ReceiptReader[EntriesOk, ipld.Node]

func NewEntriesReceiptReader() (EntriesReceiptReader, error) {
	return receipt.NewReceiptReaderFromTypes[EntriesOk, ipld.Node](EntriesOkType(), EntriesErrType(), types.Converters...)
}

var Entries = validator.NewCapability(
	EntriesAbility,
	schema.DIDString(),
	EntriesCaveatsReader,
	nil,
)

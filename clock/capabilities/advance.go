package capabilities

import (
	// for schema embed
	_ "embed"

	"github.com/ipld/go-ipld-prime/datamodel"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/validator"
)

func AdvanceCaveatsType() ipldschema.Type {
	return ts.TypeByName("AdvanceCaveats")
}

type AdvanceCaveats struct {
	Event ipld.Link
}

func (ac AdvanceCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ac, AdvanceCaveatsType())
}

const AdvanceAbility = "clock/advance"

var AdvanceCaveatsReader = schema.Struct[AdvanceCaveats](AdvanceCaveatsType(), nil)

type AdvanceOk struct {
	Head []ipld.Link
}

func (ok AdvanceOk) ToIPLD() (ipld.Node, error) {
	return ipld.WrapWithRecovery(&ok, OkType())
}

type AdvanceErr = ipld.Node

var Advance = validator.NewCapability(AdvanceAbility, schema.DIDString(), AdvanceCaveatsReader, nil)

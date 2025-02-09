package clock

import (
	"fmt"

	// for schema embed
	_ "embed"

	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

//go:embed clock.ipldsch
var clockSchema []byte

var clockTS = mustLoadTS()

func mustLoadTS() *ipldschema.TypeSystem {
	ts, err := ipldprime.LoadSchemaBytes(clockSchema)
	if err != nil {
		panic(fmt.Errorf("loading clock schema: %w", err))
	}
	return ts
}

func HeadCaveatsType() ipldschema.Type {
	return clockTS.TypeByName("HeadCaveats")
}

type HeadCaveats = ucan.NoCaveats

const HeadAbility = "clock/head"

var HeadCaveatsReader = schema.Struct[HeadCaveats](HeadCaveatsType(), nil)

var Head = validator.NewCapability(AdvanceAbility, schema.DIDString(), HeadCaveatsReader, nil)

func AdvanceCaveatsType() ipldschema.Type {
	return clockTS.TypeByName("AdvanceCaveats")
}

type AdvanceCaveats struct {
	Event ipld.Link
}

func (ac AdvanceCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ac, AdvanceCaveatsType())
}

const AdvanceAbility = "clock/advance"

var AdvanceCaveatsReader = schema.Struct[AdvanceCaveats](AdvanceCaveatsType(), nil)

var Advance = validator.NewCapability(AdvanceAbility, schema.DIDString(), AdvanceCaveatsReader, nil)

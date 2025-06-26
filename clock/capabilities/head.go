package capabilities

import (
	// for schema embed
	_ "embed"

	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

type HeadCaveats = ucan.NoCaveats

const HeadAbility = "clock/head"

var HeadCaveatsReader = schema.Struct[HeadCaveats](HeadCaveatsType(), nil)

type HeadOk struct {
	Head []ipld.Link
}

func (ok HeadOk) ToIPLD() (ipld.Node, error) {
	return ipld.WrapWithRecovery(&ok, OkType())
}

type HeadErr = ipld.Node

var Head = validator.NewCapability(AdvanceAbility, schema.DIDString(), HeadCaveatsReader, nil)

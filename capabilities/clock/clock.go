package clock

import (
	"fmt"

	// for schema embed
	_ "embed"

	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/core/ipld"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
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

func OkType() ipldschema.Type {
	return clockTS.TypeByName("Ok")
}

type Ok struct {
	Head []ipld.Link
}

func (ok Ok) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&ok, OkType())
}

func ErrType() ipldschema.Type {
	return clockTS.TypeByName("Err")
}

type Err = ipld.Node

// BindFailure binds the IPLD node to a FailureModel if possible. This works
// around IPLD requiring data to match the schema exactly
func BindFailure(n ipld.Node) (fdm.FailureModel, error) {
	var f fdm.FailureModel

	nn, err := n.LookupByString("name")
	if err == nil {
		name, err := nn.AsString()
		if err != nil {
			return fdm.FailureModel{}, fmt.Errorf("reading name: %w", err)
		}
		f.Name = &name
	}

	mn, err := n.LookupByString("message")
	if err != nil {
		return fdm.FailureModel{}, fmt.Errorf("looking up message: %w", err)
	}
	msg, err := mn.AsString()
	if err != nil {
		return fdm.FailureModel{}, fmt.Errorf("reading message: %w", err)
	}
	f.Message = msg

	sn, err := n.LookupByString("stack")
	if err == nil {
		stack, err := sn.AsString()
		if err != nil {
			return fdm.FailureModel{}, fmt.Errorf("reading stack: %w", err)
		}
		f.Stack = &stack
	}

	return f, nil
}

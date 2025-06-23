package test

import (
	// for schema embed
	_ "embed"

	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/fam/api/capabilities/util"
)

//go:embed schema.ipldsch
var schemaBytes []byte

var ts = util.MustLoadSchema(schemaBytes)

func PingCaveatsType() ipldschema.Type {
	return ts.TypeByName("PingCaveats")
}

func PingOkType() ipldschema.Type {
	return ts.TypeByName("PingOk")
}

package capabilities

import (
	// for schema embed
	_ "embed"

	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/fam/capabilities/util"
)

//go:embed schema.ipldsch
var schemaBytes []byte

var ts = util.MustLoadSchema(schemaBytes)

func SendCaveatsType() ipldschema.Type {
	return ts.TypeByName("SendCaveats")
}

func SendOkType() ipldschema.Type {
	return ts.TypeByName("SendOk")
}

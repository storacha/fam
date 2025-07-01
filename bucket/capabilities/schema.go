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

func ListCaveatsType() ipldschema.Type {
	return ts.TypeByName("ListCaveats")
}

func ListOkType() ipldschema.Type {
	return ts.TypeByName("ListOk")
}

func ListResultType() ipldschema.Type {
	return ts.TypeByName("ListResult")
}

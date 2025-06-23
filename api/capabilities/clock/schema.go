package clock

import (

	// for schema embed
	_ "embed"

	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/fam/api/capabilities/util"
)

//go:embed schema.ipldsch
var schemaBytes []byte

var ts = util.MustLoadSchema(schemaBytes)

func HeadCaveatsType() ipldschema.Type {
	return ts.TypeByName("HeadCaveats")
}

func OkType() ipldschema.Type {
	return ts.TypeByName("Ok")
}

func ErrType() ipldschema.Type {
	return ts.TypeByName("Err")
}

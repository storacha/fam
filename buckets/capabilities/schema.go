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

func ListErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

func BucketType() ipldschema.Type {
	return ts.TypeByName("Bucket")
}

func AddCaveatsType() ipldschema.Type {
	return ts.TypeByName("AddCaveats")
}

func AddOkType() ipldschema.Type {
	return ts.TypeByName("AddOk")
}

func AddErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

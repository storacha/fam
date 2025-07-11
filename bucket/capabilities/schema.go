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

func EntriesCaveatsType() ipldschema.Type {
	return ts.TypeByName("EntriesCaveats")
}

func EntriesOkType() ipldschema.Type {
	return ts.TypeByName("EntriesOk")
}

func EntriesErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

func EntryType() ipldschema.Type {
	return ts.TypeByName("Entry")
}

func RootCaveatsType() ipldschema.Type {
	return ts.TypeByName("RootCaveats")
}

func RootOkType() ipldschema.Type {
	return ts.TypeByName("RootOk")
}

func RootErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

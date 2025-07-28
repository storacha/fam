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

func PutCaveatsType() ipldschema.Type {
	return ts.TypeByName("PutCaveats")
}

func PutOkType() ipldschema.Type {
	return ts.TypeByName("PutOk")
}

func PutErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

func DeleteCaveatsType() ipldschema.Type {
	return ts.TypeByName("DeleteCaveats")
}

func DeleteOkType() ipldschema.Type {
	return ts.TypeByName("DeleteOk")
}

func DeleteErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

func GetCaveatsType() ipldschema.Type {
	return ts.TypeByName("GetCaveats")
}

func GetOkType() ipldschema.Type {
	return ts.TypeByName("GetOk")
}

func GetErrType() ipldschema.Type {
	return ts.TypeByName("Any")
}

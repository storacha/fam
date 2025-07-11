package util

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-libstoracha/capabilities/types"
)

func MustLoadSchema(s []byte) *schema.TypeSystem {
	ts, err := types.LoadSchemaBytes(s)
	if err != nil {
		panic(fmt.Errorf("loading schema: %w\n%s", err, string(s)))
	}
	return ts
}

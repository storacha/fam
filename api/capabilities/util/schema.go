package util

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
)

func MustLoadSchema(s []byte) *schema.TypeSystem {
	ts, err := ipld.LoadSchemaBytes(s)
	if err != nil {
		panic(fmt.Errorf("loading schema: %w\n%s\n", string(s), err))
	}
	return ts
}

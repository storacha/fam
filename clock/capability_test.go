package clock

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ipld/go-ipld-prime/codec/dagjson"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/storacha/fam/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRoundTripHeadCaveats(t *testing.T) {
	nb := HeadCaveats{}

	node, err := nb.ToIPLD()
	require.NoError(t, err)

	buf := bytes.NewBuffer([]byte{})
	err = dagjson.Encode(node, buf)
	require.NoError(t, err)

	fmt.Println(buf.String())

	builder := basicnode.Prototype.Any.NewBuilder()
	err = dagjson.Decode(builder, buf)
	require.NoError(t, err)

	rnb, err := HeadCaveatsReader.Read(builder.Build())
	require.NoError(t, err)
	require.Equal(t, nb, rnb)
}

func TestRoundTripAdvanceCaveats(t *testing.T) {
	nb := AdvanceCaveats{
		Event: testutil.RandomLink(t),
	}

	node, err := nb.ToIPLD()
	require.NoError(t, err)

	buf := bytes.NewBuffer([]byte{})
	err = dagjson.Encode(node, buf)
	require.NoError(t, err)

	fmt.Println(buf.String())

	builder := basicnode.Prototype.Any.NewBuilder()
	err = dagjson.Decode(builder, buf)
	require.NoError(t, err)

	rnb, err := AdvanceCaveatsReader.Read(builder.Build())
	require.NoError(t, err)
	require.Equal(t, nb, rnb)
}

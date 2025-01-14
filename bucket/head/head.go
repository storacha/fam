package head

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func Marshal(head []ipld.Link) ([]byte, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	la, err := nb.BeginList(int64(len(head)))
	if err != nil {
		return nil, err
	}
	for _, l := range head {
		err := la.AssembleValue().AssignLink(l)
		if err != nil {
			return nil, err
		}
	}
	err = la.Finish()
	if err != nil {
		return nil, err
	}

	n := nb.Build()
	buf := bytes.NewBuffer([]byte{})
	err = dagcbor.Encode(n, buf)
	if err != nil {
		return nil, fmt.Errorf("CBOR encoding: %w", err)
	}
	return buf.Bytes(), nil
}

func Unmarshal(b []byte) ([]ipld.Link, error) {
	var head []ipld.Link

	np := basicnode.Prototype.List
	nb := np.NewBuilder()
	err := dagcbor.Decode(nb, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("decoding shard: %w", err)
	}
	n := nb.Build()

	values := n.ListIterator()
	if values == nil {
		return nil, errors.New("not a list")
	}
	for {
		if values.Done() {
			break
		}
		_, n, err := values.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating links: %w", err)
		}
		link, err := n.AsLink()
		if err != nil {
			return nil, fmt.Errorf("decoding link: %w", err)
		}
		head = append(head, link)
	}

	return head, nil
}

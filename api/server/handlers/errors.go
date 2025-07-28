package handlers

import (
	"fmt"

	"github.com/storacha/go-ucanto/core/ipld"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
)

type RootConflict struct {
	expected ipld.Link
	actual   ipld.Link
}

func (rc RootConflict) Name() string {
	return "RootConflict"
}

func (rc RootConflict) Error() string {
	return fmt.Sprintf("root conflict, expected %s, actual: %s", rc.expected.String(), rc.actual.String())
}

func (rc RootConflict) ToIPLD() (ipld.Node, error) {
	name := rc.Name()
	f := fdm.FailureModel{Name: &name, Message: rc.Error()}
	return f.ToIPLD()
}

func NewRootConflictError(expected, actual ipld.Link) *RootConflict {
	return &RootConflict{expected: expected, actual: actual}
}

type NotFound struct{}

func (nf NotFound) Name() string {
	return "NotFound"
}

func (nf NotFound) Error() string {
	return "not found"
}

func (nf NotFound) ToIPLD() (ipld.Node, error) {
	name := nf.Name()
	f := fdm.FailureModel{Name: &name, Message: nf.Error()}
	return f.ToIPLD()
}

func NewNotFoundError() *NotFound {
	return &NotFound{}
}

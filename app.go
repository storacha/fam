package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/storacha/fam/bucket"
	"github.com/storacha/fam/store"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
)

var log = logging.Logger("app")

type NodeBuilder interface {
	ToIPLD() (datamodel.Node, error)
}

// App struct
type App struct {
	ctx      context.Context
	userdata *store.UserDataStore
}

// NewApp creates a new App application struct
func NewApp() *App {
	return &App{}
}

// startup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalln("getting user home directory: %w", err)
	}

	dataDir, err := mkdirp(homeDir, ".fam")
	if err != nil {
		log.Fatalln("creating data directory: %w", err)
	}

	dstore, err := leveldb.NewDatastore(dataDir, nil)
	if err != nil {
		log.Fatalln("creating datastore: %w", err)
	}

	userdata, err := store.NewUserDataStore(ctx, dstore)
	if err != nil {
		log.Fatalln(err)
	}

	a.userdata = userdata
}

func (a *App) shutdown(ctx context.Context) {
	err := a.userdata.Close()
	if err != nil {
		log.Errorln(err)
	}
}

func (a *App) ID() (string, error) {
	signer, err := a.userdata.ID(a.ctx)
	if err != nil {
		log.Error(err)
		return "", err
	}
	return marshalJSON(Bytes(signer.Encode()))
}

func (a *App) Buckets() (string, error) {
	buckets, err := a.userdata.Buckets(a.ctx)
	if err != nil {
		log.Error(err)
		return "", err
	}
	return marshalJSON(Buckets(buckets))
}

func (a *App) AddBucket(params string) (string, error) {
	proof, err := unmarshalAddBucketParams(params)
	if err != nil {
		log.Error(err)
		return "", err
	}
	id, err := a.userdata.AddBucket(a.ctx, proof)
	if err != nil {
		log.Error(err)
		return "", err
	}

	return marshalJSON(Bytes(id.Bytes()))
}

func (a *App) RemoveBucket(params string) error {
	id, err := unmarshalRemoveBucketParams(params)
	if err != nil {
		log.Error(err)
		return err
	}
	return a.userdata.RemoveBucket(a.ctx, id)
}

func (a *App) Root(params string) (string, error) {
	id, err := unmarshalRootParams(params)
	if err != nil {
		log.Error(err)
		return "", err
	}

	bk, err := a.userdata.Bucket(a.ctx, id)
	if err != nil {
		log.Error(err)
		return "", err
	}

	return marshalJSON(Bytes(bk.Root().Binary()))
}

func (a *App) Put(params string) (string, error) {
	id, key, value, err := unmarshalPutParams(params)
	if err != nil {
		log.Error(err)
		return "", err
	}

	bk, err := a.userdata.Bucket(a.ctx, id)
	if err != nil {
		log.Error(err)
		return "", err
	}

	err = bk.Put(a.ctx, key, value)
	if err != nil {
		log.Error(err)
		return "", err
	}

	return marshalJSON(Bytes(bk.Root().Binary()))
}

func (a *App) Del(params string) (string, error) {
	return "", nil
}

func (a *App) Entries(params string) (string, error) {
	id, options, err := unmarshalEntriesParams(params)
	if err != nil {
		log.Error(err)
		return "", err
	}

	size := options.Size
	if size == 0 {
		size = 10
	}

	bk, err := a.userdata.Bucket(a.ctx, id)
	if err != nil {
		log.Error(err)
		return "", err
	}

	var opts []bucket.EntriesOption
	if options.Prefix != "" {
		opts = append(opts, bucket.WithKeyPrefix(options.Prefix))
	} else {
		if options.GreaterThan != "" {
			opts = append(opts, bucket.WithKeyGreaterThan(options.GreaterThan))
		} else if options.GreaterThanOrEqual != "" {
			opts = append(opts, bucket.WithKeyGreaterThanOrEqual(options.GreaterThanOrEqual))
		}
		if options.LessThan != "" {
			opts = append(opts, bucket.WithKeyLessThan(options.LessThan))
		} else if options.LessThanOrEqual != "" {
			opts = append(opts, bucket.WithKeyLessThanOrEqual(options.LessThanOrEqual))
		}
	}

	var entries Entries
	for e, err := range bk.Entries(a.ctx, opts...) {
		if err != nil {
			log.Error(err)
			return "", err
		}
		entries = append(entries, Entry(e))
	}

	if len(entries) == 0 {
		return marshalJSON(entries)
	}

	start := options.Page * size
	if start > int64(len(entries)-1) {
		return marshalJSON(Entries{})
	}

	end := start + size
	if end > int64(len(entries)) {
		end = int64(len(entries))
	}

	return marshalJSON(entries[start:end])
}

type Bytes []byte

func (a Bytes) ToIPLD() (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	err := nb.AssignBytes(a)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

type Buckets map[did.DID]delegation.Delegation

func (bks Buckets) ToIPLD() (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, err := nb.BeginMap(int64(len(bks)))
	if err != nil {
		return nil, err
	}
	for id, dlg := range bks {
		err = ma.AssembleKey().AssignString(id.String())
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(dlg.Archive())
		if err != nil {
			return nil, err
		}
		err = ma.AssembleValue().AssignBytes(b)
		if err != nil {
			return nil, err
		}
	}
	err = ma.Finish()
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func unmarshalAddBucketParams(input string) (delegation.Delegation, error) {
	np := basicnode.Prototype.Bytes
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return nil, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()
	b, err := n.AsBytes()
	if err != nil {
		return nil, err
	}
	return delegation.Extract(b)
}

func unmarshalRemoveBucketParams(input string) (did.DID, error) {
	np := basicnode.Prototype.Bytes
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return did.Undef, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()
	b, err := n.AsBytes()
	if err != nil {
		return did.Undef, err
	}
	return did.Decode(b)
}

func unmarshalRootParams(input string) (did.DID, error) {
	np := basicnode.Prototype.Bytes
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return did.Undef, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()
	b, err := n.AsBytes()
	if err != nil {
		return did.Undef, err
	}
	return did.Decode(b)
}

func unmarshalPutParams(input string) (did.DID, string, ipld.Link, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()

	idn, err := n.LookupByString("id")
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("looking up id: %w", err)
	}
	idBytes, err := idn.AsBytes()
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("decoding id as bytes: %w", err)
	}
	id, err := did.Decode(idBytes)
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("decoding id as DID: %w", err)
	}

	kn, err := n.LookupByString("key")
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("looking up key: %w", err)
	}
	key, err := kn.AsString()
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("decoding key as string: %w", err)
	}

	vn, err := n.LookupByString("value")
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("looking up value: %w", err)
	}
	value, err := vn.AsLink()
	if err != nil {
		return did.Undef, "", nil, fmt.Errorf("decoding value as link: %w", err)
	}

	return id, key, value, nil
}

type EntriesOptions struct {
	Size               int64
	Page               int64
	Prefix             string
	GreaterThan        string
	GreaterThanOrEqual string
	LessThan           string
	LessThanOrEqual    string
}

type Entries []Entry

func (e Entries) ToIPLD() (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	la, err := nb.BeginList(int64(len(e)))
	if err != nil {
		return nil, err
	}
	for _, ent := range e {
		n, err := ent.ToIPLD()
		if err != nil {
			return nil, err
		}
		la.AssembleValue().AssignNode(n)
	}
	err = la.Finish()
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

type Entry struct {
	Key   string
	Value ipld.Link
}

func (e Entry) ToIPLD() (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	la, err := nb.BeginList(2)
	if err != nil {
		return nil, err
	}
	err = la.AssembleValue().AssignString(e.Key)
	if err != nil {
		return nil, err
	}
	err = la.AssembleValue().AssignLink(e.Value)
	if err != nil {
		return nil, err
	}
	err = la.Finish()
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func unmarshalEntriesParams(input string) (did.DID, EntriesOptions, error) {
	np := basicnode.Prototype.Map
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return did.Undef, EntriesOptions{}, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()

	idn, err := n.LookupByString("id")
	if err != nil {
		return did.Undef, EntriesOptions{}, fmt.Errorf("looking up id: %w", err)
	}
	idBytes, err := idn.AsBytes()
	if err != nil {
		return did.Undef, EntriesOptions{}, fmt.Errorf("decoding id as bytes: %w", err)
	}
	id, err := did.Decode(idBytes)
	if err != nil {
		return did.Undef, EntriesOptions{}, fmt.Errorf("decoding id as DID: %w", err)
	}

	options := EntriesOptions{}
	sn, err := n.LookupByString("size")
	if err == nil {
		options.Size, err = sn.AsInt()
		if err != nil {
			return did.Undef, EntriesOptions{}, fmt.Errorf("decoding size as int: %w", err)
		}
	}
	pgn, err := n.LookupByString("page")
	if err == nil {
		options.Page, err = pgn.AsInt()
		if err != nil {
			return did.Undef, EntriesOptions{}, fmt.Errorf("decoding page as int: %w", err)
		}
	}
	pn, err := n.LookupByString("prefix")
	if err == nil {
		options.Prefix, err = pn.AsString()
		if err != nil {
			return did.Undef, EntriesOptions{}, fmt.Errorf("decoding prefix as string: %w", err)
		}
	}

	return id, options, nil
}

func mkdirp(dirpath ...string) (string, error) {
	dir := path.Join(dirpath...)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", fmt.Errorf("creating directory: %s: %w", dir, err)
	}
	return dir, nil
}

func marshalJSON(data NodeBuilder) (string, error) {
	n, err := data.ToIPLD()
	if err != nil {
		return "", err
	}
	buf := bytes.NewBuffer([]byte{})
	err = dagjson.Encode(n, buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

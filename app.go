package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/storacha/fam/store"
)

var log = logging.Logger("app")

func mustParse(s string) ipld.Link {
	c, err := cid.Parse(s)
	if err != nil {
		panic(err)
	}
	return cidlink.Link{Cid: c}
}

var placeholderEntries = Entries{
	{"5097.gif", mustParse("bafkreieshqhbxum6fjp6r4rsw3ozx5mhjchydai5e7aags4qv5r4vazeki")},
	{"658.png", mustParse("bafkreidfrmbg7ps5ezghd6aeocexwni7y7yzzj76m5xixnfcf66s3k6a44")},
	{"83537d1a1c776edce3d1d88fb4ce1db2.jpg", mustParse("bafkreiaefxrvhyafdb7grsury5ri7h7jsnqbwwrtig33hiie3bzxclgk4i")},
	{"aaaaa-very-long-key-47825816-5eba1b80-dd74-11e8-8a31-c9db9f433c5d.jpg", mustParse("bafkreie3pgku5kzswaipjkzrstb3gzskqj27dbzmmzyynxjfhmnltn6sky")},
	{"B5ipWhjCUAAuzFq.jpg_large.jpeg", mustParse("bafkreictmrzvrpaeixcqpptfikn6kd3ykryahxu7qe4wn3kqnslthp2wbe")},
	{"CmRktjbWEAQEi3h.jpg", mustParse("bafkreicpeys5qln5komkbnymbjw2t4viju7kp6jlqgotvc2w5t4wv6dhiu")},
	{"DU0imAZWAAIfi7S.jpg large.jpg", mustParse("bafkreigmrgly3k5fez2pbkqhdujado6hdejkbz4hxjmva7iiyi437gq2fy")},
	{"E6ugdPkWUAAp5Sp.jpg", mustParse("bafkreibtsod63vtdyyq5iwfblycy6gk2te5n3lr6k6orymxp23x6cken3e")},
	{"_inconceivable_princess_bride.gif", mustParse("bafybeig3lnyc23n7vcvm754od3bqlhx62zmesrgqgrjskovtydefb2v2f4")},
	{"birfdaze.gif", mustParse("bafybeibepw6ne3a4jgwthjmsg3pfh7uonu3yqn2fvinswwxjr7epa5sj3y")},
	{"centralized-decentralized-distributed.gif", mustParse("bafybeic5ec5e3j6urosvnd6lucs5clqrows2augnovzlzxgch2skwbe2se")},
	{"comic/aliensb3ta.jpg", mustParse("bafkreibwp3p5adaxnk2y5ecqliqq3sqmwe66j2cxcmykn3tnxewdc47hie")},
	{"comic/battleelephant.jpg", mustParse("bafkreidqychd3wyw4rixs2avqdkvlp6q7is4w3c6q2ef5h4hx77rkmm6xa")},
	{"comic/cowiseatinghuman.jpg", mustParse("bafkreicpfqmunngoi5vixmfhbngefx5sdpo4tqbtbbdxdrgyuosohbki3i")},
	{"comic/donotresist.jpg", mustParse("bafkreiejwbzaebwz36nbxndyjxmlxbngkj273wgbywzhquybxgkm5julha")},
	{"comic/giantcat.jpg", mustParse("bafkreia7wmluhebzfayp66yxdkaz5rp57pezn4ffksdth6qt6f2cl67f2a")},
	{"comic/naughtylion.jpg", mustParse("bafkreibfhit3emjewk2rzlibpxb6wiufz42pq2atofaa2eo3anqwfxvaui")},
	{"comic/pinpie.jpg", mustParse("bafkreiajkbmpugz75eg2tmocmp3e33sg5kuyq2amzngslahgn6ltmqxxfa")},
	{"comic/seamonster.jpg", mustParse("bafkreifhyo4ufquwtoslssrq33xd2oqf3efhsd4zhux4q2tnoibn7ghsiq")},
	{"comic/sillydinosaur.jpg", mustParse("bafkreifoj4o4ymxkgzsg7oxi2ygqzesbeym6dek6v4ilfpobtmtpq5hppi")},
	{"comic/yellowandgiant.jpg", mustParse("bafkreiclmncicyhuvouq4uy7m5522kzopgveu4nifsypsyzpols4sr5eka")},
	{"comic/youareanonsense.jpg", mustParse("bafkreibgj6uwfebncr524o5djgt5ibx2lru4gns3lsoy7fy5ds35zrvk24")},
	{"comic/youarelarge.jpg", mustParse("bafkreig7fkwfagyrm2ahj56pemkrt5dhso4njmwne7dxizear4777apxee")},
	{"dr-is-tired.jpg", mustParse("bafkreiabltrd5zm73pvi7plq25pef3hm7jxhbi3kv4hapegrkfpkqtkbme")},
	{"everythingisfine.jpg", mustParse("bafkreiglesyr4audbcg24myztyfsar7yxbifh4hwwozju6txywzjfma2mi")},
	{"giphy.gif", mustParse("bafkreicti474y2qbzo5r2ay3yu6inpjbwuzxcrzifogwhyi7k2ix7ag5qm")},
	{"lost-dog.jpg", mustParse("bafkreibkz6773xrnhsfu4fmzotb57zsuanuzq5gm3zwiwpaafom5qzn67e")},
	{"meat-skeleton.jpg", mustParse("bafkreigooa3finvkgkxhjtwequyw7javywdh2alstbehshm7oady4pj2hi")},
	{"mercator.jpg", mustParse("bafkreidvlc7lkpu2hudqhprdz3cvufdppboq2tnkgo4h44yiogmljzlw7u")},
	{"post-42510-IT-Crowd-Maurice-Moss-fire-gif-6zWo.gif", mustParse("bafybeif4xubsfb4sxadlwuxvmcim5nzkhadazcfrmyg5lgyi46j3oavgjy")},
	{"pug-pony.jpg", mustParse("bafkreigg4a4z7o5m5pwzcfyphodsbbdp5sdiu5bwibdw5wvq5t24qswula")},
	{"room-guardian.jpg", mustParse("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")},
	{"stacktrace-or-gtfo.jpg", mustParse("bafkreiev7xx6gdmb6xb2vz5nmnf3qikqlfyopg6o7fdweozc2ptkvsiuyi")},
	{"tumblr_mxlzazrsm01s373hwo1_250.gif", mustParse("bafkreidnno4baihsy67zfcqomgsuy3shj3ri6do7tlaomtozqsewqobp6u")},
	{"yesthisisdog.jpg", mustParse("bafkreiem4twkqzsq2aj4shbycd4yvoj2cx72vezicletlhi7dijjciqpui")},
	{"you-can-do-it.jpg", mustParse("bafkreia73pstorxgufdvdzdyegjkcyekwyzrhrhjwby6okmvtzhjj6ttfq")},
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

func (a *App) Buckets() (string, error) {
	return "", nil
}

func (a *App) AddBucket(params string) (string, error) {
	return "", nil
}

func (a *App) RemoveBucket(params string) (string, error) {
	return "", nil
}

func (a *App) Put(params string) (string, error) {
	return "", nil
}

func (a *App) Del(params string) (string, error) {
	return "", nil
}

func (a *App) Entries(params string) (string, error) {
	_, options, err := decodeEntriesParams(params)
	if err != nil {
		return "", err
	}

	size := options.Size
	if size == 0 {
		size = 10
	}

	var entries Entries
	for _, e := range placeholderEntries {
		if !strings.HasPrefix(e.Key, options.Prefix) {
			continue
		}
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		return encodeEntries(entries)
	}

	start := options.Page * size
	if start > int64(len(entries)-1) {
		return encodeEntries(Entries{})
	}

	end := start + size
	if end > int64(len(entries)) {
		end = int64(len(entries))
	}

	return encodeEntries(entries[start:end])
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

func encodeEntries(entries Entries) (string, error) {
	n, err := entries.ToIPLD()
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

func decodeEntriesParams(input string) (ipld.Link, EntriesOptions, error) {
	np := basicnode.Prototype.Map
	nb := np.NewBuilder()
	err := dagjson.Decode(nb, bytes.NewReader([]byte(input)))
	if err != nil {
		return nil, EntriesOptions{}, fmt.Errorf("decoding params: %w", err)
	}
	n := nb.Build()

	rn, err := n.LookupByString("root")
	if err != nil {
		return nil, EntriesOptions{}, fmt.Errorf("looking up root: %w", err)
	}
	root, err := rn.AsLink()
	if err != nil {
		return nil, EntriesOptions{}, fmt.Errorf("decoding root as link: %w", err)
	}

	options := EntriesOptions{}
	sn, err := n.LookupByString("size")
	if err == nil {
		options.Size, err = sn.AsInt()
		if err != nil {
			return nil, EntriesOptions{}, fmt.Errorf("decoding size as int: %w", err)
		}
	}
	pgn, err := n.LookupByString("page")
	if err == nil {
		options.Page, err = pgn.AsInt()
		if err != nil {
			return nil, EntriesOptions{}, fmt.Errorf("decoding page as int: %w", err)
		}
	}
	pn, err := n.LookupByString("prefix")
	if err == nil {
		options.Prefix, err = pn.AsString()
		if err != nil {
			return nil, EntriesOptions{}, fmt.Errorf("decoding prefix as string: %w", err)
		}
	}

	return root, options, nil
}

func mkdirp(dirpath ...string) (string, error) {
	dir := path.Join(dirpath...)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", fmt.Errorf("creating directory: %s: %w", dir, err)
	}
	return dir, nil
}

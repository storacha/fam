package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
	famclient "github.com/storacha/fam/api/client"
	"github.com/storacha/fam/block"
	"github.com/storacha/fam/principal/ed25519/pem"
	"github.com/storacha/fam/store"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ucaned "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
)

var bootLog = logging.Logger("api/bootstrap")

var (
	ErrNotRunning     = errors.New("service is not running")
	ErrAlreadyRunning = errors.New("service is already running")
)

var (
	ucanServerURL  = "http://0.0.0.0:5224"
	p2pListenAddrs = []string{
		"/ip4/0.0.0.0/tcp/5225",
		"/ip4/0.0.0.0/udp/5225/quic-v1",
		"/ip4/0.0.0.0/udp/5225/quic-v1/webtransport",
		"/ip4/0.0.0.0/udp/5225/webrtc-direct",
		"/ip6/::/tcp/5225",
		"/ip6/::/udp/5225/quic-v1",
		"/ip6/::/udp/5225/quic-v1/webtransport",
		"/ip6/::/udp/5225/webrtc-direct",
	}
)

// ServiceInfo defines the service connection information.
type ServiceInfo struct {
	ID  did.DID
	URL url.URL
}

type serviceInfoJSON struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

func (info *ServiceInfo) UnmarshalJSON(b []byte) error {
	var data serviceInfoJSON
	var err error
	if err = json.Unmarshal(b, &data); err != nil {
		return err
	}
	info.ID, err = did.Parse(data.ID)
	if err != nil {
		return fmt.Errorf("parsing service ID: %w", err)
	}
	serviceURL, err := url.Parse(data.URL)
	if err != nil {
		return fmt.Errorf("parsing service URL: %w", err)
	}
	info.URL = *serviceURL
	return nil
}

func (info ServiceInfo) MarshalJSON() ([]byte, error) {
	var data serviceInfoJSON
	data.ID = info.ID.String()
	data.URL = info.URL.String()
	return json.Marshal(data)
}

func serviceInfoPath(dataDir string) string {
	return path.Join(dataDir, "service.info")
}

func readServiceInfo(dataDir string) (ServiceInfo, error) {
	infoPath := serviceInfoPath(dataDir)
	bootLog.Debugf("reading service info from: %s", infoPath)
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("reading file: %w", err)
	}
	var info ServiceInfo
	err = json.Unmarshal(data, &info)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("marshaling JSON: %w", err)
	}
	return info, err
}

func writeServiceInfo(dataDir string, id ucan.Principal, url url.URL) error {
	infoPath := serviceInfoPath(dataDir)
	bootLog.Debugf("writing service info to: %s", infoPath)
	info := ServiceInfo{ID: id.DID(), URL: url}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling JSON: %w", err)
	}
	err = os.WriteFile(infoPath, data, 0777)
	if err != nil {
		return fmt.Errorf("writing file: %w", err)
	}
	return nil
}

func deleteServiceInfo(dataDir string) error {
	infoPath := serviceInfoPath(dataDir)
	return os.Remove(infoPath)
}

func privateKeyPath(dataDir string) string {
	return path.Join(dataDir, "id.pem")
}

// IsRunning determines whether the service is running by reading the service
// info from the file in the passed data directory, and then trying to send a
// ping message. If it isn't able to get a successful pong it will return false.
func IsRunning(ctx context.Context, dataDir string) (bool, error) {
	info, err := readServiceInfo(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			bootLog.Debug("service is not running: service info not found")
			return false, nil
		}
		return false, err
	}
	bootLog.Debugf("service info: %s (%s)", info.ID, info.URL.String())
	conn, err := client.NewConnection(info.ID, ucanhttp.NewHTTPChannel(&info.URL))
	if err != nil {
		return false, err
	}
	signer, err := ucaned.Generate()
	if err != nil {
		return false, err
	}
	client := famclient.New(signer, conn, nil)
	msg := time.Now().Format(time.RFC3339)
	bootLog.Debugf("sending ping to verify run state...")
	rtt, err := client.Ping(ctx, msg)
	if err != nil {
		bootLog.Debugf("failed to ping service: %w", err)
		return false, nil
	}
	bootLog.Debugf("service is running, RTT: %s", rtt.String())
	return true, nil
}

// Connect will connect to the service if it is running.
func Connect(ctx context.Context, dataDir string) (client.Connection, error) {
	running, err := IsRunning(ctx, dataDir)
	if err != nil {
		return nil, fmt.Errorf("checking if API is already running: %w", err)
	}
	if !running {
		return nil, ErrNotRunning
	}
	info, err := readServiceInfo(dataDir)
	if err != nil {
		return nil, err
	}
	return client.NewConnection(info.ID, ucanhttp.NewHTTPChannel(&info.URL))
}

type FamServer struct {
	// config
	dataDir        string
	ucanServerURL  *url.URL
	p2pListenAddrs []multiaddr.Multiaddr
	// set after init
	id         principal.Signer
	userData   *store.UserDataStore
	p2pHost    host.Host
	ucanServer server.ServerView
}

func (fs *FamServer) init() error {
	id, err := pem.ReadPrivateKey(privateKeyPath(fs.dataDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s, err := ucaned.Generate()
			if err != nil {
				return fmt.Errorf("generating private key: %w", err)
			}
			err = pem.WritePrivateKey(privateKeyPath(fs.dataDir), s)
			if err != nil {
				return fmt.Errorf("writing private key: %w", err)
			}
			id = s
		} else {
			return fmt.Errorf("reading private key: %w", err)
		}
	}

	dstore, err := leveldb.NewDatastore(fs.dataDir, nil)
	if err != nil {
		return fmt.Errorf("creating datastore: %w", err)
	}

	sk, err := p2pcrypto.UnmarshalEd25519PrivateKey(id.Raw())
	if err != nil {
		return err
	}

	host, err := libp2p.New(libp2p.Identity(sk))
	if err != nil {
		return fmt.Errorf("creating p2p host: %w", err)
	}

	userdata, err := store.NewUserDataStore(id, dstore, host)
	if err != nil {
		return fmt.Errorf("creating user data store: %w", err)
	}

	server, err := NewServer(id, userdata, host)
	if err != nil {
		return fmt.Errorf("creating UCAN API server: %w", err)
	}

	fs.id = id
	fs.p2pHost = host
	fs.userData = userdata
	fs.ucanServer = server

	return nil
}

// Connect attempts to connect to the service if it is running. If it is not
// running then it will initialize the service and return a direct connection.
func (fs *FamServer) Connect(ctx context.Context) (client.Connection, error) {
	conn, err := Connect(ctx, fs.dataDir)
	if err != nil {
		if errors.Is(err, ErrNotRunning) {
			err = fs.init()
			if err != nil {
				return nil, fmt.Errorf("initializing service: %w", err)
			}
			return client.NewConnection(fs.id, fs.ucanServer)
		}
		return nil, err
	}
	return conn, nil
}

// Start will start the service if it is not already running.
func (fs *FamServer) Start(ctx context.Context) error {
	running, err := IsRunning(ctx, fs.dataDir)
	if err != nil {
		return fmt.Errorf("checking if API is already running: %w", err)
	}
	if running {
		return ErrAlreadyRunning
	}

	err = fs.init()
	if err != nil {
		return fmt.Errorf("initializing service: %w", err)
	}

	err = fs.p2pHost.Network().Listen(fs.p2pListenAddrs...)
	if err != nil {
		return fmt.Errorf("listening on p2p addresses: %w", err)
	}

	p2pListener, err := trustlessgateway.NewP2PListener(fs.p2pHost)
	if err != nil {
		return fmt.Errorf("creating p2p listener: %w", err)
	}

	tcpListener, err := net.Listen("tcp", fs.ucanServerURL.Host)
	if err != nil {
		return fmt.Errorf("creating TCP listener: %w", err)
	}

	url, err := url.Parse(fmt.Sprintf("http://%s", tcpListener.Addr().String()))
	if err != nil {
		return fmt.Errorf("parsing service URL: %w", err)
	}

	// write the service info
	err = writeServiceInfo(fs.dataDir, fs.id, *url)
	if err != nil {
		return fmt.Errorf("writing service info: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "🔥 fam %s\n", "v0.0.0") // TODO: add build version
		fmt.Fprint(w, "- https://github.com/storacha/fam\n")
		fmt.Fprintf(w, "- %s", fs.ucanServer.ID().DID())
	})
	mux.HandleFunc("OPTIONS /{_...}", func(w http.ResponseWriter, r *http.Request) {
		headers := w.Header()
		headers.Add("Access-Control-Allow-Origin", "*")
		headers.Add("Vary", "Origin")
		headers.Add("Vary", "Access-Control-Request-Method")
		headers.Add("Vary", "Access-Control-Request-Headers")
		headers.Add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		headers.Add("Access-Control-Allow-Headers", "*")
		headers.Add("Access-Control-Max-Age", "86400")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /", NewPostHandler(fs.ucanServer))
	mux.HandleFunc("GET /ipfs/{root}", trustlessgateway.NewHandler(&bucketBlockFetcher{fs.userData}))

	go func() {
		err := http.Serve(p2pListener, mux)
		if !errors.Is(err, http.ErrServerClosed) {
			bootLog.Error(err)
		}
	}()

	httpServer := &http.Server{Handler: mux}

	var stopErr error
	go func() {
		<-ctx.Done()
		err = deleteServiceInfo(fs.dataDir)
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("deleting service info: %w", err))
		}
		err = fs.p2pHost.Close()
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("closing p2p host: %w", err))
		}
		err = httpServer.Shutdown(ctx)
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("shutting down HTTP server: %w", err))
		}
	}()

	homedir, _ := os.UserHomeDir()

	fmt.Println(`
▄▖     
▙▖▀▌▛▛▌
▌ █▌▌▌▌🍗🍟
	`)
	fmt.Println("Data directory:")
	fmt.Printf("  %s\n", strings.Replace(fs.dataDir, homedir, "~", 1))
	fmt.Println("UCAN service:")
	fmt.Printf("  ID: %s\n", fs.id.DID().String())
	fmt.Printf("  URL: %s\n", url.String())
	fmt.Println("P2P service:")
	fmt.Printf("  ID: %s\n", fs.p2pHost.ID().String())
	fmt.Println("  Addrs:")
	for _, a := range fs.p2pListenAddrs {
		fmt.Printf("    %s\n", a.String())
	}

	err = httpServer.Serve(tcpListener)
	if !errors.Is(err, http.ErrServerClosed) {
		stopErr = errors.Join(stopErr, fmt.Errorf("serving HTTP: %w", err))
	}
	return stopErr
}

type config struct {
	ucanServerURL  *url.URL
	p2pListenAddrs []multiaddr.Multiaddr
}

type Option = func(c *config) error

func WithUCANServerURL(serverURL string) Option {
	return func(c *config) error {
		url, err := url.Parse(serverURL)
		if err != nil {
			return fmt.Errorf("parsing UCAN server URL: %w", err)
		}
		c.ucanServerURL = url
		return nil
	}
}

func WithP2PListenAddrs(addrs ...string) Option {
	return func(c *config) error {
		for _, a := range addrs {
			addr, err := multiaddr.NewMultiaddr(a)
			if err != nil {
				return fmt.Errorf("parsing multiaddr: %w", err)
			}
			c.p2pListenAddrs = append(c.p2pListenAddrs, addr)
		}
		return nil
	}
}

func New(dataDir string, opts ...Option) (*FamServer, error) {
	cfg := config{}
	for _, o := range opts {
		err := o(&cfg)
		if err != nil {
			return nil, err
		}
	}
	if cfg.ucanServerURL == nil {
		url, err := url.Parse(ucanServerURL)
		if err != nil {
			return nil, fmt.Errorf("parsing UCAN server URL: %w", err)
		}
		cfg.ucanServerURL = url
	}
	if len(cfg.p2pListenAddrs) == 0 {
		for _, a := range p2pListenAddrs {
			addr, err := multiaddr.NewMultiaddr(a)
			if err != nil {
				return nil, fmt.Errorf("parsing multiaddr: %w", err)
			}
			cfg.p2pListenAddrs = append(cfg.p2pListenAddrs, addr)
		}
	}

	return &FamServer{
		dataDir:        dataDir,
		p2pListenAddrs: cfg.p2pListenAddrs,
		ucanServerURL:  cfg.ucanServerURL,
	}, nil
}

type bucketBlockFetcher struct {
	userdata *store.UserDataStore
}

func (bf *bucketBlockFetcher) Get(ctx context.Context, link ipld.Link) (block.Block, error) {
	buckets, err := bf.userdata.Buckets(ctx)
	if err != nil {
		return nil, err
	}
	for id := range buckets {
		bs := bf.userdata.Blocks(id)
		b, err := bs.Get(ctx, link)
		if err == nil {
			return b, nil
		}
		if !errors.Is(err, block.ErrNotFound) {
			bootLog.Debugf("getting block: %w", err)
		}
	}
	return nil, block.ErrNotFound
}

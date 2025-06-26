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
	"time"

	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	famclient "github.com/storacha/fam/api/client"
	"github.com/storacha/fam/block"
	"github.com/storacha/fam/principal/ed25519/pem"
	"github.com/storacha/fam/store"
	"github.com/storacha/fam/trustlessgateway"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	ucaned "github.com/storacha/go-ucanto/principal/ed25519/signer"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
)

var bootLog = logging.Logger("api/bootstrap")

var (
	ErrNotRunning     = errors.New("service is not running")
	ErrAlreadyRunning = errors.New("service is already running")
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
	client := famclient.NewClient(signer, conn)
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

// Start either starts a new Fam API server.
func Start(ctx context.Context, dataDir string) error {
	running, err := IsRunning(ctx, dataDir)
	if err != nil {
		return fmt.Errorf("checking if API is already running: %w", err)
	}
	if running {
		return ErrAlreadyRunning
	}

	id, err := pem.ReadPrivateKey(privateKeyPath(dataDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s, err := ucaned.Generate()
			if err != nil {
				return fmt.Errorf("generating private key: %w", err)
			}
			err = pem.WritePrivateKey(privateKeyPath(dataDir), s)
			if err != nil {
				return fmt.Errorf("writing private key: %w", err)
			}
			id = s
		} else {
			return fmt.Errorf("reading private key: %w", err)
		}
	}

	dstore, err := leveldb.NewDatastore(dataDir, nil)
	if err != nil {
		return fmt.Errorf("creating datastore: %w", err)
	}

	sk, err := p2pcrypto.UnmarshalEd25519PrivateKey(id.Raw())
	if err != nil {
		return err
	}

	host, err := libp2p.New(
		libp2p.Identity(sk),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/5225",
			"/ip4/0.0.0.0/udp/5225/quic-v1",
			"/ip4/0.0.0.0/udp/5225/quic-v1/webtransport",
			"/ip4/0.0.0.0/udp/5225/webrtc-direct",
			"/ip6/::/tcp/5225",
			"/ip6/::/udp/5225/quic-v1",
			"/ip6/::/udp/5225/quic-v1/webtransport",
			"/ip6/::/udp/5225/webrtc-direct",
		),
	)
	if err != nil {
		return fmt.Errorf("creating p2p host: %w", err)
	}
	p2pListener, err := trustlessgateway.NewP2PListener(host)
	if err != nil {
		return fmt.Errorf("creating p2p listener: %w", err)
	}

	userdata, err := store.NewUserDataStore(ctx, id, dstore, host)
	if err != nil {
		return fmt.Errorf("creating user data store: %w", err)
	}

	blocks := bucketBlockFetcher{userdata}

	server, err := NewServer(id, userdata, host)
	if err != nil {
		return fmt.Errorf("creating UCAN API server: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "🔥 fam %s\n", "v0.0.0") // TODO: add build version
		fmt.Fprint(w, "- https://github.com/storacha/fam\n")
		fmt.Fprintf(w, "- %s", server.ID().DID())
	})
	mux.HandleFunc("POST /", NewHandler(server))
	mux.HandleFunc("GET /ipfs/{root}", trustlessgateway.NewHandler(&blocks))

	go func() {
		err := http.Serve(p2pListener, mux)
		if !errors.Is(err, http.ErrServerClosed) {
			bootLog.Error(err)
		}
	}()

	tcpListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return fmt.Errorf("creating TCP listener: %w", err)
	}

	url, err := url.Parse(fmt.Sprintf("http://%s", tcpListener.Addr().String()))
	if err != nil {
		return fmt.Errorf("parsing service URL: %w", err)
	}

	bootLog.Info("UCAN service:")
	bootLog.Infof("  ID: %s", id.DID().String())
	bootLog.Infof("  URL: %s", url.String())

	bootLog.Info("P2P service:")
	bootLog.Infof("  ID: %s", host.ID().String())
	bootLog.Info("  Addrs:")

	for _, a := range host.Addrs() {
		bootLog.Infof("    %s", a.String())
	}

	// write the service info
	err = writeServiceInfo(dataDir, id, *url)
	if err != nil {
		return fmt.Errorf("writing service info: %w", err)
	}

	httpServer := &http.Server{Handler: mux}

	var stopErr error
	go func() {
		<-ctx.Done()
		err = deleteServiceInfo(dataDir)
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("deleting service info: %w", err))
		}
		err = host.Close()
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("closing p2p host: %w", err))
		}
		err = httpServer.Shutdown(ctx)
		if err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("shutting down HTTP server: %w", err))
		}
	}()

	err = httpServer.Serve(tcpListener)
	if !errors.Is(err, http.ErrServerClosed) {
		stopErr = errors.Join(stopErr, fmt.Errorf("serving HTTP: %w", err))
	}
	return stopErr
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

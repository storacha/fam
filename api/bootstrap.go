package api

import (
	"context"
	crypto_ed25519 "crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	logging "github.com/ipfs/go-log/v2"
	famclient "github.com/storacha/fam/api/client"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/http"
)

var bootLog = logging.Logger("api/bootstrap")

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
		return ServiceInfo{}, fmt.Errorf("reading service information from %s: %w", infoPath, err)
	}
	var info ServiceInfo
	err = json.Unmarshal(data, &info)
	return info, err
}

func privateKeyPath(dataDir string) string {
	return path.Join(dataDir, "id.pem")
}

func readPrivateKey(dataDir string) (principal.Signer, error) {
	pemData, err := os.ReadFile(privateKeyPath(dataDir))
	if err != nil {
		return nil, fmt.Errorf("reading private key: %w", err)
	}

	var privateKey *crypto_ed25519.PrivateKey
	rest := pemData

	// Loop until no more blocks
	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			// No more PEM blocks
			break
		}
		rest = remaining

		// Look for "PRIVATE KEY"
		if block.Type == "PRIVATE KEY" {
			parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse PKCS#8 private key: %w", err)
			}

			// We expect a ed25519 private key, cast it
			key, ok := parsedKey.(crypto_ed25519.PrivateKey)
			if !ok {
				return nil, fmt.Errorf("the parsed key is not an ED25519 private key")
			}
			privateKey = &key
			break
		}
	}

	if privateKey == nil {
		return nil, fmt.Errorf("could not find a PRIVATE KEY block in the PEM file")
	}
	return ed25519.FromRaw(*privateKey)
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
	bootLog.Debugf("service info: %s (%s)", info.ID, info.URL)
	conn, err := client.NewConnection(info.ID, http.NewHTTPChannel(&info.URL))
	if err != nil {
		return false, err
	}
	signer, err := ed25519.Generate()
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
	info, err := readServiceInfo(dataDir)
	if err != nil {
		return nil, err
	}
	return client.NewConnection(info.ID, http.NewHTTPChannel(&info.URL))
}

// Start either starts a new Fam API server or connects to an already
// running instance.
func Start(ctx context.Context, dataDir string) (client.Connection, error) {
	running, err := IsRunning(ctx, dataDir)
	if err != nil {
		return nil, fmt.Errorf("checking if API is already running: %w", err)
	}
	if running {
		return Connect(ctx, dataDir)
	}

	signer, err := readPrivateKey(dataDir)
	if err != nil {
		return nil, fmt.Errorf("reading private key: %w", err)
	}

}

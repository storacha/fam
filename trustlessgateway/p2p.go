package trustlessgateway

import (
	"net"

	gostream "github.com/libp2p/go-libp2p-gostream"
	p2phttp "github.com/libp2p/go-libp2p-http"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const Protocol = protocol.ID("/libp2p-http/ipfs-trustless-gateway/0.0.1")

func NewP2PTransport(host host.Host) *p2phttp.RoundTripper {
	return p2phttp.NewTransport(host, p2phttp.ProtocolOption(Protocol))
}

func NewP2PListener(host host.Host) (net.Listener, error) {
	return gostream.Listen(host, Protocol)
}

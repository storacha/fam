package trustlessgateway

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/fam/block"
)

const AcceptRaw = "application/vnd.ipld.raw"

func NewServer(blocks block.Fetcher) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ipfs/{root}", NewHandler(blocks))
	mux.HandleFunc("GET /ipfs/{root}/{rest...}", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "path traversal not implemented", http.StatusNotImplemented)
	})
	return mux, nil
}

func NewHandler(blocks block.Fetcher) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !(isAcceptRaw(r) || isFormatRaw(r)) {
			http.Error(w, "non-raw response not implemented", http.StatusNotImplemented)
			return
		}
		root, err := cid.Parse(r.PathValue("root"))
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid CID: %s", err.Error()), http.StatusBadRequest)
			return
		}
		b, err := blocks.Get(r.Context(), cidlink.Link{Cid: root})
		if err != nil {
			if errors.Is(err, block.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			log.Errorf("getting block: %w", err)
			http.Error(w, "failed to get block", http.StatusInternalServerError)
			return
		}
		_, err = w.Write(b.Bytes())
		if err != nil {
			log.Errorf("writing block: %w", err)
		}
	}
}

func isAcceptRaw(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), AcceptRaw)
}

func isFormatRaw(r *http.Request) bool {
	return r.URL.Query().Get("format") == "raw"
}

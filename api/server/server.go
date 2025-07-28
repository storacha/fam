package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/storacha/fam/api/server/handlers"
	"github.com/storacha/fam/store"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/server"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
)

var log = logging.Logger("api")

type ServerInfo struct {
	ID    string    `json:"id"`
	Build BuildInfo `json:"build"`
}

type BuildInfo struct {
	Version string `json:"version"`
	Repo    string `json:"repo"`
}

func NewServeMux(server server.ServerView) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", NewGetHandler(server))
	mux.HandleFunc("POST /", NewPostHandler(server))
	return mux, nil
}

func NewGetHandler(server server.ServerView) http.HandlerFunc {
	info := ServerInfo{
		ID: server.ID().DID().String(),
		Build: BuildInfo{
			Version: "v0.0.0", // TODO: add build version
			Repo:    "https://github.com/storacha/fam",
		},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept"), "application/json") {
			w.Header().Set("Content-Type", "application/json")
			data, err := json.Marshal(&info)
			if err != nil {
				log.Errorf("failed JSON marshal server info: %w", err)
				http.Error(w, "failed JSON marshal server info", http.StatusInternalServerError)
				return
			}
			w.Write(data)
		} else {
			fmt.Fprintf(w, "🔥 fam UCAN RPC %s\n", info.Build.Version)
			fmt.Fprintf(w, "- %s\n", info.Build.Repo)
			fmt.Fprintf(w, "- %s", info.ID)
		}
	}
}

func NewOptionsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		headers := w.Header()
		headers.Add("Access-Control-Allow-Origin", "*")
		headers.Add("Vary", "Origin")
		headers.Add("Vary", "Access-Control-Request-Method")
		headers.Add("Vary", "Access-Control-Request-Headers")
		headers.Add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		headers.Add("Access-Control-Allow-Headers", "*")
		headers.Add("Access-Control-Max-Age", "86400")
		w.WriteHeader(http.StatusOK)
	}
}

func NewPostHandler(server server.ServerView) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		res, err := server.Request(r.Context(), ucanhttp.NewHTTPRequest(r.Body, r.Header))
		if err != nil {
			log.Errorf("handling UCAN request: %w", err)
			http.Error(w, "failed to handle UCAN request", http.StatusInternalServerError)
			return
		}

		for key, vals := range res.Headers() {
			for _, v := range vals {
				w.Header().Add(key, v)
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		_, err = io.Copy(w, res.Body())
		if err != nil {
			log.Errorf("sending UCAN response: %w", err)
			http.Error(w, "failed to send UCAN response", http.StatusInternalServerError)
			return
		}
	}
}

// NewServer creates a new UCAN RPC server.
func NewServer(agent principal.Signer, userdata *store.UserDataStore, host host.Host) (server.ServerView, error) {
	var options []server.Option
	options = append(options, handlers.Bucket(userdata)...)
	options = append(options, handlers.Buckets(userdata)...)
	options = append(options, handlers.Clock(userdata, host)...)
	options = append(options, handlers.Ping()...)
	options = append(options, server.WithErrorHandler(func(err server.HandlerExecutionError[any]) {
		log.Errorf("%s: %s", err.Name(), err.Error())
	}))
	return server.NewServer(agent, options...)
}

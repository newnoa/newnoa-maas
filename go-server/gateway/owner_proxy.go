package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const ownerTunnelPrefix = "/_newnoa/owner"

var ownerTunnelPath = regexp.MustCompile(`^/tunnels/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/([1-9][0-9]*)/(evaluation/|probe/|publication-probe/)?v1/(chat/completions|responses|messages|embeddings|rerank|audio/transcriptions|systemone|images/generations|images/edits|models)$`)

func newOwnerTunnelProxy(token string, target *url.URL) http.Handler {
	proxy := &httputil.ReverseProxy{
		FlushInterval: -1,
		Transport: withImageResponseTimeout(&http.Transport{
			Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 60 * time.Second,
		}),
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.URL.Path = strings.TrimPrefix(request.In.URL.Path, ownerTunnelPrefix)
			request.Out.URL.RawPath = ""
			request.Out.URL.RawQuery = ""
			request.Out.Host = target.Host
			request.Out.Header = make(http.Header)
			for _, name := range []string{"Authorization", "Content-Type", "Anthropic-Version", "Anthropic-Beta", "X-Newnoa-Request", "X-Newnoa-Permit", "X-Newnoa-Gateway", "X-Newnoa-Epoch"} {
				for _, value := range request.In.Header.Values(name) {
					request.Out.Header.Add(name, value)
				}
			}
			// Even empty GET probes must never replay a one-use permit on a reused connection.
			request.Out.GetBody = nil
			if request.Out.Body == nil || request.Out.Body == http.NoBody {
				request.Out.Body = io.NopCloser(bytes.NewReader(nil))
				request.Out.ContentLength = -1
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) { fail(w, 502, "Owner bridge unavailable") },
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if token == "" || len(request.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			fail(w, 401, "Owner service authentication required")
			return
		}
		path := strings.TrimPrefix(request.URL.Path, ownerTunnelPrefix)
		match := ownerTunnelPath.FindStringSubmatch(path)
		if !strings.HasPrefix(request.URL.Path, ownerTunnelPrefix+"/") || match == nil || request.URL.RawQuery != "" || request.URL.ForceQuery || request.URL.EscapedPath() != request.URL.Path {
			fail(w, 404, "Owner route not allowed")
			return
		}
		if _, err := strconv.ParseInt(match[2], 10, 64); err != nil {
			fail(w, 400, "Invalid owner epoch")
			return
		}
		models := match[3] == "probe/" && match[4] == "models"
		if (match[4] == "images/edits" && match[3] != "") || (match[4] == "models" && !models) || (match[3] == "evaluation/" && (match[4] == "responses" || match[4] == "messages" || match[4] == "models" || match[4] == "images/generations")) {
			fail(w, 404, "Owner route not allowed")
			return
		}
		if (models && request.Method != http.MethodGet) || (!models && request.Method != http.MethodPost) {
			fail(w, 405, "Owner method not allowed")
			return
		}
		if match[3] == "" {
			ctx, cancel := context.WithTimeout(request.Context(), maxExecutionTimeout+time.Minute)
			defer cancel()
			request = request.WithContext(context.WithValue(ctx, executionContextKey{}, true))
		}
		proxy.ServeHTTP(w, request)
	})
}

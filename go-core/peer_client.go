package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

type gatewaySessionContext struct{}
type selectedGatewayContext struct{}

func privateInference(control, gateway string) (http.Handler, error) {
	return privateInferenceWithSelector(control, gateway, nil)
}

// Core selects a gateway and preserves caller identity; Server owns execution decisions.
func privateInferenceWithSelector(control, gateway string, selector *gatewaySelector) (http.Handler, error) {
	if _, err := controlBase(control); err != nil {
		return nil, err
	}
	return inferenceWithSelector(gateway, selector)
}

func inferenceWithSelector(gateway string, selector *gatewaySelector) (http.Handler, error) {
	if _, err := controlBase(gateway); err != nil {
		return nil, err
	}
	target, _ := url.Parse(gateway)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	proxy.Transport = telemetryTransport{next: gatewayTransport{}, propagate: true, route: "desktop.gateway"}
	director := proxy.Director
	proxy.Director = func(request *http.Request) {
		director(request)
		if endpoint, ok := request.Context().Value(selectedGatewayContext{}).(gatewayEndpoint); ok {
			u, _ := url.Parse(gatewayBase(endpoint))
			request.URL.Scheme, request.URL.Host, request.Host = u.Scheme, u.Host, u.Host
			return
		}
		request.Host = target.Host
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		model := ""
		if request.Method == http.MethodPost && request.Body != nil {
			raw, err := readInferenceBody(request)
			if err != nil {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
				return
			}
			if isImagePath(request.URL.Path) && validateImageRequest(request.URL.Path, raw) != nil {
				http.Error(w, "invalid image generation request", http.StatusBadRequest)
				return
			}
			contentType, parameters, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
			if contentType == "multipart/form-data" && parameters["boundary"] != "" {
				reader := multipart.NewReader(bytes.NewReader(raw), parameters["boundary"])
				for {
					part, err := reader.NextPart()
					if err != nil {
						break
					}
					if part.FormName() == "model" {
						value, _ := io.ReadAll(io.LimitReader(part, 201))
						model = string(value)
						part.Close()
						break
					}
					part.Close()
				}
			} else {
				var body struct {
					Model string `json:"model"`
				}
				if json.Unmarshal(raw, &body) == nil {
					model = body.Model
				}
			}
		}
		if selector != nil {
			ctx := request.Context()
			key := request.Header.Get("X-Api-Key")
			if _, paired := request.Header["X-Api-Key"]; paired {
				ctx = context.WithValue(ctx, gatewaySessionContext{}, request.Header.Get("Authorization"))
			} else {
				key = strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
			}
			chosen, ok := selector.selectGatewayForModel(ctx, key, model)
			if !ok {
				historyRejectBeforeAuthorization(w, "Authorized Go Server unavailable", http.StatusServiceUnavailable)
				return
			}
			request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, chosen))
		}
		proxy.ServeHTTP(w, request)
	}), nil
}

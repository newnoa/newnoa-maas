package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptrace"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type proxySignal struct {
	Source   string `json:"source"`
	Kind     string `json:"kind"`
	Evidence string `json:"evidence"`
}
type proxyAssessment struct {
	Status          string        `json:"status"`
	Products        []string      `json:"products"`
	Signals         []proxySignal `json:"signals"`
	Limitations     []string      `json:"limitations"`
	DetectorVersion string        `json:"detectorVersion"`
	ObservedAt      string        `json:"observedAt"`
	RuntimeMode     string        `json:"runtimeMode"`
}
type supplyAssessmentResult struct {
	Endpoint   string          `json:"endpoint"`
	Assessment proxyAssessment `json:"assessment"`
}

var proxyProduct = regexp.MustCompile(`(?i)\b(openresty|nginx|envoy|haproxy|traefik|squid|caddy|apache)(/[0-9][a-z0-9._-]{0,30})?\b`)
var listenerName = regexp.MustCompile(`users:\(\("([a-zA-Z0-9._-]{1,80})"`)

func assessSupplyEndpoint(policy supplyPolicy, endpoint, key string) (supplyAssessmentResult, error) {
	return assessSupplyEndpointContext(context.Background(), policy, endpoint, key)
}
func assessSupplyEndpointContext(parent context.Context, policy supplyPolicy, endpoint, key string) (supplyAssessmentResult, error) {
	u, err := policy.endpoint(endpoint)
	if err != nil {
		return supplyAssessmentResult{}, err
	}
	report := proxyAssessment{Status: "unknown", Products: []string{}, Signals: []proxySignal{}, Limitations: []string{"Visible headers/process names can be hidden or spoofed; a local proxy may front a local model. This assessment does not prove compute ownership or authorization."}, DetectorVersion: "core-proxy/1", ObservedAt: time.Now().UTC().Format(time.RFC3339), RuntimeMode: "native"}
	if dockerRuntime() {
		report.RuntimeMode = "docker"
		report.Limitations = append(report.Limitations, "Docker cannot inspect the host process; host ownership is not established by this check.")
	}
	result := supplyAssessmentResult{Endpoint: strings.TrimRight(u.String(), "/"), Assessment: report}
	target, err := modelListURLWithPolicy(endpoint, policy)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	var peer string
	var mu sync.Mutex
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		mu.Lock()
		defer mu.Unlock()
		host, _, _ := net.SplitHostPort(info.Conn.RemoteAddr().String())
		peer = host
	}}
	request, _ := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), "GET", target.String(), nil)
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	client := policy.client(8 * time.Second)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	addProduct := func(value, source, kind string) {
		matches := proxyProduct.FindAllStringSubmatch(value, 8)
		for _, match := range matches {
			product := strings.ToLower(match[1])
			found := false
			for _, current := range report.Products {
				if current == product {
					found = true
				}
			}
			if !found {
				report.Products = append(report.Products, product)
			}
			report.Signals = append(report.Signals, proxySignal{source, kind, match[0]})
			report.Status = "suspected"
		}
	}
	if err != nil {
		report.Limitations = append(report.Limitations, "Endpoint probe failed; only available local listener evidence can be evaluated.")
	} else {
		response.Body.Close()
		report.Status = "no-obvious-signal"
		addProduct(response.Header.Get("Server"), "response-header", "server-product")
		if response.Header.Get("Via") != "" {
			report.Signals = append(report.Signals, proxySignal{"response-header", "via-present", "Via header present; arbitrary value omitted"})
			report.Status = "suspected"
			addProduct(response.Header.Get("Via"), "response-header", "via-product")
		}
		if response.StatusCode >= 300 && response.StatusCode < 400 {
			report.Signals = append(report.Signals, proxySignal{"response-header", "redirect", "HTTP " + strconv.Itoa(response.StatusCode) + "; redirect was not followed"})
			report.Limitations = append(report.Limitations, "The redirected service was not inspected.")
		}
	}
	mu.Lock()
	if net.ParseIP(peer) != nil {
		report.Signals = append(report.Signals, proxySignal{"socket", "connected-ip", peer})
	}
	mu.Unlock()
	if !dockerRuntime() && isLoopbackHost(u.Hostname()) {
		port := u.Port()
		if port == "" {
			if u.Scheme == "https" {
				port = "443"
			} else {
				port = "80"
			}
		}
		output, processErr := exec.CommandContext(ctx, "ss", "-H", "-ltnp", "sport", "=", ":"+port).Output()
		if processErr != nil || len(output) > 65536 || len(listenerName.FindAllStringSubmatch(string(output), 8)) == 0 {
			report.Limitations = append(report.Limitations, "Listening process metadata is unavailable; only network-visible features were checked.")
		} else {
			for _, match := range listenerName.FindAllStringSubmatch(string(output), 8) {
				addProduct(match[1], "listener-process", "executable-name")
			}
		}
	} else if !dockerRuntime() {
		report.Limitations = append(report.Limitations, "Remote endpoint process metadata is outside this Core's inspection scope.")
	}
	result.Assessment = report
	return result, nil
}

type cachedSupplyAssessment struct {
	checked     time.Time
	fingerprint string
}

// One background probe per Core at a time; no per-inference network probe or report.
func (n *connector) refreshAssessment(policy supplyPolicy, deployment string, revision int64, endpoint, key string) {
	if !nodePattern.MatchString(deployment) || revision < 1 {
		return
	}
	identity := deployment + "/" + strconv.FormatInt(revision, 10)
	n.assessmentMu.Lock()
	cached := n.assessments[identity]
	if n.assessing || time.Since(cached.checked) < 5*time.Minute {
		n.assessmentMu.Unlock()
		return
	}
	n.assessing = true
	if n.assessments == nil {
		n.assessments = map[string]cachedSupplyAssessment{}
	}
	if len(n.assessments) >= 512 {
		for id := range n.assessments {
			delete(n.assessments, id)
			break
		}
	}
	n.assessments[identity] = cachedSupplyAssessment{checked: time.Now(), fingerprint: cached.fingerprint}
	n.assessmentMu.Unlock()
	go func() {
		defer func() { n.assessmentMu.Lock(); n.assessing = false; n.assessmentMu.Unlock() }()
		result, err := assessSupplyEndpointContext(n.ctx, policy, endpoint, key)
		if err != nil {
			return
		}
		observed := result.Assessment.ObservedAt
		result.Assessment.ObservedAt = ""
		encoded, _ := json.Marshal(result.Assessment)
		fingerprint := hash(encoded)
		result.Assessment.ObservedAt = observed
		if fingerprint == cached.fingerprint {
			return
		}
		route := "/connector/" + n.nodeID + "/deployments/" + deployment + "/revisions/" + strconv.FormatInt(revision, 10) + "/assessment"
		if n.call(n.ctx, route, map[string]any{"endpoint": endpoint, "assessment": result.Assessment}, nil) == nil {
			n.assessmentMu.Lock()
			current := n.assessments[identity]
			current.fingerprint = fingerprint
			n.assessments[identity] = current
			n.assessmentMu.Unlock()
		}
	}()
}

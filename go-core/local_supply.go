package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type localSupplyDevice struct {
	Control    string `json:"control"`
	AccountID  string `json:"accountId"`
	NodeID     string `json:"nodeId"`
	Credential string `json:"credential"`
	OwnerID    string `json:"ownerId"`
}

func (c *core) localSupplyPath(control, account string) string {
	return filepath.Join(c.home, ".newnoa", "supply-"+hash([]byte(control+"\n"+account))+".json")
}

func (c *core) readLocalSupply(file string) (localSupplyDevice, error) {
	var device localSupplyDevice
	if err := c.safeConfigFile(file); err != nil {
		return device, errors.New("Local supply credentials are unavailable")
	}
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		return device, nil
	}
	if err != nil || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return device, errors.New("Local supply credentials are unavailable")
	}
	body, err := os.ReadFile(file)
	if err != nil || json.Unmarshal(body, &device) != nil {
		return device, errors.New("Local supply credentials are unavailable")
	}
	return device, nil
}

func (c *core) localSupplyIdentity(ctx context.Context, session string) (localSupplyDevice, error) {
	var device localSupplyDevice
	control, err := controlBase(c.control)
	if err != nil {
		return device, err
	}
	if session == "" || len(session) > 512 || strings.ContainsAny(session, "\r\n") {
		return device, errors.New("Authentication required")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	remote := func(route string, body any, out any) error {
		method := http.MethodGet
		var raw []byte
		if body != nil {
			method = http.MethodPost
			raw, _ = json.Marshal(body)
		}
		request, err := http.NewRequestWithContext(ctx, method, control+"/api/v1"+route, bytes.NewReader(raw))
		if err != nil {
			return errors.New("Local supply control is unavailable")
		}
		request.Header.Set("Authorization", "Bearer "+session)
		request.Header.Set("Content-Type", "application/json")
		response, err := connectorControlClient().Do(request)
		if err != nil {
			return errors.New("Local supply control is unavailable")
		}
		defer response.Body.Close()
		if response.StatusCode == 401 || response.StatusCode == 403 {
			return errors.New("Authentication required")
		}
		if response.StatusCode != http.StatusOK {
			return errors.New("Local supply control is unavailable")
		}
		raw, err = io.ReadAll(io.LimitReader(response.Body, maxFrame+1))
		if err != nil || len(raw) > maxFrame || json.Unmarshal(raw, out) != nil {
			return errors.New("Invalid local supply response")
		}
		return nil
	}
	var account struct {
		ID string `json:"id"`
	}
	if err = remote("/me", nil, &account); err != nil {
		return device, err
	}
	if !nodePattern.MatchString(account.ID) {
		return device, errors.New("Invalid local supply response")
	}
	file := c.localSupplyPath(control, account.ID)
	device, err = c.readLocalSupply(file)
	if err != nil {
		return device, err
	}
	var nodes []struct {
		ID        string           `json:"id"`
		Status    string           `json:"status"`
		RevokedAt *json.RawMessage `json:"revoked_at"`
	}
	if err = remote("/me/nodes", nil, &nodes); err != nil {
		return device, err
	}
	owned := func(id string) bool {
		for _, node := range nodes {
			if node.ID == id && node.Status != "revoked" && node.RevokedAt == nil {
				return true
			}
		}
		return false
	}
	activeOwner := ""
	c.mu.Lock()
	if n := c.connector; n != nil && n.status()["state"] != "stopped" {
		if !owned(n.nodeID) || n.purpose != "serving" {
			c.mu.Unlock()
			return device, errors.New("Local supply connector is already in use")
		}
		n.mu.Lock()
		device = localSupplyDevice{Control: control, AccountID: account.ID, NodeID: n.nodeID, Credential: n.credential, OwnerID: n.ownerID}
		activeOwner = n.ownerID
		n.mu.Unlock()
	}
	c.mu.Unlock()
	var owners []gatewayEndpoint
	if err = remote("/me/frp-owners", nil, &owners); err != nil {
		return device, err
	}
	if len(owners) == 0 || len(owners) > gatewayMaxNodes {
		return device, errors.New("No local supply coordinator is available")
	}
	for _, candidate := range owners {
		if !validGatewayEndpoint(candidate) {
			return device, errors.New("Invalid local supply response")
		}
	}
	if activeOwner != "" {
		bound := owners[:0]
		for _, candidate := range owners {
			if candidate.ID == activeOwner {
				bound = append(bound, candidate)
			}
		}
		owners = bound
	}
	selector := newGatewaySelector(control)
	selector.discover = func(context.Context, string, string) ([]gatewayEndpoint, error) { return owners, nil }
	if c.gatewaySelector != nil {
		selector.probe = c.gatewaySelector.probe
	}
	selected, ok := selector.selectGateway(ctx, session)
	if !ok {
		return device, errors.New("No local supply coordinator is available")
	}
	owner := selected.ID
	if ok, _ := regexp.MatchString(`^[a-zA-Z0-9_-]{1,80}$`, owner); !ok {
		return device, errors.New("Invalid local supply response")
	}
	created := false
	if device.NodeID == "" || !owned(device.NodeID) {
		var enrolled struct {
			ID         string `json:"id"`
			Credential string `json:"credential"`
		}
		if err = remote("/me/nodes", map[string]string{"name": "NEWNOA local model service"}, &enrolled); err != nil {
			return device, err
		}
		device = localSupplyDevice{Control: control, AccountID: account.ID, NodeID: enrolled.ID, Credential: enrolled.Credential}
		created = true
	}
	if device.Control != control || device.AccountID != account.ID || !nodePattern.MatchString(device.NodeID) || !strings.HasPrefix(device.Credential, "node_") || len(device.Credential) < 35 || len(device.Credential) > 100 {
		return device, errors.New("Invalid local supply response")
	}
	device.OwnerID = owner
	raw, _ := json.Marshal(device)
	if err = c.safeConfigFile(file); err == nil {
		err = atomicFile(file, raw)
	}
	if err != nil {
		if created {
			var ignored any
			_ = remote("/me/nodes/"+device.NodeID+"/revoke", map[string]any{"version": 1, "reason": "Local device credentials could not be saved"}, &ignored)
		}
		return device, errors.New("Local supply credentials could not be saved")
	}
	return device, nil
}

func (c *core) ensureLocalSupply(session string) (map[string]any, error) {
	c.supplyMu.Lock()
	defer c.supplyMu.Unlock()
	if _, err := verifiedFRPC(); err != nil {
		return nil, err
	}
	device, err := c.localSupplyIdentity(context.Background(), session)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connector != nil && c.connector.status()["state"] != "stopped" {
		if c.connector.nodeID != device.NodeID {
			return nil, errors.New("Local supply connector is already in use")
		}
	} else if err = c.startConnector(device.NodeID, device.Credential, device.OwnerID, "serving"); err != nil {
		return nil, err
	}
	return map[string]any{"nodeId": device.NodeID, "state": c.connector.status()["state"]}, nil
}

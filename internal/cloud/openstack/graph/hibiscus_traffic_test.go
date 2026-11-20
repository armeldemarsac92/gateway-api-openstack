/*
Copyright 2026 The gateway-api-openstack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	openstackidentity "github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/identity"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/octavia"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
)

// The Octavia API fixture stores the real Gophercloud writes. The stock render
// and HAProxy operate on that inventory, including the administrative close.
func TestHibiscusPrivateHTTPTraffic(t *testing.T) {
	python, haproxy := os.Getenv("HIBISCUS_OCTAVIA_PYTHON"), os.Getenv("HIBISCUS_HAPROXY")
	if python == "" && haproxy == "" {
		t.Skip("set HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY to run the loopback pilot")
	}
	if !filepath.IsAbs(python) || !filepath.IsAbs(haproxy) {
		t.Fatal("HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY must be absolute paths")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	version, err := exec.CommandContext(ctx, haproxy, "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("read HAProxy version: %v\n%s", err, version)
	}
	t.Log(strings.SplitN(string(version), "\n", 2)[0])

	var healthChecks atomic.Int32
	const response = "hibiscus-node-backend"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/healthz":
			healthChecks.Add(1)
			w.WriteHeader(http.StatusOK)
		case "/api":
			_, _ = io.WriteString(w, response+" "+req.Host)
		default:
			http.NotFound(w, req)
		}
	}))
	defer backend.Close()
	backendAddress := backend.Listener.Addr().(*net.TCPAddr)
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reservation.Close() }()
	frontendAddress := reservation.Addr().(*net.TCPAddr)

	const (
		lb       = "/v2/lbaas/loadbalancers"
		gateway  = lb + "/gateway"
		listener = "/v2/lbaas/listeners"
		pool     = "/v2/lbaas/pools"
		member   = pool + "/pool/members"
		monitor  = "/v2/lbaas/healthmonitors"
		policy   = "/v2/lbaas/l7policies"
		rule     = policy + "/policy/rules"
	)
	type record = map[string]any
	protected := hibiscusControlLoadBalancers(lb)
	inventory := maps.Clone(protected)
	// Public response relationships differ from the corresponding POST
	// fields. Keep that translation in the fixture alongside allocated IDs.
	collections := map[string]struct {
		key, id, parentKey, parentID, parentList string
	}{
		lb:       {key: "loadbalancer", id: "gateway"},
		listener: {key: "listener", id: "listener", parentKey: "loadbalancer_id", parentID: "gateway", parentList: "loadbalancers"},
		pool:     {key: "pool", id: "pool", parentKey: "loadbalancer_id", parentID: "gateway", parentList: "loadbalancers"},
		monitor:  {key: "healthmonitor", id: "healthmonitor", parentKey: "pool_id", parentID: "pool", parentList: "pools"},
		member:   {key: "member", id: "member"},
		policy:   {key: "l7policy", id: "policy", parentKey: "listener_id", parentID: "listener"},
		rule:     {key: "rule"},
	}
	var writes []string
	cloudAPI := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resource := req.URL.Path
		if resource == "/v2.0/floatingips" && req.Method == http.MethodGet {
			safetyWriteJSON(t, w, record{"floatingips": []any{}})
			return
		}
		collection, id := resource, ""
		if _, exists := collections[resource]; !exists {
			split := strings.LastIndex(resource, "/")
			collection, id = resource[:split], resource[split+1:]
		}
		kind, known := collections[collection]
		entry := inventory[resource]
		reject := func() {
			t.Errorf("unexpected Octavia request %s %s", req.Method, req.URL.RequestURI())
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
		if !known {
			reject()
			return
		}
		if req.Method == http.MethodGet {
			if id != "" && entry != nil {
				safetyWriteJSON(t, w, record{kind.key: entry})
				return
			}
			if id != "" || !safetyRequireQuery(t, w, req, "project_id", hibiscusProject) ||
				(kind.parentKey != "" && !safetyRequireQuery(t, w, req, kind.parentKey, kind.parentID)) {
				reject()
				return
			}
			items := []record{}
			for _, item := range slices.Sorted(maps.Keys(inventory)) {
				if strings.HasPrefix(item, collection+"/") && !strings.Contains(strings.TrimPrefix(item, collection+"/"), "/") {
					data := inventory[item]
					if tags := req.URL.Query().Get("tags"); tags != "" &&
						!openstackidentity.HasAllTags(pilotTags(data["tags"]), strings.Split(tags, ",")) {
						continue
					}
					items = append(items, data)
				}
			}
			safetyWriteJSON(t, w, record{collection[strings.LastIndex(collection, "/")+1:]: items})
			return
		}
		if protected[resource] != nil {
			reject()
			return
		}
		writes = append(writes, req.Method+" "+resource)
		switch req.Method {
		case http.MethodPost, http.MethodPut:
			var payload map[string]record
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil || payload[kind.key] == nil {
				reject()
				return
			}
			data := payload[kind.key]
			if req.Method == http.MethodPost && id == "" {
				id = kind.id
				if collection == lb {
					data["vip_address"], data["vip_port_id"] = frontendAddress.IP.String(), "vip-port"
				} else if collection == rule {
					ruleType, ok := data["type"].(string)
					if !ok {
						reject()
						return
					}
					id = strings.ToLower(ruleType)
				} else if kind.parentList != "" {
					data[kind.parentList] = []record{{"id": data[kind.parentKey]}}
				}
				if _, specified := data["default_pool_id"]; collection == listener && !specified {
					data["default_pool_id"] = nil
				}
				resource = collection + "/" + id
				if inventory[resource] != nil {
					reject()
					return
				}
				data["id"], data["project_id"], data["provisioning_status"] = id, hibiscusProject, "ACTIVE"
				inventory[resource] = data
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
			} else if req.Method == http.MethodPut && entry != nil && (resource == gateway || collection == policy) {
				maps.Copy(entry, data)
				if resource == gateway {
					entry["provisioning_status"] = "PENDING_UPDATE"
				}
				data = entry
			} else {
				reject()
				return
			}
			safetyWriteJSON(t, w, record{kind.key: data})
		case http.MethodDelete:
			if entry == nil || inventory[gateway]["admin_state_up"] != false || inventory[gateway]["provisioning_status"] != "ACTIVE" {
				reject()
				return
			}
			delete(inventory, resource)
			w.WriteHeader(http.StatusNoContent)
		default:
			reject()
		}
	})
	newProvider := func() *Provider {
		return newTestProvider(t, hibiscusServiceClients(cloudAPI), ProviderConfig{})
	}

	provider := newProvider()
	listProject := func(p *Provider) []octavia.LoadBalancer {
		t.Helper()
		items, err := p.octavia.ListLoadBalancers(ctx, loadbalancers.ListOpts{ProjectID: hibiscusProject})
		if err != nil {
			t.Fatal(err)
		}
		return items
	}
	controls := listProject(provider)
	if len(controls) != 2 || controls[0].ID != "magnum-api-server" || controls[1].ID != "occm-service" {
		t.Fatalf("protected project inventory = %v", controls)
	}
	spec := completeGraphSpec(safetyTestIdentity(t))
	spec.Gateway.Spec.ListenerPort = frontendAddress.Port
	route := &spec.Routes[0].Spec
	route.Gateway = cloud.GatewayState{}
	route.GatewayRequirements.ListenerPort = frontendAddress.Port
	route.Members = []cloud.Member{{Address: backendAddress.IP.String(), Port: backendAddress.Port}}
	route.HealthMonitor.Delay, route.HealthMonitor.Timeout = 2, 1

	reconcile := func(p *Provider, spec cloud.GatewayGraphSpec) cloud.GatewayGraphResult {
		t.Helper()
		result, err := p.ReconcileGatewayGraph(ctx, spec)
		if err != nil {
			t.Fatalf("reconcile after writes %v: %v", writes, err)
		}
		return result
	}
	converge := func(p *Provider, desired cloud.GatewayGraphSpec) cloud.GatewayGraphResult {
		t.Helper()
		var result cloud.GatewayGraphResult
		for range 12 {
			result = reconcile(p, desired)
			if result.Outcome.State == cloud.OutcomeReady {
				return result
			}
		}
		t.Fatalf("graph did not converge: %v, writes %v", result.Outcome, writes)
		return result
	}
	if result := converge(provider, spec); len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("route did not converge: %#v, writes %v", result, writes)
	}
	driftedInventory := maps.Clone(inventory)
	driftedListener := maps.Clone(inventory[listener+"/listener"])
	driftedListener["default_pool_id"] = inventory[pool+"/pool"]["id"]
	driftedInventory[listener+"/listener"] = driftedListener
	driftedJSON, err := json.Marshal(driftedInventory)
	if err != nil {
		t.Fatal(err)
	}
	render := exec.CommandContext(ctx, python, "-I", "testdata/render_hibiscus_amphora.py", t.TempDir())
	render.Stdin = bytes.NewReader(driftedJSON)
	if output, err := render.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("the pilot requires a listener without a default pool")) {
		t.Fatalf("render listener with a default pool: %v\n%s", err, output)
	}
	t.Logf("Gateway and route converged after %v", writes)
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	startProxy := func() func() {
		t.Helper()
		data, err := json.Marshal(inventory)
		if err != nil {
			t.Fatal(err)
		}
		return startPilotHAProxy(t, ctx, python, haproxy, data)
	}
	stopOpenProxy := startProxy()
	address := frontendAddress.String()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
	send := func(host, requestPath string) (int, string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+requestPath, nil)
		if err != nil {
			return 0, "", err
		}
		req.Host = host
		res, err := client.Do(req)
		if err != nil {
			return 0, "", err
		}
		body, err := io.ReadAll(res.Body)
		return res.StatusCode, string(body), errors.Join(err, res.Body.Close())
	}
	const host = "API.Example.COM:1234"
	deadline := time.Now().Add(8 * time.Second)
	for {
		status, body, err := send(host, "/api")
		if status == http.StatusOK && body == response+" "+host && healthChecks.Load() > 0 && err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loopback %s /api = %d %q, health checks = %d, err = %v", host, status, body, healthChecks.Load(), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("HTTP %s /api -> 200 %q from loopback backend %s, active health check observed", host, response+" "+host, backendAddress)
	if status, body, err := send(route.Hostname, "/api"); status != http.StatusOK || body != response+" "+route.Hostname || err != nil {
		t.Fatalf("bare Host %s /api = %d %q, err = %v", route.Hostname, status, body, err)
	}
	for _, miss := range [][2]string{{"apiXexample.com", "/api"}, {host, "/api/extra"}} {
		if status, body, err := send(miss[0], miss[1]); status != http.StatusServiceUnavailable || strings.Contains(body, response) || err != nil {
			t.Fatalf("unmatched %q = %d %q, err = %v", miss, status, body, err)
		}
		t.Logf("HTTP %s %s -> 503", miss[0], miss[1])
	}

	initialWrites := len(writes)
	provider = newProvider()
	if result := reconcile(provider, spec); result.Outcome.State != cloud.OutcomeReady || len(writes) != initialWrites {
		t.Fatalf("fresh provider did not rediscover the serving graph: %v, writes %v", result.Outcome, writes)
	}
	if status, body, err := send(host, "/api"); status != http.StatusOK || body != response+" "+host || err != nil {
		t.Fatalf("after recreating provider instance: HTTP %d %q, err = %v", status, body, err)
	}
	t.Log("fresh provider instance rediscovered the serving graph without writes")
	remove := cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{Disposition: cloud.GraphDelete}}
	if result := reconcile(provider, remove); result.Outcome.State != cloud.OutcomeProgressing ||
		!slices.Equal(writes[initialWrites:], []string{"PUT " + gateway}) || inventory[gateway]["admin_state_up"] != false {
		t.Fatalf("cleanup did not close the Gateway first: %v, writes %v", result.Outcome, writes)
	}
	provider = newProvider()
	if result := reconcile(provider, remove); result.Outcome.State != cloud.OutcomeProgressing || len(writes) != initialWrites+1 {
		t.Fatalf("fresh provider mutated a graph whose close is pending: %v, writes %v", result.Outcome, writes)
	}
	stopOpenProxy()
	stopClosedProxy := startProxy()
	// The disabled stock configuration can start HAProxy's stats socket,
	// but has no listener on the earlier serving port.
	if _, _, err := send(host, "/api"); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("disabled configuration still accepts traffic: %v", err)
	}
	inventory[gateway]["provisioning_status"] = "ACTIVE"
	converge(provider, remove)
	stopClosedProxy()
	if _, _, err := send(host, "/api"); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("removed Gateway still accepts traffic: %v", err)
	}
	if after := listProject(provider); len(inventory) != len(controls) || !reflect.DeepEqual(after, controls) {
		t.Fatalf("final inventory = %v, want unchanged protected controls %v", after, controls)
	}
	t.Logf("Gateway traffic refused before and after owned cleanup %v; Magnum API server and OCCM load balancers unchanged", writes[initialWrites:])
}

func pilotTags(value any) []string {
	if tags, ok := value.([]string); ok {
		return tags
	}
	var tags []string
	if values, ok := value.([]any); ok {
		for _, item := range values {
			tags = append(tags, item.(string))
		}
	}
	return tags
}

func startPilotHAProxy(t *testing.T, ctx context.Context, python, haproxy string, inventory []byte) func() {
	t.Helper()
	// Keep the stats socket under the platform's Unix socket path limit.
	dir, err := os.MkdirTemp("", "hibiscus-haproxy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	render := exec.CommandContext(ctx, python, "-I", "testdata/render_hibiscus_amphora.py", dir)
	render.Stdin = bytes.NewReader(inventory)
	if output, err := render.CombinedOutput(); err != nil {
		t.Fatalf("render pinned Octavia templates: %v\n%s", err, output)
	}
	config := filepath.Join(dir, "haproxy.cfg")
	if output, err := exec.CommandContext(ctx, haproxy, "-c", "-f", config).CombinedOutput(); err != nil {
		t.Fatalf("check stock HAProxy configuration: %v\n%s", err, output)
	}
	logName := filepath.Join(dir, "haproxy.log")
	logFile, err := os.Create(logName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logFile.Close() }()
	// Map only the invoking UID/GID to the account named in stock base.j2.
	// This lets an unprivileged process serve the exact rendered config.
	nobody, err := user.Lookup("nobody")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "unshare", "--user", "--map-user="+nobody.Uid, "--map-group="+nobody.Gid, "--", haproxy, "-db", "-f", config)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stop := func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			if output, err := os.ReadFile(logName); err == nil {
				t.Logf("HAProxy log:\n%s", output)
			}
		}
	})
	for deadline := time.Now().Add(4 * time.Second); ; {
		if pilotHAProxyReady(filepath.Join(dir, "stats.sock")) {
			return stop
		}
		if time.Now().After(deadline) {
			t.Fatal("HAProxy stats socket did not become reachable")
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func pilotHAProxyReady(socket string) bool {
	conn, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return false
	}
	if _, err := io.WriteString(conn, "show info\n"); err != nil {
		return false
	}
	status, err := io.ReadAll(conn)
	return err == nil && bytes.HasPrefix(status, []byte("Name: HAProxy\n"))
}

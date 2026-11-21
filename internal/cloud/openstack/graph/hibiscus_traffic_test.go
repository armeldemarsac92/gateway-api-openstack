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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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
	testHibiscusHTTPTraffic(t, false, nil)
}

func TestHibiscusSharedHTTPTraffic(t *testing.T) {
	testHibiscusHTTPTraffic(t, true, nil)
}

// Point the pilot at a private kind worker NodePort with one Ready Gateway API
// echo-basic pod. The operator supplies the pod and namespace independently.
func TestHibiscusKindWorkerHTTP(t *testing.T) {
	address := os.Getenv("HIBISCUS_KIND_NODEPORT")
	pod, namespace := os.Getenv("HIBISCUS_KIND_POD"), os.Getenv("HIBISCUS_KIND_NAMESPACE")
	if address == "" && pod == "" && namespace == "" {
		t.Skip("set HIBISCUS_KIND_NODEPORT, HIBISCUS_KIND_POD and HIBISCUS_KIND_NAMESPACE to run the worker pilot")
	}
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil || !endpoint.Addr().Is4() || !endpoint.Addr().IsPrivate() || endpoint.Port() == 0 {
		t.Fatal("HIBISCUS_KIND_NODEPORT must be a private IPv4 address with an explicit nonzero port")
	}
	if pod == "" || namespace == "" {
		t.Fatal("HIBISCUS_KIND_POD and HIBISCUS_KIND_NAMESPACE must identify the Ready echo-basic pod")
	}
	testHibiscusHTTPTraffic(t, false, &kindHTTPBackend{endpoint: endpoint, pod: pod, namespace: namespace})
}

type kindHTTPBackend struct {
	endpoint       netip.AddrPort
	pod, namespace string
}

type kindHTTPEcho struct {
	Pod       string `json:"pod"`
	Namespace string `json:"namespace"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Host      string `json:"host"`
}

func TestHibiscusTCPTraffic(t *testing.T) {
	python, haproxy := os.Getenv("HIBISCUS_OCTAVIA_PYTHON"), os.Getenv("HIBISCUS_HAPROXY")
	if python == "" && haproxy == "" {
		t.Skip("set HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY to run the loopback pilot")
	}
	if !filepath.IsAbs(python) || !filepath.IsAbs(haproxy) {
		t.Fatal("HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY must be absolute paths")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer func() { _ = reservation.Close() }()
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	var echoCount atomic.Int32
	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		for {
			conn, err := backend.Accept()
			if err != nil {
				return
			}
			func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, got); err == nil && bytes.Equal(got, payload) {
					echoCount.Add(1)
					_, _ = conn.Write(got)
				}
			}()
		}
	}()
	defer func() {
		_ = backend.Close()
		<-echoDone
	}()

	f := newTCPGraphFixture(t)
	frontend := reservation.Addr().(*net.TCPAddr)
	f.spec.Gateway.Spec.ListenerPort = frontend.Port
	route := &f.spec.Routes[0]
	route.Spec.GatewayRequirements.ListenerPort = frontend.Port
	address := backend.Addr().(*net.TCPAddr)
	route.Spec.Members = []cloud.Member{{Address: address.IP.String(), Port: address.Port}}
	route.Spec.HealthMonitor.Delay, route.Spec.HealthMonitor.Timeout = 2, 1
	provider := f.provider()
	for range 6 {
		f.reconcile(provider)
	}
	if f.inventory[tcpListener]["default_pool_id"] != "pool" {
		t.Fatalf("listener does not target the converged pool: %v", f.writes)
	}
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeReady || len(f.writes) != 6 {
		t.Fatalf("provider restart = %#v, writes %v", result, f.writes)
	}
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	startProxy := func() func() {
		t.Helper()
		data, err := json.Marshal(f.inventory)
		if err != nil {
			t.Fatal(err)
		}
		return startPilotHAProxy(t, ctx, python, haproxy, data)
	}
	exchange := func() ([]byte, error) {
		conn, err := net.DialTimeout("tcp", frontend.String(), time.Second)
		if err != nil {
			return nil, err
		}
		defer func() { _ = conn.Close() }()
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return nil, err
		}
		if _, err := conn.Write(payload); err != nil {
			return nil, err
		}
		response := make([]byte, len(payload))
		n, err := io.ReadFull(conn, response)
		return response[:n], err
	}
	stopServing := startProxy()
	if data, err := exchange(); err != nil || !bytes.Equal(data, payload) || echoCount.Load() != 1 {
		t.Fatalf("attached TCP round trip = %x, %v, backend received %d requests", data, err, echoCount.Load())
	}
	route.Disposition = cloud.GraphPreserve
	f.reconcile(provider)
	f.inventory[tcpListener]["provisioning_status"] = "ACTIVE"
	if result := f.reconcile(f.provider()); result.Outcome.State != cloud.OutcomeReady || f.inventory[tcpPool] == nil || len(f.writes) != 7 {
		t.Fatalf("revocation after restart = %#v, writes %v", result, f.writes)
	}
	stopServing()
	stopDetached := startProxy()
	defer stopDetached()
	if data, err := exchange(); len(data) != 0 || (err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE)) || echoCount.Load() != 1 {
		t.Fatalf("detached TCP round trip = %x, %v, backend received %d requests", data, err, echoCount.Load())
	}
	t.Logf("Stock HAProxy echoed all %d bytes after attach and forwarded no bytes after detach", len(payload))
}

func testHibiscusHTTPTraffic(t *testing.T, shared bool, worker *kindHTTPBackend) {
	python, haproxy := os.Getenv("HIBISCUS_OCTAVIA_PYTHON"), os.Getenv("HIBISCUS_HAPROXY")
	if worker == nil && python == "" && haproxy == "" {
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

	const response = "hibiscus-node-backend"
	var backendAddress netip.AddrPort
	var healthChecks *atomic.Int32
	var renderArgs []string
	if worker == nil {
		address, checks := startPilotBackend(t, response)
		backendAddress, healthChecks = address.AddrPort(), checks
	} else {
		backendAddress = worker.endpoint
		renderArgs = []string{backendAddress.String()}
		t.Logf("Worker NodePort %s, expected pod %s/%s", backendAddress, worker.namespace, worker.pod)
	}
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
		policy   = "/v2/lbaas/l7policies"
	)
	type record = map[string]any
	protected := hibiscusControlLoadBalancers(lb)
	inventory := maps.Clone(protected)
	// Public response relationships differ from the corresponding POST
	// fields. Keep that translation in the fixture alongside allocated IDs.
	collections := map[string]struct {
		key, id, parentKey, parentCollection, parentList string
	}{
		"loadbalancers":  {key: "loadbalancer", id: "gateway"},
		"listeners":      {key: "listener", id: "listener", parentKey: "loadbalancer_id", parentCollection: lb, parentList: "loadbalancers"},
		"pools":          {key: "pool", id: "pool", parentKey: "loadbalancer_id", parentCollection: lb, parentList: "loadbalancers"},
		"healthmonitors": {key: "healthmonitor", id: "healthmonitor", parentKey: "pool_id", parentCollection: pool, parentList: "pools"},
		"members":        {key: "member", id: "member", parentCollection: pool},
		"l7policies":     {key: "l7policy", id: "policy", parentKey: "listener_id", parentCollection: listener},
		"rules":          {key: "rule", id: "rule", parentCollection: policy},
	}
	list := func(collection string) []record {
		items := []record{}
		for _, resource := range slices.Sorted(maps.Keys(inventory)) {
			if path.Dir(resource) == collection {
				items = append(items, inventory[resource])
			}
		}
		return items
	}
	orderedPolicies := func(listenerID any) []record {
		items := slices.DeleteFunc(list(policy), func(item record) bool { return item["listener_id"] != listenerID })
		slices.SortFunc(items, func(a, b record) int { return cmp.Compare(a["position"].(int), b["position"].(int)) })
		return items
	}
	// Octavia's per-listener ordering list uses one-based positions. Inserting or
	// moving a policy shifts its siblings, and deletion closes the gap.
	placePolicy := func(data record, requested any) {
		items := slices.DeleteFunc(orderedPolicies(data["listener_id"]), func(item record) bool { return item["id"] == data["id"] })
		index := len(items)
		if position, ok := requested.(float64); ok {
			index = min(int(position)-1, index)
		}
		for i, item := range slices.Insert(items, index, data) {
			item["position"] = i + 1
		}
	}
	nextID := map[string]int{}
	var writes []string
	cloudAPI := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resource := req.URL.Path
		if resource == "/v2.0/floatingips" && req.Method == http.MethodGet {
			safetyWriteJSON(t, w, record{"floatingips": []any{}})
			return
		}
		collection, id := resource, ""
		if _, exists := collections[path.Base(resource)]; !exists {
			collection, id = path.Dir(resource), path.Base(resource)
		}
		name := path.Base(collection)
		kind, known := collections[name]
		entry := inventory[resource]
		reject := func() {
			t.Errorf("unexpected Octavia request %s %s", req.Method, req.URL.RequestURI())
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
		outer := path.Dir(collection)
		if !known || (kind.parentCollection != "" && kind.parentKey == "" &&
			(path.Dir(outer) != kind.parentCollection || inventory[outer] == nil)) ||
			((kind.parentCollection == "" || kind.parentKey != "") && outer != "/v2/lbaas") {
			reject()
			return
		}
		if req.Method == http.MethodGet {
			if id != "" && entry != nil {
				safetyWriteJSON(t, w, record{kind.key: entry})
				return
			}
			parentID := req.URL.Query().Get(kind.parentKey)
			if id != "" || !safetyRequireQuery(t, w, req, "project_id", hibiscusProject) ||
				(kind.parentKey != "" && inventory[kind.parentCollection+"/"+parentID] == nil) {
				reject()
				return
			}
			items := list(collection)
			if collection == policy {
				items = orderedPolicies(parentID)
			}
			items = slices.DeleteFunc(items, func(data record) bool {
				if kind.parentKey != "" && data[kind.parentKey] != parentID {
					return true
				}
				tags := req.URL.Query().Get("tags")
				return tags != "" && !openstackidentity.HasAllTags(pilotTags(data["tags"]), strings.Split(tags, ","))
			})
			safetyWriteJSON(t, w, record{name: items})
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
			if position, exists := data["position"]; collection == policy && exists {
				if value, ok := position.(float64); !ok || value < 1 || value != float64(int(value)) {
					reject()
					return
				}
			}
			if req.Method == http.MethodPost && id == "" {
				id = kind.id
				// Octavia allocates these IDs globally. Its HAProxy template uses
				// rule IDs as ACL names on the shared frontend.
				nextID[name]++
				if nextID[name] > 1 {
					id += "-" + strconv.Itoa(nextID[name])
				}
				parentID, _ := data[kind.parentKey].(string)
				if kind.parentKey != "" && inventory[kind.parentCollection+"/"+parentID] == nil {
					reject()
					return
				}
				if collection == lb {
					data["vip_address"], data["vip_port_id"] = frontendAddress.IP.String(), "vip-port"
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
				if collection == policy {
					placePolicy(data, data["position"])
				}
				inventory[resource] = data
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
			} else if req.Method == http.MethodPut && entry != nil && (resource == gateway || collection == policy) {
				if position, exists := data["position"]; collection == policy && exists {
					placePolicy(entry, position)
					delete(data, "position")
				}
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
			if entry == nil || inventory[gateway]["provisioning_status"] != "ACTIVE" ||
				((collection == lb || collection == listener) && inventory[gateway]["admin_state_up"] != false) {
				reject()
				return
			}
			delete(inventory, resource)
			if collection == policy {
				for i, item := range orderedPolicies(entry["listener_id"]) {
					item["position"] = i + 1
				}
			}
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
	route.Members = []cloud.Member{{Address: backendAddress.Addr().String(), Port: int(backendAddress.Port())}}
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
		for range 24 {
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
		return startPilotHAProxy(t, ctx, python, haproxy, data, renderArgs...)
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
	expectBackend := func(requestPath, backend string, checks *atomic.Int32) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Second)
		for {
			status, body, err := send(host, requestPath)
			if status == http.StatusOK && body == backend+" "+host && checks.Load() > 0 && err == nil {
				t.Logf("HTTP %s %s -> 200 %q, active health check observed", host, requestPath, body)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("loopback %s %s = %d %q, want %q, health checks = %d, err = %v", host, requestPath, status, body, backend, checks.Load(), err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	expectWorkerBackend := func(stage string) {
		t.Helper()
		for _, requestHost := range []string{host, route.Hostname} {
			want := kindHTTPEcho{Pod: worker.pod, Namespace: worker.namespace, Method: http.MethodGet, Path: "/api", Host: requestHost}
			for deadline := time.Now().Add(6 * time.Second); ; {
				status, body, err := send(requestHost, "/api")
				if status == http.StatusOK && err == nil {
					var got kindHTTPEcho
					if err := json.Unmarshal([]byte(body), &got); err != nil || got != want {
						t.Fatalf("%s: worker echo = %+v, want %+v, decode error = %v", stage, got, want, err)
					}
					t.Logf("%s: HAProxy VIP %s, Host %s -> 200 from kind pod %s/%s, %s %s",
						stage, address, got.Host, got.Namespace, got.Pod, got.Method, got.Path)
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s: HTTP %s %s = %d %q, err = %v", stage, requestHost, want.Path, status, body, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	if worker != nil {
		expectWorkerBackend("before adapter recreation")
	} else {
		expectBackend("/api", response, healthChecks)
		if status, body, err := send(route.Hostname, "/api"); status != http.StatusOK || body != response+" "+route.Hostname || err != nil {
			t.Fatalf("bare Host %s /api = %d %q, err = %v", route.Hostname, status, body, err)
		}
	}
	expectMiss := func(host, requestPath string) {
		t.Helper()
		if status, body, err := send(host, requestPath); status != http.StatusServiceUnavailable || strings.Contains(body, "hibiscus-") || err != nil {
			t.Fatalf("unmatched %s %s = %d %q, err = %v", host, requestPath, status, body, err)
		}
		t.Logf("HTTP %s %s -> 503", host, requestPath)
	}
	expectMiss("apiXexample.com", "/api")
	expectMiss(host, "/api/extra")

	initialWrites := len(writes)
	provider = newProvider()
	if result := reconcile(provider, spec); result.Outcome.State != cloud.OutcomeReady || len(writes) != initialWrites {
		t.Fatalf("fresh provider did not rediscover the serving graph: %v, writes %v", result.Outcome, writes)
	}
	if worker != nil {
		expectWorkerBackend("after adapter recreation")
	} else if status, body, err := send(host, "/api"); status != http.StatusOK || body != response+" "+host || err != nil {
		t.Fatalf("after recreating provider instance: HTTP %d %q, err = %v", status, body, err)
	}
	t.Log("fresh provider instance rediscovered the serving graph without writes")
	if shared {
		const broadResponse = "hibiscus-broad-backend"
		broadAddress, broadHealthChecks := startPilotBackend(t, broadResponse)
		broad := spec.Routes[0]
		broad.Spec.Identity.RouteNamespace = "broad-site"
		broad.Spec.Identity.RouteName = "broad"
		broad.Spec.Identity.RouteUID = "broad-route-uid"
		broad.Spec.PathType = cloud.PathMatchPrefix
		broad.Spec.Members = []cloud.Member{{Address: broadAddress.IP.String(), Port: broadAddress.Port}}
		spec.Routes[0].Spec.Precedence = 1
		spec.Routes = append(spec.Routes, broad)
		converge(provider, spec)
		stopOpenProxy()
		stopOpenProxy = startProxy()
		expectBackend("/api", broadResponse, broadHealthChecks)
		expectBackend("/api/extra", broadResponse, broadHealthChecks)

		// Switch only precedence so the same overlap reaches the other pool.
		spec.Routes[0].Spec.Precedence, spec.Routes[1].Spec.Precedence = 0, 1
		beforeReordering := len(writes)
		settled := converge(provider, spec)
		t.Logf("shared route precedence change applied %v", writes[beforeReordering:])
		initialWrites = len(writes)
		provider = newProvider()
		rediscovered := reconcile(provider, spec)
		if len(rediscovered.Routes) != 2 || !reflect.DeepEqual(rediscovered, settled) || len(writes) != initialWrites {
			t.Fatalf("shared graph rediscovery = %#v, writes %v", rediscovered, writes[initialWrites:])
		}
		t.Log("fresh provider rediscovered both shared routes without writes")
		stopOpenProxy()
		stopOpenProxy = startProxy()
		expectBackend("/api", response, healthChecks)
		expectBackend("/api/extra", broadResponse, broadHealthChecks)
		expectMiss(host, "/apiculture")
		expectMiss("apiXexample.com", "/api")

		spec.Routes[0].Disposition = cloud.GraphDelete
		spec.Routes[1].Spec.Precedence = 0
		converge(provider, spec)
		if slices.Contains(writes[initialWrites:], "PUT "+gateway) || inventory[gateway]["admin_state_up"] != true ||
			inventory[listener+"/listener"]["admin_state_up"] != true {
			t.Fatalf("removing a route changed Gateway traffic state: %v", writes[initialWrites:])
		}
		spec.Routes = spec.Routes[1:]
		initialWrites = len(writes)
		provider = newProvider()
		remaining := reconcile(provider, spec)
		if remaining.Outcome.State != cloud.OutcomeReady || len(remaining.Routes) != 1 || len(writes) != initialWrites {
			t.Fatalf("remaining route did not rediscover without writes: %#v, writes %v", remaining, writes[initialWrites:])
		}
		index := slices.IndexFunc(settled.Routes, func(result cloud.RouteGraphResult) bool { return result.Identity == broad.Spec.Identity })
		if index < 0 || !reflect.DeepEqual(settled.Routes[index], remaining.Routes[0]) {
			t.Fatalf("remaining route resources changed: before %#v, after %#v", settled.Routes, remaining.Routes[0])
		}
		stopOpenProxy()
		stopOpenProxy = startProxy()
		expectBackend("/api", broadResponse, broadHealthChecks)
		expectBackend("/api/extra", broadResponse, broadHealthChecks)
		expectMiss(host, "/apiculture")
		t.Log("removing the exact route preserved the prefix route's resources and traffic")
	}
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

func startPilotBackend(t *testing.T, response string) (*net.TCPAddr, *atomic.Int32) {
	t.Helper()
	var healthChecks atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/healthz" {
			healthChecks.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, response+" "+req.Host)
	}))
	t.Cleanup(backend.Close)
	return backend.Listener.Addr().(*net.TCPAddr), &healthChecks
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

func startPilotHAProxy(t *testing.T, ctx context.Context, python, haproxy string, inventory []byte, renderArgs ...string) func() {
	t.Helper()
	// Keep the stats socket under the platform's Unix socket path limit.
	dir, err := os.MkdirTemp("", "hibiscus-haproxy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	args := append([]string{"-I", "testdata/render_hibiscus_amphora.py", dir}, renderArgs...)
	render := exec.CommandContext(ctx, python, args...)
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

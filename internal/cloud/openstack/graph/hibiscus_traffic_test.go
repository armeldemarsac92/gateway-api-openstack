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
	"regexp"
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
	testHibiscusHTTPTraffic(t, false, false, false, false, nil, false)
}

func TestHibiscusSharedHTTPTraffic(t *testing.T) {
	testHibiscusHTTPTraffic(t, true, false, false, false, nil, false)
}

func TestHibiscusDistinctHTTPListenerPorts(t *testing.T) {
	testHibiscusHTTPTraffic(t, false, true, false, false, nil, false)
}

func TestScopedHTTPBackendPathGroups(t *testing.T) {
	testHibiscusHTTPTraffic(t, false, true, false, true, nil, false)
}

func TestUnscopedHTTPBackendRecovery(t *testing.T) {
	testHibiscusHTTPTraffic(t, false, false, false, true, nil, false)
}

func TestScopedHTTPSListenerLifecycle(t *testing.T) {
	testHibiscusHTTPTraffic(t, false, true, true, false, nil, false)
}

func TestIsolatedHTTPSListenerLifecycle(t *testing.T) {
	testHibiscusHTTPTraffic(t, false, true, true, false, nil, true)
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
	testHibiscusHTTPTraffic(t, false, false, false, false, &kindHTTPBackend{endpoint: endpoint, pod: pod, namespace: namespace}, false)
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

func testHibiscusHTTPTraffic(t *testing.T, shared, distinct, https, grouped bool, worker *kindHTTPBackend, isolated bool) {
	python, haproxy := os.Getenv("HIBISCUS_OCTAVIA_PYTHON"), os.Getenv("HIBISCUS_HAPROXY")
	renderAvailable := python != "" || haproxy != ""
	if worker == nil && !distinct && !grouped && !renderAvailable {
		t.Skip("set HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY to run the loopback pilot")
	}
	if (renderAvailable || worker != nil) && (!filepath.IsAbs(python) || !filepath.IsAbs(haproxy)) {
		t.Fatal("HIBISCUS_OCTAVIA_PYTHON and HIBISCUS_HAPROXY must be absolute paths")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if renderAvailable {
		version, err := exec.CommandContext(ctx, haproxy, "-v").CombinedOutput()
		if err != nil {
			t.Fatalf("read HAProxy version: %v\n%s", err, version)
		}
		t.Log(strings.SplitN(string(version), "\n", 2)[0])
	}

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
	selectedPool := func(listenerID, hostname, requestPath string) string {
		if inventory[gateway]["admin_state_up"] != true || inventory[listener+"/"+listenerID]["admin_state_up"] != true {
			return ""
		}
		for _, entry := range orderedPolicies(listenerID) {
			if entry["admin_state_up"] != true || entry["provisioning_status"] != "ACTIVE" {
				continue
			}
			rules := list(policy + "/" + entry["id"].(string) + "/rules")
			if len(rules) == 0 {
				continue
			}
			if !slices.ContainsFunc(rules, func(rule record) bool {
				if rule["admin_state_up"] != true || rule["provisioning_status"] != "ACTIVE" {
					return true
				}
				value, input := rule["value"].(string), requestPath
				if rule["type"] == "HOST_NAME" {
					input = hostname
				}
				switch rule["compare_type"] {
				case "EQUAL_TO":
					return input != value
				case "STARTS_WITH":
					return !strings.HasPrefix(input, value)
				case "REGEX":
					matched, err := regexp.MatchString(value, input)
					if err != nil {
						t.Fatal(err)
					}
					return !matched
				default:
					t.Fatalf("unexpected rule: %v", rule)
					return true
				}
			}) {
				return entry["redirect_pool_id"].(string)
			}
		}
		return ""
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
	ids := func(items []record, match func(record) bool) []record {
		refs := []record{}
		for _, item := range items {
			if match(item) {
				refs = append(refs, record{"id": item["id"]})
			}
		}
		return refs
	}
	var disagreePoolListeners, disagreePoolMonitor string
	var loseCertificateUpdateReply, holdMemberDisable bool
	var poolListReads int
	isolationAdvertised := isolated
	var omitVirtualHosts, enableHTTPFilters bool
	var omitPolicyRowsFor, omitURLRewriteFor string
	allowedHTTPPorts := []int{frontendAddress.Port}
	readback := func(collection string, item record) record {
		if !distinct && !grouped {
			return item
		}
		data := maps.Clone(item)
		id := item["id"]
		switch collection {
		case lb:
			data["listeners"] = ids(list(listener), func(child record) bool { return child["loadbalancer_id"] == id })
			data["pools"] = ids(list(pool), func(child record) bool { return child["loadbalancer_id"] == id })
		case listener:
			data["l7policies"] = ids(orderedPolicies(id), func(record) bool { return true })
			if omitVirtualHosts {
				delete(data, "https_virtual_hosts")
			}
		case policy:
			data["rules"] = ids(list(policy+"/"+id.(string)+"/rules"), func(record) bool { return true })
		case pool:
			data["listeners"] = ids(list(listener), func(entry record) bool {
				return entry["default_pool_id"] == id || slices.ContainsFunc(orderedPolicies(entry["id"]), func(policy record) bool {
					return policy["redirect_pool_id"] == id && policy["admin_state_up"] == true && len(list("/v2/lbaas/l7policies/"+policy["id"].(string)+"/rules")) != 0
				})
			})
			if id == disagreePoolListeners {
				data["listeners"] = []record{}
			}
			data["members"] = ids(list(pool+"/"+id.(string)+"/members"), func(record) bool { return true })
			// Stock Octavia omits healthmonitor_id until a monitor is attached.
			for _, monitor := range list("/v2/lbaas/healthmonitors") {
				if monitor["pool_id"] == id && id != disagreePoolMonitor {
					data["healthmonitor_id"] = monitor["id"]
				}
			}
		}
		return data
	}
	cloudAPI := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resource := req.URL.Path
		if distinct && req.Method == http.MethodGet {
			switch resource {
			case "/":
				safetyWriteJSON(t, w, record{"versions": []record{{"id": "v2.29", "status": "CURRENT"}}})
				return
			case "/v2/lbaas/providers":
				safetyWriteJSON(t, w, record{"providers": []record{{"name": "amphora"}}})
				return
			case "/v2/lbaas/providers/amphora/gateway_capabilities":
				var capabilities []string
				if isolated {
					if !isolationAdvertised {
						http.Error(w, "discovery unavailable", http.StatusServiceUnavailable)
						return
					}
					capabilities = append(capabilities, octavia.GatewayCapabilityHTTPSListenerIsolation)
				}
				if enableHTTPFilters {
					capabilities = append(capabilities, octavia.GatewayCapabilityRequestCORS, octavia.GatewayCapabilityRequestURLRewrite)
				}
				if len(capabilities) == 0 {
					t.Error("unexpected Gateway capability discovery")
				}
				safetyWriteJSON(t, w, record{"gateway_capabilities": capabilities})
				return
			case "/v2.0/subnets/vip-subnet":
				safetyWriteJSON(t, w, record{"subnet": record{"id": "vip-subnet", "network_id": "private-net", "project_id": hibiscusProject, "cidr": "192.0.2.0/24", "ip_version": 4}})
				return
			case "/v2.0/security-groups/sg-a":
				var rules []record
				for _, port := range allowedHTTPPorts {
					rules = append(rules, record{"direction": "ingress", "ethertype": "IPv4", "protocol": "tcp", "port_range_min": port, "port_range_max": port})
				}
				safetyWriteJSON(t, w, record{"security_group": record{"id": "sg-a", "project_id": hibiscusProject, "security_group_rules": rules}})
				return
			}
		}
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
			if resource == pool {
				poolListReads++
			}
			if id != "" && entry != nil {
				safetyWriteJSON(t, w, record{kind.key: readback(collection, entry)})
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
				if parentID == omitPolicyRowsFor {
					items = []record{}
				}
			}
			items = slices.DeleteFunc(items, func(data record) bool {
				if kind.parentKey != "" && data[kind.parentKey] != parentID {
					return true
				}
				tags := req.URL.Query().Get("tags")
				return tags != "" && !openstackidentity.HasAllTags(pilotTags(data["tags"]), strings.Split(tags, ","))
			})
			for i, item := range items {
				items[i] = readback(collection, item)
				if collection == policy && parentID == omitURLRewriteFor {
					delete(items[i], "request_url_rewrite")
				}
			}
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
			if isolated && collection == policy {
				parent, scope := data["listener_id"], data["https_listener_hostname"]
				if entry != nil {
					parent = entry["listener_id"]
					if _, set := data["https_listener_hostname"]; !set {
						scope = entry["https_listener_hostname"]
					}
				}
				hosts, _ := inventory[listener+"/"+parent.(string)]["https_virtual_hosts"].([]any)
				if len(hosts) != 0 && !slices.Contains(hosts, scope) || len(hosts) == 0 && scope != nil {
					t.Errorf("policy does not select exactly one native HTTPS section: %v", data)
					http.Error(w, "invalid section", http.StatusBadRequest)
					return
				}
				if data["https_listener_hostname"] != nil && data["admin_state_up"] != false {
					t.Errorf("policy scope was written without an administrative close: %v", data)
				}
			}
			if isolated && collection == listener && data["https_virtual_hosts"] != nil {
				switch {
				case req.Method == http.MethodPost && (data["admin_state_up"] != false || !slices.Contains(pilotTags(data["tags"]), httpsIsolationTag)):
					t.Errorf("new isolated listener did not start closed with durable enrollment: %v", data)
				case req.Method == http.MethodPut && (entry["admin_state_up"] != false || len(orderedPolicies(entry["id"])) != 0):
					t.Errorf("changed native HTTPS hosts while the port or old selectors still existed: %v", entry)
				}
			}
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
				if data["protocol"] == "TERMINATED_HTTPS" {
					data["sni_container_refs"] = []string{}
				}
				if _, specified := data["invert"]; (isolated || enableHTTPFilters) && name == "rules" && !specified {
					data["invert"] = false
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
			} else if req.Method == http.MethodPut && entry != nil &&
				(resource == gateway || collection == policy || distinct && collection == listener || (grouped || isolated) && (name == "members" || name == "rules")) {
				if position, exists := data["position"]; collection == policy && exists {
					placePolicy(entry, position)
					delete(data, "position")
				}
				if holdMemberDisable && name == "members" && len(data) == 1 && data["admin_state_up"] == false {
					entry["provisioning_status"] = "PENDING_UPDATE"
				}
				maps.Copy(entry, data)
				if collection == listener && data["default_tls_container_ref"] != nil && loseCertificateUpdateReply {
					loseCertificateUpdateReply = false
					w.WriteHeader(http.StatusGatewayTimeout)
					return
				}
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
				((collection == lb || collection == listener && !distinct) && inventory[gateway]["admin_state_up"] != false) ||
				collection == listener && len(orderedPolicies(entry["id"])) != 0 {
				reject()
				return
			}
			delete(inventory, resource)
			if collection == policy {
				if isolated {
					for _, rule := range list(resource + "/rules") {
						delete(inventory, resource+"/rules/"+rule["id"].(string))
					}
				}
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
		clients := hibiscusServiceClients(cloudAPI)
		if distinct {
			clients.LoadBalancer.Microversion = "2.29"
		}
		return newTestProvider(t, clients, ProviderConfig{HTTPSListenerIsolation: isolated, HTTPCORS: enableHTTPFilters, HTTPRequestURLRewrite: enableHTTPFilters})
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
	if distinct {
		spec.Gateway.Spec.VIPSecurityGroupIDs = []string{"sg-a"}
		route.GatewayRequirements.VIPSecurityGroupIDs = []string{"sg-a"}
	}

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
	if grouped {
		const secondResponse = "second-path-backend"
		secondBackend, _ := startPilotBackend(t, secondResponse)
		secondMember := cloud.Member{Address: secondBackend.IP.String(), Port: secondBackend.Port}
		var siblingReservation net.Listener
		legacy := spec.Routes[0]
		peer := legacy
		peer.Spec.Identity.RouteName, peer.Spec.Identity.RouteUID = "same-port-peer", "same-port-peer-uid"
		peer.Spec.Hostname, peer.Spec.PathType, peer.Spec.PathValue = "", cloud.PathMatchExact, "/peer"
		peer.Spec.Precedence = 2
		peer.Spec.Members = []cloud.Member{secondMember}
		spec.Routes = append(spec.Routes, peer)
		converge(newProvider(), spec)
		if distinct {
			siblingReservation, err = net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = siblingReservation.Close() }()
			siblingPort := siblingReservation.Addr().(*net.TCPAddr).Port
			allowedHTTPPorts = append(allowedHTTPPorts, siblingPort)
			spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
				{Name: "first", Port: frontendAddress.Port}, {Name: "sibling", Port: siblingPort},
			}
			for i := range spec.Routes {
				spec.Routes[i].Spec.ScopedListenerPorts = []int{frontendAddress.Port}
			}
			legacy = spec.Routes[0]
			sibling := peer
			sibling.Spec.Identity.RouteName, sibling.Spec.Identity.RouteUID = "sibling", "sibling-uid"
			sibling.Spec.ScopedListenerPorts = []int{siblingPort}
			spec.Routes = append(spec.Routes, sibling)
			converge(newProvider(), spec)
		}
		// A path prefix stays ahead of an independent healthy catch-all while
		// changing between the scalar pair and the grouped regular expression.
		prefix, catchAll := legacy, spec.Routes[1]
		prefix.Spec.PathType, prefix.Spec.PathValue = cloud.PathMatchPrefix, "/migration/"
		catchAll.Spec.PathType, catchAll.Spec.PathValue = cloud.PathMatchPrefix, "/"
		spec.Routes[0], spec.Routes[1] = prefix, catchAll
		prefixResult := converge(newProvider(), spec)
		var migrationListener, oldPool, catchAllPool string
		for _, item := range list(listener) {
			if item["protocol_port"] == float64(frontendAddress.Port) {
				migrationListener = item["id"].(string)
			}
		}
		for _, route := range prefixResult.Routes {
			switch route.Identity {
			case prefix.Spec.Identity:
				oldPool = route.Result.State.PoolID
			case catchAll.Spec.Identity:
				catchAllPool = route.Result.State.PoolID
			}
		}
		servesMember := func(poolID string) bool {
			members := list(pool + "/" + poolID + "/members")
			return len(members) == 1 && members[0]["admin_state_up"] == true
		}
		migrate := func(next cloud.RouteGraphNode, oldPool string) string {
			t.Helper()
			spec.Routes[0] = next
			lostMatch := false
			for range 50 {
				before, fences := len(writes), 0
				spec.ValidateMutation = func(context.Context) error { fences++; return nil }
				result := reconcile(newProvider(), spec)
				spec.ValidateMutation = nil
				if len(writes) > before+1 || len(writes)-before != fences {
					t.Fatalf("migration did not fence each single write: %v, %d fences", writes[before:], fences)
				}
				host := prefix.Spec.Hostname
				base := selectedPool(migrationListener, host, "/migration")
				child := selectedPool(migrationListener, host, "/migration/child")
				if !lostMatch && (base == "" || child == "" || base == catchAllPool || child == catchAllPool) {
					t.Errorf("migration after %v lost the prefix: bare path selected %q, child selected %q, catch-all %q", writes[before:], base, child, catchAllPool)
					lostMatch = true
				}
				if selectedPool(migrationListener, host, "/migration-other") != catchAllPool ||
					selectedPool(migrationListener, "other.example", "/migration") != catchAllPool {
					t.Fatalf("migration widened a path or hostname predicate: %v", writes[before:])
				}
				if len(next.Spec.HTTPBackends) != 0 && (base == oldPool || child == oldPool) && !servesMember(oldPool) {
					t.Fatalf("migration disabled a legacy member while its policy was still selected: %v", writes[before:])
				}
				if result.Outcome.State == cloud.OutcomeReady {
					if base != child || base == oldPool || inventory[pool+"/"+oldPool] != nil || !servesMember(base) {
						t.Fatalf("migration did not converge to the replacement pool: bare %q, child %q, old %q", base, child, oldPool)
					}
					return base
				}
			}
			t.Fatal("prefix migration did not converge")
			return ""
		}
		groupedPrefix := prefix
		groupedPrefix.Spec.Hostname, groupedPrefix.Spec.PathType, groupedPrefix.Spec.PathValue, groupedPrefix.Spec.Members = "", "", "", nil
		groupedPrefix.Spec.HTTPBackends = []cloud.HTTPBackend{{
			Namespace: "default", Name: "first", Port: 80, Members: prefix.Spec.Members,
			Matches: []cloud.HTTPPathMatch{{Hostname: prefix.Spec.Hostname, PathType: prefix.Spec.PathType, PathValue: prefix.Spec.PathValue}},
		}}
		groupedPool := migrate(groupedPrefix, oldPool)
		migrate(prefix, groupedPool)
		spec.Routes[0], spec.Routes[1] = legacy, peer
		if distinct {
			spec.Routes[1].Spec.ScopedListenerPorts = []int{frontendAddress.Port}
		}
		legacyResult := converge(newProvider(), spec)

		// Appending routes can move their backing array.
		route = &spec.Routes[0].Spec
		firstMember := route.Members[0]
		route.Hostname, route.PathType, route.PathValue, route.Members = "", "", "", nil
		route.HTTPBackends = []cloud.HTTPBackend{
			{Namespace: "default", Name: "first", Port: 80, Members: []cloud.Member{firstMember}, Matches: []cloud.HTTPPathMatch{
				{PathType: cloud.PathMatchExact, PathValue: "/shop/item", Precedence: 0},
				{PathType: cloud.PathMatchPrefix, PathValue: "/shop", Precedence: 3},
			}},
			{Namespace: "default", Name: "second", Port: 80, Members: []cloud.Member{secondMember}, Matches: []cloud.HTTPPathMatch{
				{PathType: cloud.PathMatchPrefix, PathValue: "/shop/sale", Precedence: 1},
			}},
		}
		// Treat each call as a fresh controller process and permit one write.
		settle := func() cloud.GatewayGraphResult {
			t.Helper()
			for range 50 {
				before := len(writes)
				result := reconcile(newProvider(), spec)
				if len(writes) > before+1 {
					t.Fatalf("one graph pass made several writes: %v", writes[before:])
				}
				if result.Outcome.State == cloud.OutcomeReady {
					return result
				}
			}
			t.Fatalf("grouped routes failed to converge: %v", writes)
			return cloud.GatewayGraphResult{}
		}
		result := settle()
		if len(result.Routes) != len(spec.Routes) || len(result.Gateway.ScopedListenerReadyPorts) != len(spec.Gateway.Spec.ScopedListeners) {
			t.Fatalf("grouping changed logical route or listener results: %#v", result)
		}
		states := make(map[string]cloud.RouteState)
		for _, observed := range result.Routes {
			if observed.Result.Outcome.State != cloud.OutcomeReady {
				t.Fatalf("logical route was not ready: %#v", observed)
			}
			states[observed.Identity.RouteUID] = observed.Result.State
		}
		first := states[route.Identity.RouteUID]
		if len(first.PoolIDs) != 2 || len(first.MonitorIDs) != 2 || len(first.L7PolicyIDs) != 3 ||
			inventory[pool+"/"+legacyResult.Routes[0].Result.State.PoolID] != nil {
			t.Fatalf("expected three policies over two pools and removal of the legacy pool: %#v", first)
		}
		var listenerID string
		for _, item := range list(listener) {
			if item["protocol_port"] == float64(frontendAddress.Port) {
				listenerID = item["id"].(string)
			}
		}
		pathOrder, poolOrder := []string{}, []string{}
		for _, entry := range orderedPolicies(listenerID) {
			for _, rule := range list(policy + "/" + entry["id"].(string) + "/rules") {
				if rule["type"] == "PATH" {
					pathOrder = append(pathOrder, rule["value"].(string))
					poolOrder = append(poolOrder, entry["redirect_pool_id"].(string))
				}
			}
		}
		if !slices.Equal(pathOrder, []string{"/shop/item", "^/shop/sale($|/)", "/peer", "^/shop($|/)"}) ||
			poolOrder[0] != poolOrder[3] || poolOrder[0] == poolOrder[1] || len(list(pool)) != len(spec.Routes)+1 {
			t.Fatalf("listener paths and redirect pools = %v, %v", pathOrder, poolOrder)
		}
		before := len(writes)
		if again := settle(); len(writes) != before || !reflect.DeepEqual(again.Routes, result.Routes) {
			t.Fatalf("restarting a converged group changed resources: %v", writes[before:])
		}
		// A policy may only reference the exact backend encoded by its
		// own role, even if the other pool has the same route UID.
		policies := orderedPolicies(listenerID)
		policies[1]["redirect_pool_id"] = poolOrder[0]
		if _, err := newProvider().ReconcileGatewayGraph(ctx, spec); !errors.Is(err, cloud.ErrOwnershipConflict) || len(writes) != before {
			t.Fatalf("cross-backend policy was treated as owned: %v, writes %v", err, writes[before:])
		}
		policies[1]["redirect_pool_id"] = poolOrder[1]
		probe := func(unavailable bool) {
			t.Helper()
			if !renderAvailable || !distinct {
				return
			}
			data, err := json.Marshal(inventory)
			if err != nil {
				t.Fatal(err)
			}
			stop := startPilotHAProxy(t, ctx, python, haproxy, data)
			defer stop()
			cases := []struct {
				address, path, body string
				status              int
			}{
				{frontendAddress.String(), "/shop/item", response, http.StatusOK},
				{frontendAddress.String(), "/shop/sale", secondResponse, http.StatusOK},
				{frontendAddress.String(), "/shop/sale/deep", secondResponse, http.StatusOK},
				{frontendAddress.String(), "/shop/sales", response, http.StatusOK},
				{siblingReservation.Addr().String(), "/peer", secondResponse, http.StatusOK},
			}
			if unavailable {
				cases[1].status, cases[2].status = http.StatusServiceUnavailable, http.StatusServiceUnavailable
			}
			client := &http.Client{Timeout: time.Second}
			for _, test := range cases {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+test.address+test.path, nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Host = "path.example"
				got, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				body, readErr := io.ReadAll(got.Body)
				if closeErr := got.Body.Close(); readErr != nil || closeErr != nil ||
					got.StatusCode != test.status || test.status == http.StatusOK && string(body) != test.body+" "+request.Host {
					t.Fatalf("stock HAProxy GET %s%s = %d %q, want %d %q", test.address, test.path, got.StatusCode, body, test.status, test.body)
				}
			}
		}
		if renderAvailable && distinct {
			if err := errors.Join(reservation.Close(), siblingReservation.Close()); err != nil {
				t.Fatal(err)
			}
		}
		probe(false)

		// The specific path keeps its enabled policy while Octavia disables
		// its member, so HAProxy does not fall through to the healthy prefix.
		backend := &route.HTTPBackends[1]
		backend.Unavailable, backend.Members = true, nil
		memberCollection := pool + "/" + poolOrder[1] + "/members"
		member := list(memberCollection)[0]
		memberPath := memberCollection + "/" + member["id"].(string)
		fenceErr := errors.New("backend authorization changed during observation")
		spec.ValidateMutation = func(context.Context) error { return fenceErr }
		if _, err := newProvider().ReconcileGatewayGraph(ctx, spec); !errors.Is(err, fenceErr) || len(writes) != before {
			t.Fatalf("member withdrawal bypassed the mutation fence: %v, writes %v", err, writes[before:])
		}
		spec.ValidateMutation = nil
		holdMemberDisable = true
		withdrawal, err := newProvider().ReconcileGatewayGraph(ctx, spec)
		if err != nil || !withdrawal.Outcome.MutationSubmitted || !withdrawal.Routes[0].Result.Outcome.MutationSubmitted {
			t.Fatalf("member withdrawal did not request a fresh observation: %v, %#v", err, withdrawal)
		}
		if pending := reconcile(newProvider(), spec); pending.Outcome.State != cloud.OutcomeProgressing || len(writes) != before+1 ||
			member["admin_state_up"] != false {
			t.Fatalf("pending member disable was accepted as convergence: %#v, writes %v", pending, writes[before:])
		}
		// Octavia records the intended admin state before reloading HAProxy.
		// Its single-member worker leaves the member ERROR after a failed reload.
		member["provisioning_status"] = "ERROR"
		retry := reconcile(newProvider(), spec)
		if !retry.Outcome.MutationSubmitted || !slices.Equal(writes[before:], []string{"PUT " + memberPath, "PUT " + memberPath}) {
			t.Fatalf("failed disable was not retried on the same member: %#v, writes %v", retry, writes[before:])
		}
		holdMemberDisable = false
		member["provisioning_status"] = "ACTIVE"
		disabled := settle()
		if len(writes) != before+2 || inventory[memberPath] == nil || member["admin_state_up"] != false ||
			policies[1]["admin_state_up"] != true || !slices.ContainsFunc(disabled.Routes, func(route cloud.RouteGraphResult) bool {
			return route.Result.Outcome.State == cloud.OutcomeReady && slices.Equal(route.Result.State.PoolIDs, first.PoolIDs) &&
				slices.Equal(route.Result.State.MemberIDs, first.MemberIDs)
		}) {
			t.Fatalf("unavailable backend did not retain its match and disabled member: %v, %#v", writes[before:], disabled)
		}
		probe(true)
		backend.Unavailable, backend.Members = false, []cloud.Member{secondMember}
		member["provisioning_status"] = "ERROR"
		before = len(writes)
		reconcile(newProvider(), spec)
		if !slices.Equal(writes[before:], []string{"PUT " + memberPath}) || member["admin_state_up"] != true {
			t.Fatalf("restoring authorization did not re-enable the same member: %v", writes[before:])
		}
		member["provisioning_status"] = "ACTIVE"
		settle()

		before = len(writes)
		exact := &route.HTTPBackends[0].Matches[0]
		broad := &route.HTTPBackends[0].Matches[1]
		exact.Precedence = 2 // Conflicts with the scalar peer on this listener.
		if _, err := newProvider().ReconcileGatewayGraph(ctx, spec); !errors.Is(err, cloud.ErrTerminalValidation) || len(writes) != before {
			t.Fatalf("accepted an ambiguous policy rank: %v, writes %v", err, writes[before:])
		}
		exact.Precedence, broad.Precedence = 3, 0
		settle()
		newOrder := orderedPolicies(listenerID)
		want := []string{policies[3]["id"].(string), policies[1]["id"].(string),
			policies[2]["id"].(string), policies[0]["id"].(string)}
		for i, item := range newOrder {
			if item["id"] != want[i] || item["admin_state_up"] != true {
				t.Fatalf("rank update recreated or disabled an existing policy: %v", writes[before:])
			}
		}
		for _, write := range writes[before:] {
			if !strings.HasPrefix(write, "PUT "+policy+"/") {
				t.Fatalf("rank update mutated something besides policy positions: %v", writes[before:])
			}
		}

		// Stage an identical match before retiring it from the original
		// backend. Observe every API transition with the broader pool serving.
		exact.Precedence, broad.Precedence = 0, 3
		backend.Matches = append(backend.Matches, cloud.HTTPPathMatch{PathType: cloud.PathMatchExact, PathValue: "/kept", Precedence: 4})
		settle()
		poolForPath := func(requestPath string) string {
			return selectedPool(listenerID, "path.example", requestPath)
		}
		replaceBackend := func(oldPool string) string {
			t.Helper()
			for range 50 {
				before := len(writes)
				result := reconcile(newProvider(), spec)
				base, deep := poolForPath("/shop/sale"), poolForPath("/shop/sale/deep")
				if len(writes) > before+1 || base == "" || deep == "" || base == poolOrder[0] || deep == poolOrder[0] || poolForPath("/kept") != poolOrder[1] {
					t.Fatalf("a transition lost the specific predicate or the retained match: base %q deep %q, writes %v", base, deep, writes[before:])
				}
				if result.Outcome.State == cloud.OutcomeReady {
					if base != deep || base == oldPool || len(list(pool+"/"+base+"/members")) != 0 {
						t.Fatalf("the normalized prefix and its exact path must select the new unavailable target, got %q and %q", base, deep)
					}
					return base
				}
			}
			t.Fatal("replacement did not converge")
			return ""
		}
		sale := backend.Matches[0]
		backend.Matches = backend.Matches[1:]
		route.HTTPBackends = append(route.HTTPBackends, cloud.HTTPBackend{
			Namespace: "remote", Name: "denied", Port: 80, Unavailable: true, Matches: []cloud.HTTPPathMatch{sale},
		})
		newPool := replaceBackend(poolOrder[1])
		if member["admin_state_up"] != true {
			t.Fatal("moving one match disabled the backend that still serves /kept")
		}
		current := &route.HTTPBackends[2]
		current.Unavailable, current.Members = false, []cloud.Member{secondMember}
		settle()
		newMember := list(pool + "/" + newPool + "/members")[0]
		newMemberPath := pool + "/" + newPool + "/members/" + newMember["id"].(string)
		route.HTTPBackends[2] = cloud.HTTPBackend{
			Namespace: "remote", Name: "replacement-denied", Port: 80, Unavailable: true, Matches: []cloud.HTTPPathMatch{sale},
		}
		before = len(writes)
		replaceBackend(newPool)
		if writes[before] != "PUT "+newMemberPath || inventory[pool+"/"+newPool] != nil {
			t.Fatalf("obsolete backend did not disable its member before handing off the predicate and cleaning up: %v", writes[before:])
		}
		probe(true)

		// Identical paths in distinct backends are two rule candidates.
		// The first rank wins even when the other Service name sorts earlier.
		route.HTTPBackends[1].Matches = append(route.HTTPBackends[1].Matches, cloud.HTTPPathMatch{
			PathType: cloud.PathMatchExact, PathValue: "/shop/item", Precedence: 5,
		})
		settle()
		if poolForPath("/shop/item") != poolOrder[0] {
			t.Fatal("an identical later match overrode the first rule")
		}
		route.HTTPBackends[0].Matches[0].Precedence, route.HTTPBackends[1].Matches[1].Precedence = 5, 0
		final := settle()
		if poolForPath("/shop/item") != poolOrder[1] {
			t.Fatal("reordering identical matches did not update the winning backend")
		}
		duplicate := sale
		duplicate.PathValue, duplicate.Precedence = "/shop/sale/", 6
		route.HTTPBackends[2].Matches = append(route.HTTPBackends[2].Matches, duplicate)
		before = len(writes)
		if _, err := newProvider().ReconcileGatewayGraph(ctx, spec); !errors.Is(err, cloud.ErrTerminalValidation) || len(writes) != before {
			t.Fatalf("duplicate normalized prefixes within one backend were accepted: %v, writes %v", err, writes[before:])
		}
		route.HTTPBackends[2].Matches = route.HTTPBackends[2].Matches[:1]

		if distinct {
			// Returning to scalar ownership withdraws all versioned policies
			// and pools while keeping the unrelated routes and listeners.
			spec.Routes[0] = legacy
			restored := settle()
			for _, id := range final.Routes[0].Result.State.PoolIDs {
				if inventory[pool+"/"+id] != nil {
					t.Fatalf("scalar migration retained versioned pool %s", id)
				}
			}
			if len(restored.Routes) != 3 || !slices.ContainsFunc(restored.Routes, func(route cloud.RouteGraphResult) bool {
				return route.Identity == spec.Routes[2].Spec.Identity && route.Result.State.PoolID == legacyResult.Routes[2].Result.State.PoolID
			}) {
				t.Fatalf("scalar migration changed the sibling route: %#v", restored)
			}
		}

		// A Gateway can be recovered from provider state after its route
		// models were lost. Keep the unscoped variant versioned here.
		spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete}
		spec.Routes = nil
		before = len(writes)
		inventory[listener+"/"+listenerID]["provisioning_status"] = "PENDING_UPDATE"
		if result := reconcile(newProvider(), spec); result.Outcome.State != cloud.OutcomeProgressing || len(writes) != before {
			t.Fatalf("Gateway recovered before its frontend settled: %v, writes %v", result.Outcome, writes[before:])
		}
		inventory[listener+"/"+listenerID]["provisioning_status"] = "ACTIVE"
		for range 48 {
			if len(inventory) == len(protected) {
				break
			}
			before := len(writes)
			reconcile(newProvider(), spec)
			if len(writes) > before+1 {
				t.Fatalf("recovery made several writes in one pass: %v", writes[before:])
			}
			if inventory[gateway] != nil {
				inventory[gateway]["provisioning_status"] = "ACTIVE"
			}
		}
		if len(inventory) != len(protected) {
			t.Fatalf("model-less Gateway recovery left descendants: %v", writes)
		}
		return
	}
	if https {
		const refA = "https://barbican.example/v1/secrets/revision-a"
		const refB = "https://barbican.example/v1/secrets/revision-b"
		httpsPort := 8443
		if httpsPort == frontendAddress.Port {
			httpsPort++
		}
		allowedHTTPPorts = append(allowedHTTPPorts, httpsPort)
		spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
		spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{{Name: "https", Port: httpsPort, TLS: &cloud.ListenerTLS{}}}
		desiredRoutes := spec.Routes
		spec.Routes = nil
		if result := reconcile(provider, spec); result.Outcome.State != cloud.OutcomeProgressing || len(writes) != 0 {
			t.Fatalf("denied HTTPS allocated resources: %v, writes %v", result.Outcome, writes)
		}

		spec.Gateway.Spec.ScopedListeners = append(spec.Gateway.Spec.ScopedListeners, cloud.ScopedListener{Name: "http", Port: frontendAddress.Port})
		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
		spec.Routes = desiredRoutes
		spec.Routes[0].Spec.ScopedListenerPorts = []int{frontendAddress.Port}
		httpsRoute := spec.Routes[0]
		httpsRoute.Spec.Identity.RouteName, httpsRoute.Spec.Identity.RouteUID = "tls-route", "tls-route-uid"
		httpsRoute.Spec.ScopedListenerPorts = []int{httpsPort}
		const exactScope, wildcardScope = "api.example.test", "*.example.test"
		if isolated {
			spec.Gateway.Spec.ScopedListeners[0].TLS.VirtualHosts = []string{wildcardScope, exactScope}
			member := httpsRoute.Spec.Members
			httpsRoute.Spec.Hostname, httpsRoute.Spec.PathType, httpsRoute.Spec.PathValue, httpsRoute.Spec.Members = "", "", "", nil
			httpsRoute.Spec.HTTPBackends = []cloud.HTTPBackend{{
				Namespace: "test", Name: "api", Port: 80, Members: member, Matches: []cloud.HTTPPathMatch{
					{HTTPSListenerHostname: exactScope, Hostname: exactScope, PathType: cloud.PathMatchExact, PathValue: "/api"},
					{HTTPSListenerHostname: wildcardScope, Hostname: exactScope, PathType: cloud.PathMatchExact, PathValue: "/api"},
				},
			}}
		}
		spec.Routes = append(spec.Routes, httpsRoute)
		if !isolated {
			t.Run("start without an HTTPS certificate", func(t *testing.T) {
				cold := spec
				cold.Routes = slices.Clone(spec.Routes[:1])
				primary := &cold.Routes[0].Spec
				secondary := *primary
				secondary.ScopedListenerPorts, secondary.ScopeHTTPResources = []int{httpsPort}, true
				primary.AdditionalHTTPPorts = []cloud.RouteSpec{secondary}
				cold.Gateway.Spec.ScopedListeners = slices.Clone(spec.Gateway.Spec.ScopedListeners)
				cold.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
				before := len(writes)
				for range 24 {
					last := len(writes)
					result := reconcile(newProvider(), cold)
					if len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing ||
						result.Routes[0].Result.ObservationDeferred {
						t.Fatalf("an unavailable HTTPS port cannot certify the parent: %+v", result.Routes)
					}
					if len(writes) == last {
						break
					}
				}
				frontends := list(listener)
				if len(frontends) != 1 || frontends[0]["protocol"] != "HTTP" ||
					frontends[0]["admin_state_up"] != true || inventory[gateway]["admin_state_up"] != true {
					t.Fatalf("expected an enabled HTTP listener and no HTTPS listener, got %v, writes %v", frontends, writes[before:])
				}
				httpID := frontends[0]["id"].(string)
				original := routeStateFromInventoryForTest(t, orderedPolicies(httpID), inventory, pool, int(backendAddress.Port()))
				if got := selectedPool(httpID, primary.Hostname, primary.PathValue); got != original[1] {
					t.Fatalf("HTTP request selected pool %q with HTTPS unavailable, want %q", got, original[1])
				}
				cold.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
				result := converge(newProvider(), cold)
				if result.Routes[0].Result.Outcome.State != cloud.OutcomeReady || len(result.Routes[0].Result.State.L7PolicyIDs) != 2 ||
					!slices.Contains(result.Gateway.ScopedListenerReadyPorts, httpsPort) ||
					routeStateFromInventoryForTest(t, orderedPolicies(httpID), inventory, pool, int(backendAddress.Port())) != original {
					t.Fatalf("HTTPS recovery must complete the route and preserve its HTTP backend: %+v", result)
				}
			})
		}
		result := converge(newProvider(), spec)
		var tlsID, httpID string
		for _, frontend := range list(listener) {
			switch frontend["protocol"] {
			case terminatedHTTPS:
				tlsID = frontend["id"].(string)
			case cloud.ListenerProtocolHTTP:
				httpID = frontend["id"].(string)
			}
		}
		if tlsID == "" || httpID == "" {
			t.Fatal("expected both HTTP and HTTPS listeners")
		}
		tlsPath := listener + "/" + tlsID
		server := inventory[tlsPath]
		if len(result.Gateway.ScopedListenerReadyPorts) != 2 || server["protocol"] != "TERMINATED_HTTPS" ||
			server["default_tls_container_ref"] != refA || server["admin_state_up"] != true || server["default_pool_id"] != nil ||
			inventory[listener+"/"+httpID]["protocol"] != "HTTP" || len(orderedPolicies(tlsID)) == 0 ||
			inventory[pool+"/"+orderedPolicies(tlsID)[0]["redirect_pool_id"].(string)]["protocol"] != "HTTP" {
			t.Fatalf("HTTPS listener and HTTP routing did not converge: %v, writes %v", result.Gateway.Outcome, writes)
		}

		if !isolated {
			t.Run("one route on HTTPS and HTTP", func(t *testing.T) {
				want := spec
				want.Routes = slices.Clone(spec.Routes)
				primary := &want.Routes[0].Spec
				second := *primary
				second.ScopedListenerPorts, second.ScopeHTTPResources = []int{httpsPort}, true
				second.PathType, second.PathValue, second.Precedence = cloud.PathMatchExact, "/multi-port", 1
				primary.AdditionalHTTPPorts = []cloud.RouteSpec{second}
				original := routeStateFromInventoryForTest(t, orderedPolicies(httpID), inventory, pool, int(backendAddress.Port()))
				peerPolicy := orderedPolicies(tlsID)[0]["id"]

				assertReady := func(result cloud.GatewayGraphResult) {
					t.Helper()
					if len(result.Routes) != 2 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
						t.Fatalf("expected one ready result for the route with two ports: %+v", result.Routes)
					}
					state := result.Routes[0].Result.State
					if state.PoolID != original[1] || len(state.PoolIDs) != 1 || state.PoolIDs[0] == original[1] || len(state.L7PolicyIDs) != 2 {
						t.Fatalf("expected separate forwarding graphs across both ports: %+v", state)
					}
					rows := orderedPolicies(tlsID)
					if len(rows) != 2 || rows[0]["id"] != peerPolicy || rows[1]["admin_state_up"] != true ||
						rows[1]["redirect_pool_id"] != state.PoolIDs[0] {
						t.Fatalf("HTTPS policies did not retain the peer and reach the port's backend: %v", rows)
					}
				}
				result := reconcile(newProvider(), want)
				if len(result.Routes) != 2 || result.Routes[0].Result.Outcome.State != cloud.OutcomeProgressing {
					t.Fatalf("one existing port certified an incomplete route: %+v", result)
				}
				assertReady(converge(newProvider(), want))
				stableWrites := len(writes)
				assertReady(reconcile(newProvider(), want))
				if len(writes) != stableWrites {
					t.Fatalf("restart changed the two-port forwarding graph: %v", writes[stableWrites:])
				}

				finishWithPeer := func(desired cloud.GatewayGraphSpec) {
					t.Helper()
					converge(newProvider(), desired)
					rows := orderedPolicies(tlsID)
					if len(rows) != 1 || rows[0]["id"] != peerPolicy {
						t.Fatalf("retiring a port removed or replaced its unrelated peer: %v", rows)
					}
				}
				finishWithPeer(spec)
				if state := routeStateFromInventoryForTest(t, orderedPolicies(httpID), inventory, pool, int(backendAddress.Port())); state != original {
					t.Fatalf("retiring the additional port changed the anchor: %+v -> %+v", original, state)
				}
				assertReady(converge(newProvider(), want))

				want.Routes[0] = cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{Identity: primary.Identity}}
				finishWithPeer(want)
				if len(orderedPolicies(httpID)) != 0 || len(list(pool)) != 1 {
					t.Fatal("route deletion left forwarding policies or pools")
				}
				finishWithPeer(spec)
			})
		}

		if !isolated {
			t.Run("CORS and URL rewrite on both ports", func(t *testing.T) {
				enableHTTPFilters = true
				want := spec
				want.Gateway.Spec.ScopedListeners = slices.Clone(spec.Gateway.Spec.ScopedListeners)
				primary := spec.Routes[0].Spec
				primary.Identity.RouteName, primary.Identity.RouteUID = "filtered", "filtered-uid"
				primary.Hostname, primary.PathType, primary.PathValue, primary.Members = "", "", "", nil
				const requestPath = "/filtered"
				rewrittenPath := "/upstream"
				primary.HTTPBackends = []cloud.HTTPBackend{{
					Namespace: "test", Name: "backend", Port: 80, Members: spec.Routes[0].Spec.Members,
					Matches: []cloud.HTTPPathMatch{{PathType: cloud.PathMatchExact, PathValue: requestPath, Precedence: 1,
						CORS:       &cloud.HTTPCORS{AllowOrigins: []string{"https://client.example.test"}},
						URLRewrite: &cloud.HTTPURLRewrite{ReplaceFullPath: &rewrittenPath}}},
				}}
				secondary := primary
				secondary.ScopedListenerPorts, secondary.ScopeHTTPResources = []int{httpsPort}, true
				primary.AdditionalHTTPPorts = []cloud.RouteSpec{secondary}
				want.Routes = append(slices.Clone(spec.Routes), cloud.RouteGraphNode{Disposition: cloud.GraphEnsure, Spec: primary})
				filteredState := func(result cloud.GatewayGraphResult) cloud.RouteResult {
					t.Helper()
					for _, route := range result.Routes {
						if route.Identity == primary.Identity {
							return route.Result
						}
					}
					t.Fatal("missing result for the two-port filtered route")
					return cloud.RouteResult{}
				}
				ready := func() {
					t.Helper()
					state := filteredState(converge(newProvider(), want))
					if state.Outcome.State != cloud.OutcomeReady || len(state.State.L7PolicyIDs) != 2 {
						t.Fatalf("both filtered policies must be ready: %+v", state)
					}
					for _, id := range []string{httpID, tlsID} {
						if !slices.ContainsFunc(orderedPolicies(id), func(row record) bool {
							return row["request_cors"] != nil && row["request_url_rewrite"] != nil && row["admin_state_up"] == true
						}) || selectedPool(id, "route.example.test", requestPath) == "" {
							t.Fatalf("listener %s has no serving CORS/URL rewrite policy: %v", id, orderedPolicies(id))
						}
					}
				}
				expectTLSClosed := func() {
					t.Helper()
					before := len(writes)
					result, err := newProvider().ReconcileGatewayGraph(ctx, want)
					var incomplete *httpFilterInventoryError
					if omitPolicyRowsFor != "" && (!errors.As(err, &incomplete) || incomplete.filter != "CORS") ||
						omitPolicyRowsFor == "" && err != nil {
						t.Fatalf("unexpected error closing the affected listener: %v", err)
					}
					state := filteredState(result)
					if state.Outcome.State != cloud.OutcomeProgressing || state.ObservationDeferred ||
						server["admin_state_up"] != false || !slices.Equal(writes[before:], []string{"PUT " + tlsPath}) ||
						selectedPool(httpID, "route.example.test", requestPath) == "" {
						t.Fatalf("closure must preserve the active HTTP sibling: state %+v, writes %v", state, writes[before:])
					}
				}
				ready()
				omitPolicyRowsFor = tlsID
				expectTLSClosed()
				omitPolicyRowsFor = ""
				ready()
				omitURLRewriteFor = tlsID
				expectTLSClosed()
				omitURLRewriteFor = ""
				ready()
				want.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
				expectTLSClosed()
				want.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
				ready()
				want.Routes[2] = cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{Identity: primary.Identity}}
				converge(newProvider(), want)
				enableHTTPFilters = false
			})
		}

		if isolated {
			peer := inventory[listener+"/"+httpID]
			rows := orderedPolicies(tlsID)
			if len(rows) != 2 || rows[0]["https_listener_hostname"] != wildcardScope || rows[1]["https_listener_hostname"] != exactScope ||
				!slices.Contains(pilotTags(server["tags"]), httpsIsolationTag) {
				t.Fatalf("expected a single enrolled listener with two scoped policies, got %v", rows)
			}
			for _, row := range rows {
				rules := list(policy + "/" + row["id"].(string) + "/rules")
				if !slices.ContainsFunc(rules, func(rule record) bool { return rule["type"] == "HOST_NAME" }) {
					t.Fatal("native scope replaced the independent HTTP hostname predicate")
				}
			}
			assertClosed := func(provider *Provider) {
				t.Helper()
				before := len(writes)
				result := reconcile(provider, spec)
				if server["admin_state_up"] != false || peer["admin_state_up"] != true ||
					!slices.Equal(result.Gateway.ScopedListenerDeferredPorts, []int{frontendAddress.Port}) ||
					!slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
					t.Fatalf("uncertainty did not withdraw only the isolated port: %v, writes %v", result.Gateway, writes[before:])
				}
			}

			for _, drift := range []string{"", wildcardScope} {
				if drift == "" {
					delete(rows[1], "https_listener_hostname")
				} else {
					rows[1]["https_listener_hostname"] = drift
				}
				assertClosed(newProvider())
				before := len(writes)
				reconcile(newProvider(), spec)
				if rows[1]["https_listener_hostname"] != exactScope || rows[1]["admin_state_up"] != false ||
					server["admin_state_up"] != false || !slices.Equal(writes[before:], []string{"PUT " + policy + "/" + rows[1]["id"].(string)}) {
					t.Fatalf("unverified policy scope did not repair while closed: %v, writes %v", rows[1], writes[before:])
				}
				converge(newProvider(), spec)
			}
			// A failed Kubernetes route read supplies only the Gateway's TLS
			// snapshot. Membership in the host list cannot prove a route's section.
			rows[1]["https_listener_hostname"] = wildcardScope
			withdraw := cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{
				Disposition: cloud.GraphWithdrawTLS, Spec: cloud.GatewaySpec{
					Identity: spec.Identity, ListenerProtocol: cloud.ListenerProtocolHTTP, ListenerPort: frontendAddress.Port,
					ScopedListeners: spec.Gateway.Spec.ScopedListeners[:1],
				},
			}}
			before := len(writes)
			result = reconcile(newProvider(), withdraw)
			if server["admin_state_up"] != false || peer["admin_state_up"] != true ||
				!slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
				t.Fatalf("incomplete route snapshot preserved an unverified section: %v, writes %v", result.Outcome, writes[before:])
			}
			converge(newProvider(), spec)
			omitVirtualHosts = true
			assertClosed(newProvider())
			// Each observed retirement is restricted to this closed listener.
			omitVirtualHosts = false
			converge(newProvider(), spec)

			isolationAdvertised = false
			assertClosed(newProvider())
			before = len(writes)
			reconcile(newProvider(), spec)
			if len(writes) != before {
				t.Fatalf("discovery failure allowed an isolated write: %v", writes[before:])
			}
			isolationAdvertised = true
			converge(newProvider(), spec)

			const replacementScope = "*.replacement.test"
			spec.Gateway.Spec.ScopedListeners[0].TLS.VirtualHosts = []string{replacementScope, exactScope}
			spec.Routes[1].Spec.HTTPBackends[0].Matches[1].HTTPSListenerHostname = replacementScope
			assertClosed(newProvider())
			converge(newProvider(), spec)
			if len(orderedPolicies(tlsID)) != 2 || !slices.Equal(server["https_virtual_hosts"].([]any), []any{replacementScope, exactScope}) ||
				!slices.Contains(pilotTags(server["tags"]), httpsIsolationTag) || peer["admin_state_up"] != true {
				t.Fatalf("host-set replacement lost enrollment or changed a sibling: %v", server)
			}

			optedOut := newProvider()
			optedOut.httpsListenerIsolation = false
			spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
			assertClosed(optedOut)
			spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
			spec.Routes[1].Disposition = cloud.GraphPreserve
			before = len(writes)
			result = reconcile(optedOut, spec)
			if server["admin_state_up"] != false || !slices.Contains(pilotTags(server["tags"]), httpsIsolationTag) ||
				!slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{frontendAddress.Port}) || len(writes) != before {
				t.Fatalf("opt-out reinterpreted the enclosed listener as ordinary HTTPS: %v, writes %v", result.Gateway, writes[before:])
			}
			spec.Gateway.Spec.ScopedListeners = spec.Gateway.Spec.ScopedListeners[1:]
			spec.Routes[1].Disposition = cloud.GraphDelete
			for range 24 {
				reconcile(newProvider(), spec)
				if inventory[tlsPath] == nil {
					break
				}
			}
			if inventory[tlsPath] != nil || peer["admin_state_up"] != true {
				t.Fatalf("explicit port removal retained the isolated frontend or changed the HTTP sibling: %v", writes[before:])
			}
			return
		}

		// A retained route's temporarily incomplete inventory must not block
		// an independent certificate withdrawal.
		before := len(writes)
		beforePoolReads := poolListReads
		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
		disagreePoolMonitor = orderedPolicies(tlsID)[0]["redirect_pool_id"].(string)
		if result := reconcile(newProvider(), spec); result.Outcome.State != cloud.OutcomeProgressing || server["admin_state_up"] != false || poolListReads != beforePoolReads ||
			!slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatalf("HTTPS withdrawal was blocked: %v, writes %v", result.Outcome, writes[before:])
		} else if !slices.Equal(result.Gateway.ScopedListenerDeferredPorts, []int{frontendAddress.Port}) ||
			!slices.ContainsFunc(result.Routes, func(route cloud.RouteGraphResult) bool {
				return route.Identity == spec.Routes[0].Spec.Identity && route.Result.ObservationDeferred
			}) {
			t.Fatal("withdrawing one port replaced the sibling's last observation")
		}
		disagreePoolMonitor = ""
		before = len(writes)
		if result := reconcile(newProvider(), spec); !slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{frontendAddress.Port}) || len(writes) != before {
			t.Fatalf("restart changed the revoked frontend or lost the HTTP sibling: %v", result.Gateway.ScopedListenerReadyPorts)
		}

		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refB}
		loseCertificateUpdateReply = true
		_, err := newProvider().ReconcileGatewayGraph(ctx, spec)
		if !errors.Is(err, cloud.ErrTimeout) || server["default_tls_container_ref"] != refB || server["admin_state_up"] != false ||
			!slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatalf("ambiguous certificate update did not leave the frontend closed: error %v, writes %v", err, writes[before:])
		}
		server["provisioning_status"], inventory[gateway]["provisioning_status"] = "PENDING_UPDATE", "PENDING_UPDATE"
		if result := reconcile(newProvider(), spec); slices.Contains(result.Gateway.ScopedListenerReadyPorts, httpsPort) || len(writes) != before+1 {
			t.Fatal("pending certificate readback authorized an enable")
		}
		server["provisioning_status"], inventory[gateway]["provisioning_status"] = "ACTIVE", "ACTIVE"
		result = converge(newProvider(), spec)
		if !slices.Equal(writes[before:], []string{"PUT " + tlsPath, "PUT " + tlsPath}) || len(result.Gateway.ScopedListenerReadyPorts) != 2 {
			t.Fatalf("restart repeated the certificate update or omitted the enable: %v", writes[before:])
		}
		before = len(writes)
		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
		if result := reconcile(newProvider(), spec); server["default_tls_container_ref"] != refB || server["admin_state_up"] != false ||
			slices.Contains(result.Gateway.ScopedListenerReadyPorts, httpsPort) || inventory[listener+"/"+httpID]["admin_state_up"] != true ||
			!slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatalf("rotation changed a serving certificate before withdrawal: %v, writes %v", result.Gateway.ScopedListenerReadyPorts, writes[before:])
		}
		converge(newProvider(), spec)

		// The narrow withdrawal works without a complete HTTPRoute graph
		// and still rechecks authorization immediately before the write.
		withdraw := cloud.GatewayGraphSpec{Identity: spec.Identity, Gateway: cloud.GatewayGraphNode{
			Disposition: cloud.GraphWithdrawTLS, Spec: cloud.GatewaySpec{
				Identity: spec.Identity, ListenerProtocol: cloud.ListenerProtocolHTTP, ListenerPort: frontendAddress.Port,
				ScopedListeners: []cloud.ScopedListener{{Port: httpsPort, TLS: &cloud.ListenerTLS{CertificateRef: refA}}},
			},
		}}
		before = len(writes)
		if result := reconcile(newProvider(), withdraw); result.Outcome.State != cloud.OutcomeReady || len(writes) != before {
			t.Fatal("a currently approved certificate was withdrawn")
		}
		withdraw.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
		mutationDenied := errors.New("authorization changed before mutation")
		withdraw.ValidateMutation = func(context.Context) error { return mutationDenied }
		if _, err := newProvider().ReconcileGatewayGraph(ctx, withdraw); !errors.Is(err, mutationDenied) || len(writes) != before {
			t.Fatalf("withdrawal bypassed the mutation fence: %v, writes %v", err, writes[before:])
		}
		withdraw.ValidateMutation = nil
		if result := reconcile(newProvider(), withdraw); result.Outcome.State != cloud.OutcomeProgressing || server["admin_state_up"] != false ||
			inventory[listener+"/"+httpID]["admin_state_up"] != true || !slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatalf("restricted withdrawal changed unrelated resources: %v, writes %v", result.Outcome, writes[before:])
		}
		if result := reconcile(newProvider(), withdraw); result.Outcome.State != cloud.OutcomeReady || len(writes) != before+1 {
			t.Fatalf("restricted withdrawal did not settle after readback: %v, writes %v", result.Outcome, writes[before:])
		}
		converge(newProvider(), spec)
		withdraw.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{}
		before = len(writes)
		if result := reconcile(newProvider(), withdraw); result.Outcome.State != cloud.OutcomeProgressing || server["admin_state_up"] != false ||
			inventory[listener+"/"+httpID]["admin_state_up"] != true || !slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatal("withdrawing all owned certificates changed the plaintext sibling")
		}
		converge(newProvider(), spec)

		delete(server, "sni_container_refs")
		before = len(writes)
		reconcile(newProvider(), spec)
		if result := reconcile(newProvider(), spec); slices.Contains(result.Gateway.ScopedListenerReadyPorts, httpsPort) ||
			server["admin_state_up"] != false || !slices.Equal(writes[before:], []string{"PUT " + tlsPath}) {
			t.Fatal("missing SNI readback was treated as proof of an empty SNI list")
		}
		server["sni_container_refs"] = []string{}
		converge(newProvider(), spec)

		// A deliberate protocol change retires the previously owned frontend
		// and its policies before claiming that socket for the new protocol.
		spec.Gateway.Spec.ScopedListeners[0].TLS = nil
		converge(newProvider(), spec)
		current := func(port int) record {
			for _, candidate := range list(listener) {
				if candidate["protocol_port"] == float64(port) {
					return candidate
				}
			}
			return nil
		}
		plain := current(httpsPort)
		if plain["protocol"] != "HTTP" || plain["default_tls_container_ref"] != nil || inventory[tlsPath] != nil {
			t.Fatal("replacing HTTPS with an explicitly requested HTTP listener retained the wrong protocol")
		}
		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{}
		if result := reconcile(newProvider(), spec); slices.Contains(result.Gateway.ScopedListenerReadyPorts, httpsPort) ||
			plain["admin_state_up"] != false || inventory[listener+"/"+httpID]["admin_state_up"] != true {
			t.Fatal("an older plaintext listener remained open when the port required HTTPS")
		}
		spec.Gateway.Spec.ScopedListeners[0].TLS = &cloud.ListenerTLS{CertificateRef: refA}
		converge(newProvider(), spec)

		spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.GatewaySpec{
			ListenerProtocol: cloud.ListenerProtocolHTTP, ListenerPort: frontendAddress.Port, ScopedListeners: []cloud.ScopedListener{},
		}}
		for i := range spec.Routes {
			spec.Routes[i].Disposition = cloud.GraphPreserve
		}
		for attempts := 0; attempts < 24 && len(inventory) != len(protected); attempts++ {
			reconcile(newProvider(), spec)
			if inventory[gateway] != nil {
				inventory[gateway]["provisioning_status"] = "ACTIVE"
			}
		}
		if len(inventory) != len(protected) {
			t.Fatalf("HTTPS Gateway cleanup left provider resources: %v", writes)
		}
		return
	}
	if result := converge(provider, spec); len(result.Routes) != 1 || result.Routes[0].Result.Outcome.State != cloud.OutcomeReady {
		t.Fatalf("route did not converge: %#v, writes %v", result, writes)
	}
	if distinct {
		secondReservation, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = secondReservation.Close() }()
		secondAddress := secondReservation.Addr().(*net.TCPAddr)
		allowedHTTPPorts = append(allowedHTTPPorts, secondAddress.Port)
		const otherResponse = "hibiscus-other-port-backend"
		secondBackend, _ := startPilotBackend(t, otherResponse)
		second := spec.Routes[0]
		second.Spec.Identity.RouteName, second.Spec.Identity.RouteUID = "other-port", "other-port-uid"
		second.Spec.Members = []cloud.Member{{Address: secondBackend.IP.String(), Port: secondBackend.Port}}
		second.Spec.ScopedListenerPorts = []int{secondAddress.Port}
		spec.Gateway.Spec.ListenerProtocol = cloud.ListenerProtocolHTTP
		spec.Gateway.Spec.ScopedListeners = []cloud.ScopedListener{
			{Name: "first", Port: frontendAddress.Port}, {Name: "second", Port: secondAddress.Port},
		}
		firstState := routeStateFromInventoryForTest(t, orderedPolicies("listener"), inventory, pool, int(backendAddress.Port()))
		spec.Routes[0].Spec.ScopedListenerPorts = []int{frontendAddress.Port}
		spec.Routes = append(spec.Routes, second)
		converge(newProvider(), spec)
		if len(list(listener)) != 2 || inventory[listener+"/listener"]["protocol_port"] != float64(frontendAddress.Port) ||
			inventory[listener+"/listener-2"]["protocol_port"] != float64(secondAddress.Port) ||
			inventory[listener+"/listener"]["default_pool_id"] != nil || inventory[listener+"/listener-2"]["default_pool_id"] != nil {
			t.Fatalf("unexpected physical listeners: %v", list(listener))
		}
		if state := routeStateFromInventoryForTest(t, orderedPolicies("listener"), inventory, pool, int(backendAddress.Port())); state != firstState {
			t.Fatalf("adding the port changed the first route: %v -> %v", firstState, state)
		}
		secondState := routeStateFromInventoryForTest(t, orderedPolicies("listener-2"), inventory, pool, secondBackend.Port)
		stableWrites := len(writes)
		if outcome := reconcile(newProvider(), spec).Outcome; outcome.State != cloud.OutcomeReady || len(writes) != stableWrites {
			t.Fatalf("restart changed converged graph: %v, %v", outcome, writes[stableWrites:])
		}
		// While an established route still has policies on its old port,
		// neither port can certify the move, even if both listeners are active.
		move := spec
		move.Routes = slices.Clone(spec.Routes)
		move.Routes[0].Spec.ScopedListenerPorts = []int{secondAddress.Port}
		move.Routes[0].Spec.Precedence = 1
		inventory[gateway]["provisioning_status"] = "PENDING_UPDATE"
		if result := reconcile(newProvider(), move); !result.Gateway.HTTPPolicyTransition ||
			len(result.Gateway.ScopedListenerReadyPorts) != 0 || len(writes) != stableWrites {
			t.Fatalf("moving route exposed premature listener readiness: %#v", result)
		}
		inventory[gateway]["provisioning_status"] = "ACTIVE"
		probe := func(expectFirst bool) {
			t.Helper()
			if !renderAvailable {
				return
			}
			data, err := json.Marshal(inventory)
			if err != nil {
				t.Fatal(err)
			}
			stop := startPilotHAProxy(t, ctx, python, haproxy, data)
			defer stop()
			client := &http.Client{Timeout: time.Second}
			for _, test := range []struct{ address, response string }{
				{frontendAddress.String(), response}, {secondAddress.String(), otherResponse},
			} {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+test.address+"/api", nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Host = "api.example.com"
				got, err := client.Do(req)
				if !expectFirst && test.address == frontendAddress.String() {
					if !errors.Is(err, syscall.ECONNREFUSED) {
						t.Fatalf("removed physical port still accepts requests: %v, %v", got, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(got.Body)
				err = errors.Join(err, got.Body.Close())
				if err != nil || got.StatusCode != http.StatusOK || string(body) != test.response+" "+req.Host {
					t.Fatalf("port %s forwarded to %d %q, want %q; %v", test.address, got.StatusCode, body, test.response, err)
				}
				t.Logf("HTTP %s /api -> %q", test.address, body)
			}
		}
		if err := errors.Join(reservation.Close(), secondReservation.Close()); err != nil {
			t.Fatal(err)
		}
		probe(true)
		// Change a section's port to one absent from the customer security
		// group. The old policies must be withdrawn independently of the new
		// port, and the unrelated listener and pool must remain in service.
		replacementPort := 81
		for slices.Contains(allowedHTTPPorts, replacementPort) {
			replacementPort++
		}
		move.Gateway.Spec.ScopedListeners = slices.Clone(spec.Gateway.Spec.ScopedListeners)
		move.Gateway.Spec.ScopedListeners[0].Port = replacementPort
		move.Routes[0].Spec.ScopedListenerPorts = []int{replacementPort}
		beforeMove := len(writes)
		var moveErr error
		for range 8 {
			_, moveErr = newProvider().ReconcileGatewayGraph(ctx, move)
			if moveErr != nil {
				break
			}
		}
		if !errors.Is(moveErr, cloud.ErrDependencyNotReady) || len(orderedPolicies("listener")) != 0 || len(list(listener)) != 2 ||
			routeStateFromInventoryForTest(t, orderedPolicies("listener-2"), inventory, pool, secondBackend.Port) != secondState ||
			inventory[pool+"/"+firstState[1]] == nil || inventory[gateway]["admin_state_up"] != true {
			t.Fatalf("blocked port move did not independently withdraw its old policies: %v, writes %v", moveErr, writes[beforeMove:])
		}
		converge(newProvider(), spec)
		firstState = routeStateFromInventoryForTest(t, orderedPolicies("listener"), inventory, pool, int(backendAddress.Port()))
		stableWrites = len(writes)
		spec.Gateway.Spec.ScopedListeners = spec.Gateway.Spec.ScopedListeners[1:]
		spec.Routes[0].Disposition = cloud.GraphDelete
		spec.Routes[0].Spec = cloud.RouteSpec{Identity: route.Identity}
		disagreePoolListeners = firstState[1]
		if _, err := provider.ReconcileGatewayGraph(ctx, spec); err == nil || len(writes) != stableWrites {
			t.Fatalf("cleanup began before verifying the first port's pool association: %v, writes %v", err, writes[stableWrites:])
		}
		disagreePoolListeners = ""
		disagreePoolMonitor = firstState[1]
		if _, err := provider.ReconcileGatewayGraph(ctx, spec); err == nil || len(writes) != stableWrites {
			t.Fatalf("cleanup began while the pool omitted the listed monitor: %v, writes %v", err, writes[stableWrites:])
		}
		disagreePoolMonitor = ""
		inventory[gateway]["provisioning_status"] = "PENDING_UPDATE"
		inventory[listener+"/listener"]["provisioning_status"] = "PENDING_DELETE"
		if result := reconcile(newProvider(), spec); result.Outcome.State != cloud.OutcomeProgressing || len(writes) != stableWrites ||
			!slices.Equal(result.Gateway.ScopedListenerReadyPorts, []int{secondAddress.Port}) ||
			result.Routes[0].Identity != second.Spec.Identity || !result.Routes[0].Result.ObservationDeferred {
			t.Fatalf("pending first port lost proof for the serving sibling: %#v, writes %v", result, writes[stableWrites:])
		}
		inventory[gateway]["provisioning_status"] = "ACTIVE"
		inventory[listener+"/listener"]["provisioning_status"] = "ACTIVE"
		converge(newProvider(), spec)
		if len(list(listener)) != 1 || len(list(pool)) != 1 ||
			routeStateFromInventoryForTest(t, orderedPolicies("listener-2"), inventory, pool, secondBackend.Port) != secondState ||
			inventory[gateway]["admin_state_up"] != true {
			t.Fatalf("removing the legacy port disrupted the remaining route: writes %v", writes[stableWrites:])
		}
		probe(false)
		// Whole-Gateway deletion still closes forwarding before changing L7
		// predicates and observes the sibling exclusively through its port tag.
		spec.Gateway = cloud.GatewayGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.GatewaySpec{
			ListenerProtocol: cloud.ListenerProtocolHTTP, ListenerPort: frontendAddress.Port, ScopedListeners: []cloud.ScopedListener{},
		}}
		spec.Routes[1].Disposition = cloud.GraphPreserve
		beforeDelete := len(writes)
		if result := reconcile(newProvider(), spec); result.Outcome.State != cloud.OutcomeProgressing ||
			!slices.Equal(writes[beforeDelete:], []string{"PUT " + gateway}) || inventory[gateway]["admin_state_up"] != false {
			t.Fatalf("scoped Gateway teardown did not close forwarding first: %#v, writes %v", result, writes[beforeDelete:])
		}
		inventory[gateway]["provisioning_status"] = "ACTIVE"
		converge(newProvider(), spec)
		if len(inventory) != len(protected) {
			t.Fatalf("scoped Gateway teardown left resources: %v", inventory)
		}
		return
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

func routeStateFromInventoryForTest(t *testing.T, policies []map[string]any, inventory map[string]map[string]any, pool string, memberPort int) [2]string {
	t.Helper()
	if len(policies) != 1 || policies[0]["position"] != 1 || policies[0]["admin_state_up"] != true {
		t.Fatalf("expected exactly one enabled policy in position one, got %v", policies)
	}
	policy := policies[0]
	poolID := policy["redirect_pool_id"].(string)
	for resource, record := range inventory {
		if path.Dir(resource) == pool+"/"+poolID+"/members" && record["protocol_port"] == float64(memberPort) {
			return [2]string{policy["id"].(string), poolID}
		}
	}
	t.Fatalf("policy %v does not reach backend port %d", policy["id"], memberPort)
	return [2]string{}
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

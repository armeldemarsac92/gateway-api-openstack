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
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/backendtls"
)

type gatewaySourceFunc func(context.Context, Request) (GatewayInput, error)

func (f gatewaySourceFunc) ObserveGatewayGraph(ctx context.Context, request Request) (GatewayInput, error) {
	return f(ctx, request)
}

type routeSourceFunc func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error)

func (f routeSourceFunc) ObserveRouteGraph(ctx context.Context, request Request, gateway *gatewayv1.Gateway) (RouteInputs, error) {
	return f(ctx, request, gateway)
}

type graphProviderFunc func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error)

func (f graphProviderFunc) ReconcileGatewayGraph(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
	return f(ctx, spec)
}

func writerRequest() Request {
	return Request{Action: Ensure, Identity: cloud.Identity{
		OpenStackProjectID: "project-a", ClusterID: "cluster-a", Controller: "example.com/gateway-controller",
		GatewayNamespace: "default", GatewayName: "edge", GatewayUID: "gateway-uid",
	}}
}

func writerFixture() *Writer {
	return &Writer{
		Coordinator: &Coordinator{},
		Gateway: gatewaySourceFunc(func(_ context.Context, request Request) (GatewayInput, error) {
			return GatewayInput{
				Object: &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{UID: types.UID(request.Identity.GatewayUID)}},
				Node:   cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.GatewaySpec{Identity: request.Identity}},
			}, nil
		}),
		Routes: routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) { return RouteInputs{}, nil }),
		Provider: graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
			return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
		}),
	}
}

func TestWriterKeepsGuardedHTTPPortsWhenTLSWithdrawalDoesNotObserveThem(t *testing.T) {
	for _, test := range []struct {
		name  string
		ports []int
		want  []int
	}{
		{name: "no new observation", want: []int{8080, 8443}},
		{name: "new TLS withdrawal", ports: []int{80, 8080}, want: []int{8080}},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := writerFixture()
			gateway := GatewayInput{ObserveTLS: func(context.Context) (GatewayInput, error) {
				return GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS,
					Spec: cloud.GatewaySpec{ScopedListeners: []cloud.ScopedListener{}}},
					Validate: func(context.Context) error { return nil }}, nil
			}}
			w.Provider = graphProviderFunc(func(_ context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				if spec.Gateway.Disposition != cloud.GraphWithdrawTLS {
					t.Fatal("expected TLS-only withdrawal")
				}
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(),
					Gateway: cloud.GatewayResult{ScopedListenerDeferredPorts: test.ports}}, nil
			})
			initial := cloud.GatewayGraphResult{
				Gateway: cloud.GatewayResult{HTTPPolicyTransition: true, ScopedListenerDeferredPorts: []int{8080, 8443}},
			}
			result, err := w.finishTLS(t.Context(), writerRequest().Identity, gateway, initial, cloud.ErrDependencyNotReady)
			if !errors.Is(err, cloud.ErrDependencyNotReady) || !slices.Equal(result.Gateway.ScopedListenerDeferredPorts, test.want) {
				t.Fatalf("expected the inventory error and deferred ports %v, got %+v, %v", test.want, result.Gateway, err)
			}
		})
	}
}

func TestWriterRequiresCurrentRouteAuthorityAfterScopedTLSChange(t *testing.T) {
	request := writerRequest()
	makeInput := func(certificate string, hosts ...string) GatewayInput {
		return GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS, Spec: cloud.GatewaySpec{
			Identity: request.Identity, ListenerProtocol: cloud.ListenerProtocolHTTP, ScopedListeners: []cloud.ScopedListener{
				{Port: 443, TLS: &cloud.ListenerTLS{CertificateRef: certificate, VirtualHosts: hosts}},
			},
		}}, Validate: func(context.Context) error { return nil }}
	}
	original := makeInput("shared-revision", "gateway.example.test", "set.example.test")
	original.Node.Disposition = cloud.GraphEnsure
	original.Object = &gatewayv1.Gateway{}
	reduced := makeInput("shared-revision", "gateway.example.test")
	for _, test := range []struct {
		name      string
		after     GatewayInput
		withProof bool
		readErr   bool
		failFence bool
	}{
		{name: "one section revoked", after: reduced, withProof: true},
		{name: "route read failed after revocation", after: reduced, readErr: true},
		{name: "route validation failed on its second read", after: reduced, failFence: true},
		{name: "an interrupted revocation already restored", after: original},
		{name: "the server certificate has changed", after: makeInput("other-revision", "gateway.example.test")},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := writerFixture()
			live := reduced
			original.ObserveTLS = func(context.Context) (GatewayInput, error) { return live, nil }
			staleRoutes := errors.New("route snapshot changed")
			fence := validateInputs(t.Context(), original, RouteInputs{Validate: func(context.Context) error { return staleRoutes }})
			var changed *TLSAuthorizationChanged
			if !errors.Is(fence, staleRoutes) || !errors.As(fence, &changed) {
				t.Fatalf("the route validation must retain the observed TLS change, got %v", fence)
			}
			live = test.after
			live.Node.Disposition = cloud.GraphWithdrawTLS
			w.Gateway = gatewaySourceFunc(func(context.Context, Request) (GatewayInput, error) {
				ensure := live
				ensure.Node.Disposition = cloud.GraphEnsure
				ensure.Object = original.Object
				return ensure, nil
			})
			fences := 0
			w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
				if test.readErr {
					return RouteInputs{}, staleRoutes
				}
				return RouteInputs{Validate: func(context.Context) error {
					fences++
					if test.failFence && fences == 2 {
						return staleRoutes
					}
					return nil
				}}, nil
			})
			attempts := 0
			w.Provider = graphProviderFunc(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				attempts++
				if !slices.Equal(spec.Gateway.TLSWithdrawalPorts, []int{443}) ||
					(spec.HTTPSPolicyAuthority != nil) != test.withProof ||
					(slices.Equal(spec.Gateway.TLSWithdrawalPreviousHosts[443], []string{"gateway.example.test", "set.example.test"})) != test.withProof {
					t.Errorf("unexpected scoped withdrawal authority: %#v", spec)
				}
				var serving []int
				if spec.HTTPSPolicyAuthority != nil {
					serving = []int{443}
				}
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayResult{
					ScopedListenerServingPorts: serving,
				}}, spec.ValidateMutation(ctx)
			})
			initial := cloud.GatewayGraphResult{Gateway: cloud.GatewayResult{
				ScopedListenerReadyPorts: []int{443}, ScopedListenerDeferredPorts: []int{443}, ScopedListenerServingPorts: []int{443},
			}}
			result, err := w.finishTLS(t.Context(), request.Identity, original, initial, fence)
			if len(result.Gateway.ScopedListenerReadyPorts) != 0 || len(result.Gateway.ScopedListenerDeferredPorts) != 0 ||
				slices.Contains(result.Gateway.ScopedListenerServingPorts, 443) != test.withProof {
				t.Fatalf("TLS withdrawal retained stale discovery or logical readiness: %+v", result.Gateway)
			}
			if attempts != 1 {
				t.Fatalf("expected a single provider call, got %d", attempts)
			}
			if !errors.Is(err, staleRoutes) || !errors.As(err, &changed) {
				t.Fatalf("the withdrawal lost one cause of the failed fence: %v", err)
			}
		})
	}
}

func TestWriterUsesCompleteHTTPSBindingsWithoutChangingRouteCleanupScope(t *testing.T) {
	request := writerRequest()
	gatewayIdentity := request.Identity
	request.Action, request.Route = DeleteRoute, &gatewayv1.HTTPRoute{}
	request.Identity.RouteNamespace, request.Identity.RouteName, request.Identity.RouteUID = "default", "detached", "detached-uid"
	sibling := request.Identity
	sibling.RouteName, sibling.RouteUID = "sibling", "sibling-uid"
	gateway := &gatewayv1.Gateway{}
	detach := cloud.RouteGraphNode{Disposition: cloud.GraphDelete, Spec: cloud.RouteSpec{Identity: request.Identity}}
	preserve := cloud.RouteGraphNode{Disposition: cloud.GraphPreserve, Spec: cloud.RouteSpec{Identity: sibling}}
	ensure := preserve
	ensure.Disposition = cloud.GraphEnsure

	w := writerFixture()
	validate := func(context.Context) error { return nil }
	w.Gateway = gatewaySourceFunc(func(_ context.Context, req Request) (GatewayInput, error) {
		spec := cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolHTTP, ScopedListeners: []cloud.ScopedListener{}}
		disposition := cloud.GraphPreserve
		if req.Action == Ensure {
			if req.Identity != gatewayIdentity || req.Gateway != gateway || req.Route != nil {
				t.Fatalf("cleanup used the wrong Gateway authority: %#v", req)
			}
			disposition = cloud.GraphEnsure
			spec.ScopedListeners = []cloud.ScopedListener{{Port: 443, TLS: &cloud.ListenerTLS{
				CertificateRef: "shared-revision", VirtualHosts: []string{"gateway.example.test", "set.example.test"},
			}}}
		}
		return GatewayInput{Object: gateway, Node: cloud.GatewayGraphNode{Disposition: disposition, Spec: spec}, Validate: validate}, nil
	})
	w.Routes = routeSourceFunc(func(_ context.Context, req Request, _ *gatewayv1.Gateway) (RouteInputs, error) {
		if req.Action == Ensure {
			return RouteInputs{Nodes: []cloud.RouteGraphNode{detach, ensure}, Validate: validate}, nil
		}
		return RouteInputs{Nodes: []cloud.RouteGraphNode{detach, preserve}, Validate: validate}, nil
	})
	w.Provider = graphProviderFunc(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		if spec.Gateway.Disposition != cloud.GraphPreserve || len(spec.Routes) != 2 || spec.Routes[1].Disposition != cloud.GraphPreserve ||
			spec.HTTPSPolicyAuthority == nil || spec.HTTPSPolicyAuthority.Routes[1].Disposition != cloud.GraphEnsure {
			t.Fatalf("cleanup changed its mutation scope or lost the surviving authority: %#v", spec)
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, spec.ValidateMutation(ctx)
	})
	if _, err := w.ReconcileGraph(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func TestWriterRebuildsHTTPSBindingsAfterStaleRouteObservation(t *testing.T) {
	request := writerRequest()
	gateway := &gatewayv1.Gateway{}
	listeners := []cloud.ScopedListener{{Port: 443, TLS: &cloud.ListenerTLS{
		CertificateRef: "shared-revision", VirtualHosts: []string{"sibling.example.test"},
	}}}
	stale := errors.New("route projection changed")
	validate := func(context.Context) error { return nil }
	tls := GatewayInput{Node: cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS,
		Spec: cloud.GatewaySpec{ListenerProtocol: cloud.ListenerProtocolHTTP, ScopedListeners: listeners}}, Validate: validate}
	input := GatewayInput{Object: gateway, ObserveTLS: func(context.Context) (GatewayInput, error) { return tls, nil }}
	for _, test := range []struct {
		name      string
		routesErr error
	}{
		{name: "complete refresh"},
		{name: "incomplete refresh", routesErr: stale},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := writerFixture()
			w.Gateway = gatewaySourceFunc(func(_ context.Context, req Request) (GatewayInput, error) {
				if req.Identity != request.Identity || req.Gateway != gateway || req.Route != nil {
					t.Fatalf("invalid authority refresh: %#v", req)
				}
				live := tls
				live.Node.Disposition = cloud.GraphEnsure
				live.Object = gateway
				return live, nil
			})
			w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
				return RouteInputs{Validate: validate}, test.routesErr
			})
			w.Provider = graphProviderFunc(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				if spec.Gateway.Disposition != cloud.GraphWithdrawTLS || len(spec.Routes) != 0 ||
					(spec.HTTPSPolicyAuthority != nil) != (test.routesErr == nil) {
					t.Fatalf("unexpected TLS withdrawal authority: %#v", spec)
				}
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, spec.ValidateMutation(ctx)
			})
			if _, err := w.finishTLS(t.Context(), request.Identity, input, cloud.GatewayGraphResult{}, stale); !errors.Is(err, stale) {
				t.Fatalf("authorization refresh lost the triggering error: %v", err)
			}
		})
	}
}

func TestBackendClientCertificateWithdrawalSurvivesFailedPolicyInventory(t *testing.T) {
	for _, policyErr := range []error{errors.New("could not list BackendTLSPolicies"), backendtls.ErrClusterTrustBundleUnavailable} {
		t.Run(policyErr.Error(), func(t *testing.T) {
			w := writerFixture()
			request := writerRequest()
			request.Gateway = backendInputGateway()
			var actions []Action
			checks := 0
			w.Gateway = gatewaySourceFunc(func(_ context.Context, req Request) (GatewayInput, error) {
				actions = append(actions, req.Action)
				node := cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, BackendCAOwned: true,
					Spec: cloud.GatewaySpec{Identity: req.Identity}}
				// A singleton HTTP Gateway uses the old empty protocol binding.
				if req.Action == WithdrawBackendClientCertificate {
					if req.Gateway != request.Gateway {
						t.Fatal("missing expected Gateway on withdrawal")
					}
					node.Disposition = cloud.GraphWithdrawBackendClientCertificate
					node.BackendClientCertificate.Denied = true
				}
				return GatewayInput{Node: node, Validate: func(context.Context) error {
					checks++
					return nil
				}}, nil
			})
			w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
				return RouteInputs{}, policyErr
			})
			submitted := false
			w.Provider = graphProviderFunc(func(ctx context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				if spec.Gateway.Disposition != cloud.GraphWithdrawBackendClientCertificate || !spec.Gateway.BackendClientCertificate.Denied || len(spec.Routes) != 0 {
					t.Fatalf("unexpected fallback authority: %+v", spec)
				}
				if err := spec.ValidateMutation(ctx); err != nil {
					t.Fatal(err)
				}
				submitted = true
				return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
			})
			if _, err := w.ReconcileGraph(t.Context(), request); !errors.Is(err, policyErr) || !submitted || checks != 1 ||
				!slices.Equal(actions, []Action{Ensure, WithdrawBackendClientCertificate}) {
				t.Fatalf("fallback did not withdraw under a fresh fence: actions %v, submitted %t, checks %d, error %v", actions, submitted, checks, err)
			}
		})
	}
}

func TestClusterTrustBundleAPIErrorPreservesProgrammedGraph(t *testing.T) {
	w := writerFixture()
	observe := w.Gateway
	w.Gateway = gatewaySourceFunc(func(ctx context.Context, req Request) (GatewayInput, error) {
		if req.Action != Ensure {
			t.Fatalf("unexpected fallback after an unconfirmed trust read: %s", req.Action)
		}
		input, err := observe.ObserveGatewayGraph(ctx, req)
		input.ObserveTLS = func(context.Context) (GatewayInput, error) {
			t.Fatal("unconfirmed trust triggered fallback TLS withdrawal")
			return GatewayInput{}, nil
		}
		return input, err
	})
	w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
		return RouteInputs{}, backendtls.ErrClusterTrustBundleUnavailable
	})
	w.Provider = graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		t.Fatal("unconfirmed trust changed the programmed graph")
		return cloud.GatewayGraphResult{}, nil
	})
	if _, err := w.ReconcileGraph(t.Context(), writerRequest()); !errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable) {
		t.Fatalf("expected a retryable trust bundle error, got %v", err)
	}
}

func TestClusterTrustBundleBackendReadFailureAllowsConfirmedHTTPSWithdrawal(t *testing.T) {
	w := writerFixture()
	w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
		return RouteInputs{}, backendtls.ErrClusterTrustBundleUnavailable
	})
	w.Gateway = gatewaySourceFunc(func(_ context.Context, req Request) (GatewayInput, error) {
		if req.Action != Ensure {
			t.Fatalf("unexpected fallback action %s", req.Action)
		}
		spec := cloud.GatewaySpec{
			Identity: req.Identity, ListenerProtocol: cloud.ListenerProtocolHTTP,
			ScopedListeners: []cloud.ScopedListener{
				{Port: 443, TLS: &cloud.ListenerTLS{}},
				{Port: 8443, TLS: &cloud.ListenerTLS{CertificateRef: "healthy", VirtualHosts: []string{"healthy.example.test"}}},
			},
		}
		return GatewayInput{
			Node: cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: spec},
			ObserveTLS: func(context.Context) (GatewayInput, error) {
				return GatewayInput{
					Node:     cloud.GatewayGraphNode{Disposition: cloud.GraphWithdrawTLS, Spec: spec},
					Validate: func(context.Context) error { return nil },
				}, nil
			},
		}, nil
	})
	w.Provider = graphProviderFunc(func(_ context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		if spec.Gateway.Disposition != cloud.GraphWithdrawTLS || !slices.Equal(spec.Gateway.TLSWithdrawalPorts, []int{443}) ||
			len(spec.Routes) != 0 || spec.HTTPSPolicyAuthority != nil {
			t.Fatalf("confirmed withdrawal must affect only port 443: %+v", spec.Gateway)
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome(), Gateway: cloud.GatewayResult{ScopedTLSClosedPorts: []int{443}}}, nil
	})
	result, err := w.ReconcileGraph(t.Context(), writerRequest())
	var change *TLSAuthorizationChanged
	if !errors.Is(err, backendtls.ErrClusterTrustBundleUnavailable) || !errors.As(err, &change) ||
		!slices.Equal(result.Gateway.ScopedTLSClosedPorts, []int{443}) {
		t.Fatalf("want a confirmed frontend close and backend retry, got %+v and %v", result, err)
	}
}

func TestWriterObservesFreshInputsAfterAcquiringGateway(t *testing.T) {
	w := writerFixture()
	request := writerRequest()
	var generation atomic.Int64
	generation.Store(1)
	entered, continueFirst := make(chan struct{}), make(chan struct{})
	ports := make(chan int, 2)
	w.Gateway = gatewaySourceFunc(func(context.Context, Request) (GatewayInput, error) {
		current := generation.Load()
		return GatewayInput{
			Object: &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Generation: current}},
			Node:   cloud.GatewayGraphNode{Disposition: cloud.GraphEnsure, Spec: cloud.GatewaySpec{ListenerPort: int(80 + current)}},
		}, nil
	})
	w.Routes = routeSourceFunc(func(_ context.Context, _ Request, gateway *gatewayv1.Gateway) (RouteInputs, error) {
		if gateway.Generation != generation.Load() {
			t.Error("route source received a stale Gateway observation")
		}
		return RouteInputs{}, nil
	})
	w.Provider = graphProviderFunc(func(_ context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		ports <- spec.Gateway.Spec.ListenerPort
		if spec.Gateway.Spec.ListenerPort == 81 {
			close(entered)
			<-continueFirst
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := w.ReconcileGraph(context.Background(), request); first <- err }()
	<-entered
	go func() { _, err := w.ReconcileGraph(context.Background(), request); second <- err }()
	waitForGraphReferences(t, w.Coordinator, types.UID(request.Identity.GatewayUID), 2)
	generation.Store(2)
	close(continueFirst)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if firstPort, secondPort := <-ports, <-ports; firstPort != 81 || secondPort != 82 {
		t.Fatalf("provider listener ports = %d, %d, want observations before and after lock wait", firstPort, secondPort)
	}
	waitForGraphReferences(t, w.Coordinator, types.UID(request.Identity.GatewayUID), 0)
}

func TestWriterDifferentGatewayDoesNotWait(t *testing.T) {
	w := writerFixture()
	request := writerRequest()
	release, err := Acquire(context.Background(), w.Coordinator, request.Identity.GatewayUID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	request.Identity.GatewayUID = "other-gateway-uid"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := w.ReconcileGraph(ctx, request); err != nil {
		t.Fatalf("independent Gateway was blocked: %v", err)
	}
}

func TestWriterCanceledWaitDoesNotObserveOrCallProvider(t *testing.T) {
	w := writerFixture()
	request := writerRequest()
	release, err := Acquire(context.Background(), w.Coordinator, request.Identity.GatewayUID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var reads, providerCalls atomic.Int32
	w.Gateway = gatewaySourceFunc(func(context.Context, Request) (GatewayInput, error) { reads.Add(1); return GatewayInput{}, nil })
	w.Provider = graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		providerCalls.Add(1)
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, 1)
	go func() { _, err := w.ReconcileGraph(ctx, request); completed <- err }()
	waitForGraphReferences(t, w.Coordinator, types.UID(request.Identity.GatewayUID), 2)
	cancel()
	if err := <-completed; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if reads.Load() != 0 || providerCalls.Load() != 0 {
		t.Fatal("canceled lock waiter performed graph work")
	}
}

func TestWriterFailedSourceOrFencePreventsProviderCall(t *testing.T) {
	for _, stage := range []string{"gateway source", "HTTP source", "TCP source", "HTTP fence", "TCP fence", "gateway fence"} {
		t.Run(stage, func(t *testing.T) {
			w := writerFixture()
			failure := errors.New("input changed")
			var calls []string
			failAt := func(current string) error {
				calls = append(calls, current)
				if current == stage {
					return failure
				}
				return nil
			}
			w.Gateway = gatewaySourceFunc(func(context.Context, Request) (GatewayInput, error) {
				return GatewayInput{Validate: func(context.Context) error { return failAt("gateway fence") }}, failAt("gateway source")
			})
			source := func(kind string) RouteSource {
				return routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
					return RouteInputs{Validate: func(context.Context) error { return failAt(kind + " fence") }}, failAt(kind + " source")
				})
			}
			w.Routes = RouteSources{source("HTTP"), source("TCP")}
			w.Provider = graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
				t.Error("failed input allowed provider call")
				return cloud.GatewayGraphResult{}, nil
			})
			if _, err := w.ReconcileGraph(context.Background(), writerRequest()); !errors.Is(err, failure) {
				t.Fatalf("error = %v, want source failure", err)
			}
			order := []string{"gateway source", "HTTP source", "TCP source", "HTTP fence", "TCP fence", "gateway fence"}
			want := order[:slices.Index(order, stage)+1]
			if !slices.Equal(calls, want) {
				t.Fatalf("calls = %v, want prerequisite order %v", calls, want)
			}
			waitForGraphReferences(t, w.Coordinator, types.UID(writerRequest().Identity.GatewayUID), 0)
		})
	}
}

func TestWriterReconstructsCompleteGraphAfterRestart(t *testing.T) {
	request := writerRequest()
	request.Identity.RouteNamespace, request.Identity.RouteName, request.Identity.RouteUID = "default", "route", "route-uid"
	var reads, providerCalls, mutations int
	var observed cloud.GatewayGraphSpec
	provider := graphProviderFunc(func(_ context.Context, spec cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		providerCalls++
		if !reflect.DeepEqual(spec, observed) {
			mutations++
			observed = spec
		}
		return cloud.GatewayGraphResult{Outcome: cloud.ReadyOutcome()}, nil
	})
	for range 2 {
		w := writerFixture()
		w.Provider = provider
		w.Routes = routeSourceFunc(func(context.Context, Request, *gatewayv1.Gateway) (RouteInputs, error) {
			reads++
			return RouteInputs{Nodes: []cloud.RouteGraphNode{
				{Disposition: cloud.GraphEnsure, Spec: cloud.RouteSpec{Identity: request.Identity}},
			}}, nil
		})
		if _, err := w.ReconcileGraph(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if reads != 2 || providerCalls != 2 || mutations != 1 {
		t.Fatalf("reads = %d, provider calls = %d, mutations = %d", reads, providerCalls, mutations)
	}
	if observed.Identity.RouteUID != "" || len(observed.Routes) != 1 || observed.Routes[0].Spec.Identity != request.Identity {
		t.Fatalf("writer confused whole-graph and route identity: %#v", observed)
	}
}

func TestWriterPreservesComponentResults(t *testing.T) {
	w := writerFixture()
	want := cloud.GatewayGraphResult{
		Outcome: cloud.ProgressingOutcome("members changing", time.Second),
		Gateway: cloud.GatewayReadyResult(cloud.GatewayState{LoadBalancerID: "lb-1"}),
		Routes:  []cloud.RouteGraphResult{{Identity: writerRequest().Identity, Result: cloud.RouteProgressingResult("waiting", time.Second)}},
	}
	w.Provider = graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) { return want, nil })
	got, err := w.ReconcileGraph(context.Background(), writerRequest())
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("result = %#v, error = %v, want independent component outcomes %#v", got, err, want)
	}
	w.Provider = graphProviderFunc(func(context.Context, cloud.GatewayGraphSpec) (cloud.GatewayGraphResult, error) {
		return cloud.GatewayGraphResult{}, nil
	})
	if _, err := w.ReconcileGraph(context.Background(), writerRequest()); err == nil {
		t.Fatal("unknown provider outcome was accepted")
	}
}

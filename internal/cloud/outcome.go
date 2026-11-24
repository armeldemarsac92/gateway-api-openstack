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

package cloud

import (
	"fmt"
	"time"
)

// OutcomeState describes whether an ensure operation has converged or still
// needs observation. Failures are returned separately as errors.
type OutcomeState string

const (
	// OutcomeReady means the provider observed the requested state.
	OutcomeReady OutcomeState = "Ready"
	// OutcomeProgressing means an asynchronous provider operation is not ready
	// for another mutation and should be observed again later.
	OutcomeProgressing OutcomeState = "Progressing"
)

// Outcome describes the progress of a provider ensure operation.
type Outcome struct {
	State        OutcomeState
	Message      string
	RequeueAfter time.Duration
	// MutationSubmitted requests one prompt observation after a successful
	// write. A subsequent read that finds a busy provider uses normal polling.
	MutationSubmitted bool
}

// Validate rejects an outcome that does not describe a supported provider
// state. The controller, rather than the provider, bounds RequeueAfter.
func (o Outcome) Validate() error {
	switch o.State {
	case OutcomeReady:
		if o.MutationSubmitted {
			return fmt.Errorf("ready provider outcome cannot request post-mutation observation")
		}
		if o.RequeueAfter != 0 {
			return fmt.Errorf("ready provider outcome has a requeue interval")
		}
		return nil
	case OutcomeProgressing:
		return nil
	default:
		return fmt.Errorf("provider returned unknown outcome state %q", o.State)
	}
}

// ReadyOutcome returns an outcome for converged provider state.
func ReadyOutcome() Outcome {
	return Outcome{State: OutcomeReady}
}

// ProgressingOutcome returns an outcome that asks the controller to observe
// the provider again after requeueAfter.
func ProgressingOutcome(message string, requeueAfter time.Duration) Outcome {
	return Outcome{
		State:        OutcomeProgressing,
		Message:      message,
		RequeueAfter: requeueAfter,
	}
}

// GatewayResult is the provider-neutral result of ensuring a Gateway graph.
type GatewayResult struct {
	State   GatewayState
	Outcome Outcome
	// AddressIssue explains why the requested fixed address cannot be assigned.
	// The observed Gateway and route outcomes still describe the existing infrastructure.
	AddressIssue string
	// AddressBlocked indicates that the rejected address leaves no provider work
	// to retry at the provisioning cadence.
	AddressBlocked bool
	// AddressActive reports that the observed address still has an enabled
	// frontend, including when replacement was disallowed.
	AddressActive bool
	// AddressActivityObserved distinguishes confirmed inactivity from an
	// incomplete read of the load balancer, its listeners, or its Floating IP.
	AddressActivityObserved bool
	// AddressAbsent is a complete observation that the client-facing address
	// is unassigned. An incomplete observation leaves this false.
	AddressAbsent bool
	// HTTPPolicyTransition indicates an outdated rule or a policy awaiting withdrawal.
	HTTPPolicyTransition bool
	// ScopedListenerReadyPorts contains the individually programmed listener ports.
	ScopedListenerReadyPorts []int
	// ScopedListenerServingPorts contains verified physical ports on which
	// only some HTTPS sections may be serving. This evidence is for discovery.
	ScopedListenerServingPorts []int
	// ScopedTLSClosedPorts contains denied HTTPS ports with settled absence
	// or a verified disabled listener. Accepting a withdrawal write is insufficient.
	ScopedTLSClosedPorts []int
	// ScopedListenerDeferredPorts still match their active frontends while
	// route observations are deferred. Only previously programmed sections
	// at the same generation can retain status. A nil list means the provider
	// made no such observation. An empty list means no ports retain status.
	ScopedListenerDeferredPorts []int
}

// GatewayReadyResult returns a converged Gateway result.
func GatewayReadyResult(state GatewayState) GatewayResult {
	return GatewayResult{State: state, Outcome: ReadyOutcome()}
}

// GatewayProgressingResult returns a Gateway result that needs another
// observation.
func GatewayProgressingResult(message string, requeueAfter time.Duration) GatewayResult {
	return GatewayResult{Outcome: ProgressingOutcome(message, requeueAfter)}
}

// RouteResult is the provider-neutral result of ensuring an HTTPRoute graph.
type RouteResult struct {
	State   RouteState
	Outcome Outcome
	// ObservationDeferred means the parent is busy and the provider could not
	// establish a new outcome for this route on this pass. For scoped UDP, it
	// also confirms the winning listeners still reach the route's active pool
	// while a sibling listener is pending.
	ObservationDeferred bool
	// UnsupportedBackendProtocol is populated when the provider explicitly lacks a requested transport.
	UnsupportedBackendProtocol string
	// UnsupportedMatch reports an incompatible match without discarding other programmed rules.
	UnsupportedMatch string
}

// RouteReadyResult returns a converged HTTPRoute result.
func RouteReadyResult(state RouteState) RouteResult {
	return RouteResult{State: state, Outcome: ReadyOutcome()}
}

// RouteProgressingResult returns an HTTPRoute result that needs another
// observation.
func RouteProgressingResult(message string, requeueAfter time.Duration) RouteResult {
	return RouteResult{Outcome: ProgressingOutcome(message, requeueAfter)}
}

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
	"slices"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud/openstack/backendca"
)

// retireUnusedBackendCA runs with the Gateway writer lock and a converged Octavia graph.
// The selected CA can exist before the first pool is attached. Keep that revision as
// well as every PEM still used by a pool or client-authenticating listener.
func (p *Provider) retireUnusedBackendCA(ctx context.Context, spec cloud.GatewayGraphSpec, actual observedGatewayGraph, result cloud.GatewayGraphResult) (cloud.GatewayGraphResult, error) {
	deleting := spec.Gateway.Disposition == cloud.GraphDelete
	if !spec.Gateway.BackendCAOwned || result.Outcome.State != cloud.OutcomeReady ||
		(!deleting && spec.Gateway.Disposition != cloud.GraphEnsure) {
		return result, nil
	}
	if !deleting && slices.ContainsFunc(spec.Routes, func(node cloud.RouteGraphNode) bool {
		return node.Disposition == cloud.GraphPreserve
	}) {
		return result, nil
	}
	if p.backendCA == nil {
		return result, errors.New("backend CA retirement requires the regional Barbican client")
	}

	revisions, err := p.backendCA.List(ctx, spec.Identity.GatewayUID)
	if err != nil {
		return result, err
	}
	keep := retainedBackendCAs(spec, actual, revisions)
	pendingUse := false
	for _, revision := range revisions {
		if keep[revision.Reference()] {
			continue
		}
		var inUse, removing bool
		var authorityErr error
		err := p.backendCA.Retire(ctx, spec.Identity.GatewayUID, revision, func(ctx context.Context, ref string) (bool, error) {
			unused, err := p.octavia.CAUnused(ctx, p.projectID, ref)
			if err != nil {
				return false, err
			}
			if !unused {
				inUse = true
				return false, nil
			}
			if spec.ValidateMutation != nil {
				authorityErr = spec.ValidateMutation(ctx)
				if authorityErr != nil {
					return false, authorityErr
				}
			}
			removing = true
			return true, nil
		})
		if authorityErr != nil {
			return cloud.GatewayGraphResult{}, authorityErr
		}
		if err != nil {
			return result, err
		}
		pendingUse = pendingUse || inUse
		if removing {
			result.Outcome = p.progressingOutcome("Waiting to observe retirement of the unused backend CA")
			if deleting {
				result.Gateway.Outcome = result.Outcome
			}
			return result, nil
		}
	}
	if deleting && pendingUse {
		result.Outcome = p.progressingOutcome("An Octavia resource still uses a CA owned by this Gateway")
		result.Gateway.Outcome = result.Outcome
	}
	return result, nil
}

func retainedBackendCAs(spec cloud.GatewayGraphSpec, actual observedGatewayGraph, revisions []backendca.Revision) map[string]bool {
	if len(revisions) == 0 || spec.Gateway.Disposition != cloud.GraphEnsure {
		return nil
	}
	keep := make(map[string]bool)
	attached := make(map[string]bool)
	for _, route := range actual.routes {
		if route.pool != nil && route.pool.CATLSContainerRef != "" {
			attached[route.pool.CATLSContainerRef] = true
		}
		for _, backend := range route.httpParts {
			if backend.pool != nil && backend.pool.CATLSContainerRef != "" {
				attached[backend.pool.CATLSContainerRef] = true
			}
		}
	}
	selectRevision := func(tls *cloud.BackendTLSValidation) {
		if tls == nil || tls.Denied {
			return
		}
		revision := backendca.SelectConfigMapRevision(revisions, tls.CAConfigMapUID, tls.CADigest, func(ref string) bool { return attached[ref] })
		if ref := revision.Reference(); ref != "" {
			keep[ref] = true
		}
	}
	for _, node := range spec.Routes {
		if node.Disposition == cloud.GraphEnsure {
			for spec := range node.Spec.HTTPPortSpecs() {
				selectRevision(spec.BackendTLS)
				for _, backend := range spec.HTTPBackends {
					selectRevision(backend.BackendTLS)
				}
			}
		}
	}
	return keep
}

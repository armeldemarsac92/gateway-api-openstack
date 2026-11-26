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

package l4route

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/graph"
	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

type serviceImportGrantRevision struct {
	grants []objectRevision
	failed bool
}

type serviceImportRevision struct {
	model.ServiceImportObservation
	withMembers bool
}

func readServiceImport(ctx context.Context, reader client.Reader, key types.NamespacedName, withMembers bool) (serviceImportRevision, error) {
	read := graph.ObserveServiceImportReference
	if withMembers {
		read = graph.ObserveServiceImport
	}
	observation, err := read(ctx, reader, key)
	return serviceImportRevision{ServiceImportObservation: observation, withMembers: withMembers}, err
}

// serviceImportInputs fences status writes, including zero-weight references.
type serviceImportInputs struct {
	grants  map[string]serviceImportGrantRevision
	imports []serviceImportRevision
}

func (s serviceImportInputs) validate(ctx context.Context, r *Reconciler) error {
	for namespace, expected := range s.grants {
		list, err := r.backendGrants(ctx, namespace)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !reflect.DeepEqual(expected, serviceImportGrantRevision{grants: grantRevisions(list), failed: err != nil}) {
			return controller.ErrGraphInputChanged
		}
	}
	for _, expected := range s.imports {
		current, err := readServiceImport(ctx, r.APIReader, expected.Key, expected.withMembers)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(expected, current) {
			return controller.ErrGraphInputChanged
		}
	}
	return nil
}

func (r *Reconciler) backendGrants(ctx context.Context, namespace string) ([]gatewayv1.ReferenceGrant, error) {
	var list gatewayv1.ReferenceGrantList
	if err := r.APIReader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	if list.Continue != "" {
		return nil, fmt.Errorf("incomplete %s ReferenceGrant list", r.kind())
	}
	slices.SortFunc(list.Items, func(left, right gatewayv1.ReferenceGrant) int { return cmp.Compare(left.Name, right.Name) })
	return list.Items, nil
}

func grantRevisions(grants []gatewayv1.ReferenceGrant) []objectRevision {
	var revisions []objectRevision
	for i := range grants {
		revisions = append(revisions, revisionOf(&grants[i]))
	}
	return revisions
}

func serviceImportModeIssue(mode cloud.MemberMode) *model.Diagnostic {
	if mode != "" && mode != cloud.MemberModeEndpoint {
		return backendRejection(gatewayv1.RouteReasonUnsupportedValue, "backend ServiceImport requires IPv4 Endpoint member mode")
	}
	return nil
}

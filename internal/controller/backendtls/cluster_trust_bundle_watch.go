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

package backendtls

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const clusterTrustBundleResource = "clustertrustbundles"

// ClusterTrustBundles observes the optional native API independently of the
// manager's required caches. Only the live object reader authorizes mutations.
type ClusterTrustBundles struct {
	discovery *discovery.DiscoveryClient
	metadata  metadata.Interface
	objects   dynamic.Interface

	mu           sync.RWMutex
	resource     schema.GroupVersionResource
	discoveryErr error

	// Setup registers subscribers before the manager starts the runnable.
	subscribers []chan event.GenericEvent
}

func NewClusterTrustBundles(config *rest.Config, httpClient *http.Client) (*ClusterTrustBundles, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	metadataClient, err := metadata.NewForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	objectClient, err := dynamic.NewForConfigAndClient(config, httpClient)
	if err != nil {
		return nil, err
	}
	return &ClusterTrustBundles{
		discovery: discoveryClient, metadata: metadataClient, objects: objectClient,
		discoveryErr: errors.New("waiting for initial discovery"),
	}, nil
}

func (*ClusterTrustBundles) NeedLeaderElection() bool { return true }

func (b *ClusterTrustBundles) Watch(mapEvent handler.MapFunc) source.Source {
	if b == nil {
		return nil
	}
	events := make(chan event.GenericEvent, 128)
	b.subscribers = append(b.subscribers, events)
	return source.Channel(events, handler.EnqueueRequestsFromMapFunc(mapEvent))
}

// Start discovers the native resource and starts a metadata-only informer as
// soon as a version becomes available. Its LIST/WATCH retries never block the
// manager cache or any controller's queue.
func (b *ClusterTrustBundles) Start(ctx context.Context) error {
	var watching schema.GroupVersionResource
	stopWatch := func() {}
	defer func() { stopWatch() }()
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		discoveryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resource, err := b.discoverResource(discoveryCtx)
		cancel()

		b.mu.Lock()
		b.discoveryErr = err
		// A working live GET can still establish trust during a discovery outage.
		if err == nil {
			b.resource = resource
		}
		b.mu.Unlock()

		if err != nil || resource == watching {
			return
		}
		stopWatch()
		watching = resource
		if resource.Empty() {
			return
		}
		watchCtx, cancelWatch := context.WithCancel(ctx)
		stopWatch = cancelWatch
		b.watchResource(watchCtx, resource)
	}, 5*time.Second)
	return nil
}

func (b *ClusterTrustBundles) discoverResource(ctx context.Context) (schema.GroupVersionResource, error) {
	groups, resources, failed, err := b.discovery.GroupsAndMaybeResourcesWithContext(ctx)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	if groups == nil {
		return schema.GroupVersionResource{}, errors.New("discovery returned no API group inventory")
	}
	// Aggregated discovery omits stale versions from the group inventory.
	// A failed version cannot establish that ClusterTrustBundle is unserved.
	var incomplete error
	for gv, cause := range failed {
		if gv.Group == clusterTrustBundleKind.Group {
			incomplete = fmt.Errorf("discover %s: %w", gv, cause)
		}
	}
	for _, group := range groups.Groups {
		if group.Name != clusterTrustBundleKind.Group {
			continue
		}
		for _, version := range group.Versions {
			gv := schema.GroupVersion{Group: group.Name, Version: version.Version}
			if failed[gv] != nil {
				continue
			}
			list := resources[gv]
			if resources == nil {
				list, err = b.discovery.ServerResourcesForGroupVersionWithContext(ctx, gv.String())
				if err != nil {
					incomplete = fmt.Errorf("discover %s: %w", gv, err)
					continue
				}
			}
			if list == nil || list.GroupVersion != gv.String() {
				incomplete = fmt.Errorf("incomplete discovery for %s", gv)
				continue
			}
			for _, resource := range list.APIResources {
				if resource.Name == clusterTrustBundleResource && resource.Kind == clusterTrustBundleKind.Kind && !resource.Namespaced {
					return gv.WithResource(clusterTrustBundleResource), nil
				}
			}
		}
		break
	}
	return schema.GroupVersionResource{}, incomplete
}

func (b *ClusterTrustBundles) watchResource(ctx context.Context, resource schema.GroupVersionResource) {
	factory := metadatainformer.NewSharedInformerFactory(b.metadata, 0)
	informer := factory.ForResource(resource).Informer()
	notify := func(obj any) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		meta, ok := obj.(*metav1.PartialObjectMetadata)
		if !ok {
			return
		}
		for _, events := range b.subscribers {
			select {
			case events <- event.GenericEvent{Object: meta}:
			case <-ctx.Done():
				return
			}
		}
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: notify,
		UpdateFunc: func(old, current any) {
			if old.(*metav1.PartialObjectMetadata).ResourceVersion != current.(*metav1.PartialObjectMetadata).ResourceVersion {
				notify(current)
			}
		},
		DeleteFunc: notify,
	})
	if err != nil {
		// The informer is new, so registration cannot fail in normal use.
		return
	}
	go informer.RunWithContext(ctx)
}

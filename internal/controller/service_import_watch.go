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

package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"
	mcsclient "sigs.k8s.io/mcs-api/pkg/client/clientset/versioned/typed/apis/v1beta1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const serviceImportCRDName = "serviceimports." + model.ServiceImportGroup

// ServiceImportWatch uses the existing metadata cache to notice CRD lifecycle changes.
// The optional ServiceImport informer never gates the manager's required caches.
type ServiceImportWatch struct {
	reader      client.Reader
	imports     mcsclient.ServiceImportInterface
	refresh     workqueue.TypedRateLimitingInterface[string]
	subscribers []chan event.GenericEvent
}

// SetupServiceImportWatch registers one optional informer shared by the route controllers.
func SetupServiceImportWatch(manager ctrl.Manager, enabled bool) (*ServiceImportWatch, error) {
	if !enabled {
		return nil, nil
	}
	client, err := mcsclient.NewForConfigAndClient(manager.GetConfig(), manager.GetHTTPClient())
	if err != nil {
		return nil, fmt.Errorf("create ServiceImport watch client: %w", err)
	}
	watch := &ServiceImportWatch{
		reader:  manager.GetAPIReader(),
		imports: client.ServiceImports(metav1.NamespaceAll),
		refresh: workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 30*time.Second)),
	}
	if err := manager.Add(watch); err != nil {
		return nil, err
	}
	return watch, nil
}

// WatchCRD registers the CRD lifecycle watch on one controller.
func (w *ServiceImportWatch) WatchCRD(setup *builder.Builder) {
	setup.WatchesMetadata(&apiextensionsv1.CustomResourceDefinition{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, _ client.Object) []reconcile.Request {
		w.refresh.Add(serviceImportCRDName)
		return nil
	}), builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}, predicate.NewPredicateFuncs(func(object client.Object) bool {
		return object.GetName() == serviceImportCRDName
	})))
}

func (*ServiceImportWatch) NeedLeaderElection() bool { return true }

// Subscribe returns a controller source for import and CRD lifecycle events.
func (w *ServiceImportWatch) Subscribe(mapEvent handler.MapFunc) source.Source {
	events := make(chan event.GenericEvent, 128)
	w.subscribers = append(w.subscribers, events)
	return source.Channel(events, handler.EnqueueRequestsFromMapFunc(mapEvent))
}

func (w *ServiceImportWatch) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		w.refresh.ShutDown()
	}()
	w.refresh.Add(serviceImportCRDName)
	var watching types.UID
	stopWatch := func() {}
	defer func() { stopWatch() }()
	for {
		key, stopped := w.refresh.Get()
		if stopped {
			return nil
		}
		uid, err := w.servedCRD(ctx)
		if err == nil && uid != watching {
			stopWatch()
			watching = ""
			if uid != "" {
				watchCtx, stop := context.WithCancel(ctx)
				if err = w.watchResource(watchCtx); err == nil {
					stopWatch, watching = stop, uid
				} else {
					stop()
				}
			}
			// No object-level event is guaranteed when a version appears
			// without objects or when a CRD disappears.
			w.notify(ctx, &metav1.PartialObjectMetadata{})
		}
		if err != nil && ctx.Err() == nil {
			log.FromContext(ctx).Error(err, "Could not observe the ServiceImport API")
			w.refresh.AddRateLimited(key)
		} else {
			w.refresh.Forget(key)
		}
		w.refresh.Done(key)
	}
}

func (w *ServiceImportWatch) servedCRD(ctx context.Context) (types.UID, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var crd apiextensionsv1.CustomResourceDefinition
	err := w.reader.Get(readCtx, client.ObjectKey{Name: serviceImportCRDName}, &crd)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !apihelpers.IsCRDConditionTrue(&crd, apiextensionsv1.Established) ||
		!slices.ContainsFunc(crd.Spec.Versions, func(version apiextensionsv1.CustomResourceDefinitionVersion) bool {
			return version.Name == mcs.SchemeGroupVersion.Version && version.Served
		}) {
		return "", nil
	}
	return crd.UID, nil
}

func (w *ServiceImportWatch) watchResource(ctx context.Context) error {
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return w.imports.List(ctx, options)
		},
		WatchFuncWithContext: w.imports.Watch,
	}, &mcs.ServiceImport{}, 0, nil)
	notify := func(object any) {
		if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
			object = tombstone.Obj
		}
		if imported, ok := object.(*mcs.ServiceImport); ok {
			w.notify(ctx, imported)
		}
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: notify,
		UpdateFunc: func(old, current any) {
			previous, next := old.(*mcs.ServiceImport), current.(*mcs.ServiceImport)
			if previous.UID != next.UID || previous.Generation != next.Generation ||
				previous.DeletionTimestamp.IsZero() != next.DeletionTimestamp.IsZero() ||
				next.Spec.Type == mcs.Headless && (previous.Status.EndpointSliceObjects != next.Status.EndpointSliceObjects ||
					!slices.Equal(previous.Status.Clusters, next.Status.Clusters)) ||
				!equalServiceImportReadiness(previous, next) {
				notify(next)
			}
		},
		DeleteFunc: notify,
	}); err != nil {
		return err
	}
	go informer.RunWithContext(ctx)
	return nil
}

func equalServiceImportReadiness(previous, current *mcs.ServiceImport) bool {
	before, after := model.ServiceImportReadiness(previous), model.ServiceImportReadiness(current)
	if before == nil || after == nil {
		return before == after
	}
	return *before == *after
}

func (w *ServiceImportWatch) notify(ctx context.Context, object client.Object) {
	for _, events := range w.subscribers {
		select {
		case events <- event.GenericEvent{Object: object}:
		case <-ctx.Done():
			return
		}
	}
}

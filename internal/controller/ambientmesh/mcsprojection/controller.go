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

package mcsprojection

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcs "sigs.k8s.io/mcs-api/pkg/apis/v1beta1"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/controller/model"
)

const (
	queueKey        = "ambient-service-imports"
	recheckInterval = time.Minute
)

var importResource = schema.GroupResource{Group: mcs.GroupName, Resource: mcs.ServiceImportPluralName}

// SetupWithManager uses the required manager cache only for built-in resource
// events. The optional CRDs can appear, disappear or change versions at runtime.
func SetupWithManager(manager ctrl.Manager) error {
	api, err := dynamic.NewForConfigAndClient(manager.GetConfig(), manager.GetHTTPClient())
	if err != nil {
		return err
	}
	return manager.Add(&projector{api: api, reader: manager.GetAPIReader(), cache: manager.GetCache()})
}

type projector struct {
	api    dynamic.Interface
	reader client.Reader
	cache  runtimecache.Cache
}

func (*projector) NeedLeaderElection() bool { return true }

func (p *projector) Start(ctx context.Context) error {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 30*time.Second))
	defer queue.ShutDown()
	go func() {
		<-ctx.Done()
		queue.ShutDown()
	}()

	enqueue := func() { queue.Add(queueKey) }
	if err := p.watchBuiltins(ctx, eventHandler(enqueue)); err != nil {
		return err
	}
	importWatch := optionalWatch{api: p.api, handler: eventHandler(enqueue)}
	entryWatch := optionalWatch{api: p.api, handler: eventHandler(enqueue)}
	defer importWatch.stop()
	defer entryWatch.stop()
	enqueue()

	for {
		key, shutdown := queue.Get()
		if shutdown {
			return nil
		}
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		// Establish the optional watches from the CRDs first. The reconciliation
		// itself uses live reads so absence during initial informer sync cannot
		// be mistaken for deletion of an import or its last slice.
		importAPI, importErr := findServedAPI(readCtx, p.reader, importResource, "v1beta1", "v1alpha1")
		entryAPI, entryErr := findServedAPI(readCtx, p.reader, entryResource.GroupResource(), entryResource.Version)
		err := errors.Join(importErr, entryErr)
		if err == nil {
			err = errors.Join(importWatch.observe(ctx, importAPI), entryWatch.observe(ctx, entryAPI))
		}
		if err == nil && entryAPI.valid() {
			err = p.reconcile(readCtx, importAPI)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			log.FromContext(ctx).Error(err, "Could not reconcile ambient ServiceImports")
			queue.AddRateLimited(key)
		} else {
			queue.Forget(key)
			queue.AddAfter(key, recheckInterval)
		}
		queue.Done(key)
	}
}

func (p *projector) watchBuiltins(ctx context.Context, handler cache.ResourceEventHandler) error {
	crdMetadata := &metav1.PartialObjectMetadata{}
	crdMetadata.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"))
	watches := []struct {
		object client.Object
		filter func(client.Object) bool
	}{
		{object: &corev1.Service{}, filter: func(client.Object) bool { return true }},
		{object: &discoveryv1.EndpointSlice{}, filter: func(object client.Object) bool { return object.GetLabels()[mcs.LabelServiceName] != "" }},
		{object: crdMetadata, filter: func(object client.Object) bool {
			return object.GetName() == importResource.String() || object.GetName() == entryResource.GroupResource().String()
		}},
	}
	for _, watch := range watches {
		informer, err := p.cache.GetInformer(ctx, watch.object, runtimecache.BlockUntilSynced(false))
		if err != nil {
			return err
		}
		if _, err := informer.AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: func(value any) bool {
				if tombstone, ok := value.(cache.DeletedFinalStateUnknown); ok {
					value = tombstone.Obj
				}
				object, ok := value.(client.Object)
				return ok && watch.filter(object)
			},
			Handler: handler,
		}); err != nil {
			return err
		}
	}
	return nil
}

func eventHandler(enqueue func()) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(any) { enqueue() },
		UpdateFunc: func(previous, next any) {
			if previous.(metav1.Object).GetResourceVersion() != next.(metav1.Object).GetResourceVersion() {
				enqueue()
			}
		},
		DeleteFunc: func(any) { enqueue() },
	}
}

type servedAPI struct {
	resource schema.GroupVersionResource
	uid      types.UID
}

func (a servedAPI) valid() bool { return a.uid != "" }

func findServedAPI(ctx context.Context, reader client.Reader, resource schema.GroupResource, versions ...string) (servedAPI, error) {
	var crd apiextensionsv1.CustomResourceDefinition
	err := reader.Get(ctx, client.ObjectKey{Name: resource.String()}, &crd)
	if apierrors.IsNotFound(err) {
		return servedAPI{}, nil
	}
	if err != nil {
		return servedAPI{}, err
	}
	if !crd.DeletionTimestamp.IsZero() || !apihelpers.IsCRDConditionTrue(&crd, apiextensionsv1.Established) ||
		crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		return servedAPI{}, nil
	}
	for _, preferred := range versions {
		for _, version := range crd.Spec.Versions {
			if version.Name == preferred && version.Served {
				return servedAPI{resource: resource.WithVersion(preferred), uid: crd.UID}, nil
			}
		}
	}
	return servedAPI{}, nil
}

type optionalWatch struct {
	api     dynamic.Interface
	handler cache.ResourceEventHandler
	current servedAPI
	cancel  context.CancelFunc
}

func (w *optionalWatch) stop() {
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
}

func (w *optionalWatch) observe(ctx context.Context, api servedAPI) error {
	if w.current == api {
		return nil
	}
	w.stop()
	w.current = servedAPI{}
	if !api.valid() {
		return nil
	}
	resource := w.api.Resource(api.resource)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return resource.List(ctx, options)
		},
		WatchFuncWithContext: resource.Watch,
	}, &unstructured.Unstructured{}, 0, nil)
	if _, err := informer.AddEventHandler(w.handler); err != nil {
		return err
	}
	watchCtx, stop := context.WithCancel(ctx)
	w.cancel, w.current = stop, api
	go informer.RunWithContext(watchCtx)
	return nil
}

func (p *projector) reconcile(ctx context.Context, importAPI servedAPI) error {
	entries, err := p.api.Resource(entryResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list ServiceEntries: %w", err)
	}
	desired, inventoryErr := p.desiredEntries(ctx, importAPI, entries.Items)
	var reconcileErrors []error
	if inventoryErr != nil {
		// These lists cover the whole projected inventory. Once its contents
		// cannot be established, withdraw only entries owned by this projector
		// and retry before exposing endpoints again.
		reconcileErrors = append(reconcileErrors, inventoryErr)
	}
	for i := range entries.Items {
		entry := &entries.Items[i]
		key := client.ObjectKeyFromObject(entry)
		if _, wanted := desired[key]; wanted {
			if err := p.update(ctx, entry, desired[key]); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("update projection %s: %w", key, err))
			}
			delete(desired, key)
		} else if owner := projectionOwner(entry); owner != nil && entry.GetDeletionTimestamp() == nil {
			preconditions := &metav1.Preconditions{UID: ptr.To(entry.GetUID()), ResourceVersion: ptr.To(entry.GetResourceVersion())}
			err := p.api.Resource(entryResource).Namespace(entry.GetNamespace()).Delete(ctx, entry.GetName(), metav1.DeleteOptions{Preconditions: preconditions})
			if err != nil && !apierrors.IsNotFound(err) {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("withdraw projection %s: %w", key, err))
			} else {
				log.FromContext(ctx).Info("Withdrew ambient ServiceImport projection", "serviceEntry", key)
			}
		}
	}
	for key, item := range desired {
		_, err := p.api.Resource(entryResource).Namespace(key.Namespace).Create(ctx, item, metav1.CreateOptions{})
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("create projection %s: %w", key, err))
		} else {
			log.FromContext(ctx).Info("Created ambient ServiceImport projection", "serviceEntry", key)
		}
	}
	return errors.Join(reconcileErrors...)
}

func (p *projector) listImports(ctx context.Context, api servedAPI) ([]mcs.ServiceImport, error) {
	if !api.valid() {
		return nil, nil
	}
	list, err := p.api.Resource(api.resource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	imports := make([]mcs.ServiceImport, 0, len(list.Items))
	for _, item := range list.Items {
		var imported mcs.ServiceImport
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, &imported); err != nil {
			return nil, fmt.Errorf("decode %s/%s: %w", item.GetNamespace(), item.GetName(), err)
		}
		// Get the actual served version from the resource, even if a list item
		// omits its type metadata.
		imported.APIVersion = api.resource.GroupVersion().String()
		imports = append(imports, imported)
	}
	return imports, nil
}

func (p *projector) desiredEntries(ctx context.Context, importAPI servedAPI, entries []unstructured.Unstructured) (map[types.NamespacedName]*unstructured.Unstructured, error) {
	imports, err := p.listImports(ctx, importAPI)
	if err != nil {
		return nil, fmt.Errorf("list ServiceImports: %w", err)
	}
	desired := make(map[types.NamespacedName]*unstructured.Unstructured)
	if len(imports) == 0 {
		return desired, nil
	}
	var services corev1.ServiceList
	if err := p.reader.List(ctx, &services); err != nil {
		return nil, fmt.Errorf("list Kubernetes Services: %w", err)
	}
	var endpointSlices discoveryv1.EndpointSliceList
	if err := p.reader.List(ctx, &endpointSlices, client.HasLabels{mcs.LabelServiceName}); err != nil {
		return nil, fmt.Errorf("list MCS EndpointSlices: %w", err)
	}
	slicesByImport := make(map[types.NamespacedName][]discoveryv1.EndpointSlice)
	for i := range endpointSlices.Items {
		slice := &endpointSlices.Items[i]
		key := model.ServiceImportEndpointSliceKey(slice)
		slicesByImport[key] = append(slicesByImport[key], *slice)
	}
	for i := range imports {
		imported := &imports[i]
		key := types.NamespacedName{Namespace: imported.Namespace, Name: imported.Name}
		item, reason := derive(imported, slicesByImport[key])
		if reason == "" {
			reason = item.collision(services.Items, entries, imports)
		}
		if reason == "NotEnrolled" {
			continue
		}
		if reason != "" {
			log.FromContext(ctx).V(1).Info("Ambient ServiceImport not projected", "serviceImport", key, "reason", reason)
			continue
		}
		desired[client.ObjectKeyFromObject(item.entry)] = item.entry
	}
	return desired, nil
}

func (p *projector) update(ctx context.Context, current, desired *unstructured.Unstructured) error {
	if current.GetDeletionTimestamp() != nil {
		return nil
	}
	owner := projectionOwner(current)
	if owner == nil || owner.UID != desired.GetOwnerReferences()[0].UID {
		return nil
	}
	if reflect.DeepEqual(current.Object["spec"], desired.Object["spec"]) &&
		reflect.DeepEqual(current.GetLabels(), desired.GetLabels()) &&
		current.GetAnnotations()[sourceAnnotation] == desired.GetAnnotations()[sourceAnnotation] &&
		reflect.DeepEqual(current.GetOwnerReferences(), desired.GetOwnerReferences()) {
		return nil
	}
	next := current.DeepCopy()
	next.Object["spec"] = desired.Object["spec"]
	next.SetLabels(desired.GetLabels())
	annotations := next.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[sourceAnnotation] = desired.GetAnnotations()[sourceAnnotation]
	next.SetAnnotations(annotations)
	next.SetOwnerReferences(desired.GetOwnerReferences())
	_, err := p.api.Resource(entryResource).Namespace(next.GetNamespace()).Update(ctx, next, metav1.UpdateOptions{})
	return err
}

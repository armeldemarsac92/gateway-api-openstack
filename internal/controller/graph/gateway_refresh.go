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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gateway-api-openstack/gateway-api-openstack/internal/cloud"
)

// GatewayRefreshQueue schedules graph refreshes on the Gateway controller's
// queue, where delayed requests for the same Gateway coalesce.
type GatewayRefreshQueue struct {
	mu    sync.RWMutex
	queue workqueue.TypedRateLimitingInterface[reconcile.Request]
}

func (q *GatewayRefreshQueue) Start(_ context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = queue
	return nil
}

func (q *GatewayRefreshQueue) addAt(identity cloud.Identity, deadline time.Time) {
	if q == nil || deadline.IsZero() {
		return
	}
	q.mu.RLock()
	queue := q.queue
	q.mu.RUnlock()
	if queue != nil {
		queue.AddAfter(reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: identity.GatewayNamespace, Name: identity.GatewayName,
		}}, time.Until(deadline))
	}
}

# CRD channel schema field inventory

Standard: 10 CRDs; Experimental: 14; shared: 10; Experimental-only: 4. Compares property paths in every served version; excludes differences in constraints, defaults, validation, semantics and implementation.


## backendtlspolicies.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_backendtlspolicies.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_backendtlspolicies.yaml)

**v1**: Standard 38 paths; Experimental 38; Experimental-only 0; Standard-only 0.

**v1alpha3**: Standard 0 paths; Experimental 38; Experimental-only 38; Standard-only 0.

Experimental-only: apiVersion, kind, metadata, spec, spec.options, spec.targetRefs, spec.targetRefs[].group, spec.targetRefs[].kind, spec.targetRefs[].name, spec.targetRefs[].sectionName, spec.validation, spec.validation.caCertificateRefs, spec.validation.caCertificateRefs[].group, spec.validation.caCertificateRefs[].kind, spec.validation.caCertificateRefs[].name, spec.validation.hostname, spec.validation.subjectAltNames, spec.validation.subjectAltNames[].hostname, spec.validation.subjectAltNames[].type, spec.validation.subjectAltNames[].uri, spec.validation.wellKnownCACertificates, status, status.ancestors, status.ancestors[].ancestorRef, status.ancestors[].ancestorRef.group, status.ancestors[].ancestorRef.kind, status.ancestors[].ancestorRef.name, status.ancestors[].ancestorRef.namespace, status.ancestors[].ancestorRef.port, status.ancestors[].ancestorRef.sectionName, status.ancestors[].conditions, status.ancestors[].conditions[].lastTransitionTime, status.ancestors[].conditions[].message, status.ancestors[].conditions[].observedGeneration, status.ancestors[].conditions[].reason, status.ancestors[].conditions[].status, status.ancestors[].conditions[].type, status.ancestors[].controllerName


## gatewayclasses.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_gatewayclasses.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_gatewayclasses.yaml)

**v1**: Standard 21 paths; Experimental 21; Experimental-only 0; Standard-only 0.

**v1beta1**: Standard 21 paths; Experimental 21; Experimental-only 0; Standard-only 0.


## gateways.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_gateways.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_gateways.yaml)

**v1**: Standard 100 paths; Experimental 126; Experimental-only 26; Standard-only 0.

Experimental-only: spec.addresses[].routability, spec.defaultScope, spec.listeners[].filters, spec.listeners[].filters.requests, spec.listeners[].filters.requests[].extensionRef, spec.listeners[].filters.requests[].extensionRef.group, spec.listeners[].filters.requests[].extensionRef.kind, spec.listeners[].filters.requests[].extensionRef.name, spec.listeners[].filters.requests[].externalAuth, spec.listeners[].filters.requests[].externalAuth.backendRef, spec.listeners[].filters.requests[].externalAuth.backendRef.group, spec.listeners[].filters.requests[].externalAuth.backendRef.kind, spec.listeners[].filters.requests[].externalAuth.backendRef.name, spec.listeners[].filters.requests[].externalAuth.backendRef.namespace, spec.listeners[].filters.requests[].externalAuth.backendRef.port, spec.listeners[].filters.requests[].externalAuth.forwardBody, spec.listeners[].filters.requests[].externalAuth.forwardBody.maxSize, spec.listeners[].filters.requests[].externalAuth.grpc, spec.listeners[].filters.requests[].externalAuth.grpc.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http, spec.listeners[].filters.requests[].externalAuth.http.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http.allowedResponseHeaders, spec.listeners[].filters.requests[].externalAuth.http.path, spec.listeners[].filters.requests[].externalAuth.protocol, spec.listeners[].filters.requests[].type, status.addresses[].routability

**v1beta1**: Standard 100 paths; Experimental 126; Experimental-only 26; Standard-only 0.

Experimental-only: spec.addresses[].routability, spec.defaultScope, spec.listeners[].filters, spec.listeners[].filters.requests, spec.listeners[].filters.requests[].extensionRef, spec.listeners[].filters.requests[].extensionRef.group, spec.listeners[].filters.requests[].extensionRef.kind, spec.listeners[].filters.requests[].extensionRef.name, spec.listeners[].filters.requests[].externalAuth, spec.listeners[].filters.requests[].externalAuth.backendRef, spec.listeners[].filters.requests[].externalAuth.backendRef.group, spec.listeners[].filters.requests[].externalAuth.backendRef.kind, spec.listeners[].filters.requests[].externalAuth.backendRef.name, spec.listeners[].filters.requests[].externalAuth.backendRef.namespace, spec.listeners[].filters.requests[].externalAuth.backendRef.port, spec.listeners[].filters.requests[].externalAuth.forwardBody, spec.listeners[].filters.requests[].externalAuth.forwardBody.maxSize, spec.listeners[].filters.requests[].externalAuth.grpc, spec.listeners[].filters.requests[].externalAuth.grpc.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http, spec.listeners[].filters.requests[].externalAuth.http.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http.allowedResponseHeaders, spec.listeners[].filters.requests[].externalAuth.http.path, spec.listeners[].filters.requests[].externalAuth.protocol, spec.listeners[].filters.requests[].type, status.addresses[].routability


## grpcroutes.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_grpcroutes.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_grpcroutes.yaml)

**v1**: Standard 113 paths; Experimental 123; Experimental-only 10; Standard-only 0.

Experimental-only: spec.rules[].sessionPersistence, spec.rules[].sessionPersistence.absoluteTimeout, spec.rules[].sessionPersistence.cookie, spec.rules[].sessionPersistence.cookie.lifetimeType, spec.rules[].sessionPersistence.cookie.name, spec.rules[].sessionPersistence.cookie.path, spec.rules[].sessionPersistence.header, spec.rules[].sessionPersistence.header.name, spec.rules[].sessionPersistence.type, spec.useDefaultGateways


## httproutes.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_httproutes.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_httproutes.yaml)

**v1**: Standard 168 paths; Experimental 210; Experimental-only 42; Standard-only 0.

Experimental-only: spec.rules[].backendRefs[].filters[].externalAuth, spec.rules[].backendRefs[].filters[].externalAuth.backendRef, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.group, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.kind, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.name, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.namespace, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.port, spec.rules[].backendRefs[].filters[].externalAuth.forwardBody, spec.rules[].backendRefs[].filters[].externalAuth.forwardBody.maxSize, spec.rules[].backendRefs[].filters[].externalAuth.grpc, spec.rules[].backendRefs[].filters[].externalAuth.grpc.allowedHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http, spec.rules[].backendRefs[].filters[].externalAuth.http.allowedHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http.allowedResponseHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http.path, spec.rules[].backendRefs[].filters[].externalAuth.protocol, spec.rules[].filters[].externalAuth, spec.rules[].filters[].externalAuth.backendRef, spec.rules[].filters[].externalAuth.backendRef.group, spec.rules[].filters[].externalAuth.backendRef.kind, spec.rules[].filters[].externalAuth.backendRef.name, spec.rules[].filters[].externalAuth.backendRef.namespace, spec.rules[].filters[].externalAuth.backendRef.port, spec.rules[].filters[].externalAuth.forwardBody, spec.rules[].filters[].externalAuth.forwardBody.maxSize, spec.rules[].filters[].externalAuth.grpc, spec.rules[].filters[].externalAuth.grpc.allowedHeaders, spec.rules[].filters[].externalAuth.http, spec.rules[].filters[].externalAuth.http.allowedHeaders, spec.rules[].filters[].externalAuth.http.allowedResponseHeaders, spec.rules[].filters[].externalAuth.http.path, spec.rules[].filters[].externalAuth.protocol, spec.rules[].sessionPersistence, spec.rules[].sessionPersistence.absoluteTimeout, spec.rules[].sessionPersistence.cookie, spec.rules[].sessionPersistence.cookie.lifetimeType, spec.rules[].sessionPersistence.cookie.name, spec.rules[].sessionPersistence.cookie.path, spec.rules[].sessionPersistence.header, spec.rules[].sessionPersistence.header.name, spec.rules[].sessionPersistence.type, spec.useDefaultGateways

**v1beta1**: Standard 168 paths; Experimental 210; Experimental-only 42; Standard-only 0.

Experimental-only: spec.rules[].backendRefs[].filters[].externalAuth, spec.rules[].backendRefs[].filters[].externalAuth.backendRef, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.group, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.kind, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.name, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.namespace, spec.rules[].backendRefs[].filters[].externalAuth.backendRef.port, spec.rules[].backendRefs[].filters[].externalAuth.forwardBody, spec.rules[].backendRefs[].filters[].externalAuth.forwardBody.maxSize, spec.rules[].backendRefs[].filters[].externalAuth.grpc, spec.rules[].backendRefs[].filters[].externalAuth.grpc.allowedHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http, spec.rules[].backendRefs[].filters[].externalAuth.http.allowedHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http.allowedResponseHeaders, spec.rules[].backendRefs[].filters[].externalAuth.http.path, spec.rules[].backendRefs[].filters[].externalAuth.protocol, spec.rules[].filters[].externalAuth, spec.rules[].filters[].externalAuth.backendRef, spec.rules[].filters[].externalAuth.backendRef.group, spec.rules[].filters[].externalAuth.backendRef.kind, spec.rules[].filters[].externalAuth.backendRef.name, spec.rules[].filters[].externalAuth.backendRef.namespace, spec.rules[].filters[].externalAuth.backendRef.port, spec.rules[].filters[].externalAuth.forwardBody, spec.rules[].filters[].externalAuth.forwardBody.maxSize, spec.rules[].filters[].externalAuth.grpc, spec.rules[].filters[].externalAuth.grpc.allowedHeaders, spec.rules[].filters[].externalAuth.http, spec.rules[].filters[].externalAuth.http.allowedHeaders, spec.rules[].filters[].externalAuth.http.allowedResponseHeaders, spec.rules[].filters[].externalAuth.http.path, spec.rules[].filters[].externalAuth.protocol, spec.rules[].sessionPersistence, spec.rules[].sessionPersistence.absoluteTimeout, spec.rules[].sessionPersistence.cookie, spec.rules[].sessionPersistence.cookie.lifetimeType, spec.rules[].sessionPersistence.cookie.name, spec.rules[].sessionPersistence.cookie.path, spec.rules[].sessionPersistence.header, spec.rules[].sessionPersistence.header.name, spec.rules[].sessionPersistence.type, spec.useDefaultGateways


## listenersets.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_listenersets.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_listenersets.yaml)

**v1**: Standard 55 paths; Experimental 78; Experimental-only 23; Standard-only 0.

Experimental-only: spec.listeners[].filters, spec.listeners[].filters.requests, spec.listeners[].filters.requests[].extensionRef, spec.listeners[].filters.requests[].extensionRef.group, spec.listeners[].filters.requests[].extensionRef.kind, spec.listeners[].filters.requests[].extensionRef.name, spec.listeners[].filters.requests[].externalAuth, spec.listeners[].filters.requests[].externalAuth.backendRef, spec.listeners[].filters.requests[].externalAuth.backendRef.group, spec.listeners[].filters.requests[].externalAuth.backendRef.kind, spec.listeners[].filters.requests[].externalAuth.backendRef.name, spec.listeners[].filters.requests[].externalAuth.backendRef.namespace, spec.listeners[].filters.requests[].externalAuth.backendRef.port, spec.listeners[].filters.requests[].externalAuth.forwardBody, spec.listeners[].filters.requests[].externalAuth.forwardBody.maxSize, spec.listeners[].filters.requests[].externalAuth.grpc, spec.listeners[].filters.requests[].externalAuth.grpc.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http, spec.listeners[].filters.requests[].externalAuth.http.allowedHeaders, spec.listeners[].filters.requests[].externalAuth.http.allowedResponseHeaders, spec.listeners[].filters.requests[].externalAuth.http.path, spec.listeners[].filters.requests[].externalAuth.protocol, spec.listeners[].filters.requests[].type


## referencegrants.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_referencegrants.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_referencegrants.yaml)

**v1**: Standard 12 paths; Experimental 12; Experimental-only 0; Standard-only 0.

**v1beta1**: Standard 12 paths; Experimental 12; Experimental-only 0; Standard-only 0.


## tcproutes.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_tcproutes.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_tcproutes.yaml)

**v1**: Standard 37 paths; Experimental 38; Experimental-only 1; Standard-only 0.

Experimental-only: spec.useDefaultGateways

**v1alpha2**: Standard 0 paths; Experimental 38; Experimental-only 38; Standard-only 0.

Experimental-only: apiVersion, kind, metadata, spec, spec.parentRefs, spec.parentRefs[].group, spec.parentRefs[].kind, spec.parentRefs[].name, spec.parentRefs[].namespace, spec.parentRefs[].port, spec.parentRefs[].sectionName, spec.rules, spec.rules[].backendRefs, spec.rules[].backendRefs[].group, spec.rules[].backendRefs[].kind, spec.rules[].backendRefs[].name, spec.rules[].backendRefs[].namespace, spec.rules[].backendRefs[].port, spec.rules[].backendRefs[].weight, spec.rules[].name, spec.useDefaultGateways, status, status.parents, status.parents[].conditions, status.parents[].conditions[].lastTransitionTime, status.parents[].conditions[].message, status.parents[].conditions[].observedGeneration, status.parents[].conditions[].reason, status.parents[].conditions[].status, status.parents[].conditions[].type, status.parents[].controllerName, status.parents[].parentRef, status.parents[].parentRef.group, status.parents[].parentRef.kind, status.parents[].parentRef.name, status.parents[].parentRef.namespace, status.parents[].parentRef.port, status.parents[].parentRef.sectionName


## tlsroutes.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_tlsroutes.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_tlsroutes.yaml)

**v1**: Standard 38 paths; Experimental 39; Experimental-only 1; Standard-only 0.

Experimental-only: spec.useDefaultGateways

**v1alpha2**: Standard 0 paths; Experimental 39; Experimental-only 39; Standard-only 0.

Experimental-only: apiVersion, kind, metadata, spec, spec.hostnames, spec.parentRefs, spec.parentRefs[].group, spec.parentRefs[].kind, spec.parentRefs[].name, spec.parentRefs[].namespace, spec.parentRefs[].port, spec.parentRefs[].sectionName, spec.rules, spec.rules[].backendRefs, spec.rules[].backendRefs[].group, spec.rules[].backendRefs[].kind, spec.rules[].backendRefs[].name, spec.rules[].backendRefs[].namespace, spec.rules[].backendRefs[].port, spec.rules[].backendRefs[].weight, spec.rules[].name, spec.useDefaultGateways, status, status.parents, status.parents[].conditions, status.parents[].conditions[].lastTransitionTime, status.parents[].conditions[].message, status.parents[].conditions[].observedGeneration, status.parents[].conditions[].reason, status.parents[].conditions[].status, status.parents[].conditions[].type, status.parents[].controllerName, status.parents[].parentRef, status.parents[].parentRef.group, status.parents[].parentRef.kind, status.parents[].parentRef.name, status.parents[].parentRef.namespace, status.parents[].parentRef.port, status.parents[].parentRef.sectionName

**v1alpha3**: Standard 0 paths; Experimental 39; Experimental-only 39; Standard-only 0.

Experimental-only: apiVersion, kind, metadata, spec, spec.hostnames, spec.parentRefs, spec.parentRefs[].group, spec.parentRefs[].kind, spec.parentRefs[].name, spec.parentRefs[].namespace, spec.parentRefs[].port, spec.parentRefs[].sectionName, spec.rules, spec.rules[].backendRefs, spec.rules[].backendRefs[].group, spec.rules[].backendRefs[].kind, spec.rules[].backendRefs[].name, spec.rules[].backendRefs[].namespace, spec.rules[].backendRefs[].port, spec.rules[].backendRefs[].weight, spec.rules[].name, spec.useDefaultGateways, status, status.parents, status.parents[].conditions, status.parents[].conditions[].lastTransitionTime, status.parents[].conditions[].message, status.parents[].conditions[].observedGeneration, status.parents[].conditions[].reason, status.parents[].conditions[].status, status.parents[].conditions[].type, status.parents[].controllerName, status.parents[].parentRef, status.parents[].parentRef.group, status.parents[].parentRef.kind, status.parents[].parentRef.name, status.parents[].parentRef.namespace, status.parents[].parentRef.port, status.parents[].parentRef.sectionName


## udproutes.gateway.networking.k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.k8s.io_udproutes.yaml)
 · [Standard CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/standard/gateway.networking.k8s.io_udproutes.yaml)

**v1**: Standard 37 paths; Experimental 38; Experimental-only 1; Standard-only 0.

Experimental-only: spec.useDefaultGateways

**v1alpha2**: Standard 0 paths; Experimental 38; Experimental-only 38; Standard-only 0.

Experimental-only: apiVersion, kind, metadata, spec, spec.parentRefs, spec.parentRefs[].group, spec.parentRefs[].kind, spec.parentRefs[].name, spec.parentRefs[].namespace, spec.parentRefs[].port, spec.parentRefs[].sectionName, spec.rules, spec.rules[].backendRefs, spec.rules[].backendRefs[].group, spec.rules[].backendRefs[].kind, spec.rules[].backendRefs[].name, spec.rules[].backendRefs[].namespace, spec.rules[].backendRefs[].port, spec.rules[].backendRefs[].weight, spec.rules[].name, spec.useDefaultGateways, status, status.parents, status.parents[].conditions, status.parents[].conditions[].lastTransitionTime, status.parents[].conditions[].message, status.parents[].conditions[].observedGeneration, status.parents[].conditions[].reason, status.parents[].conditions[].status, status.parents[].conditions[].type, status.parents[].controllerName, status.parents[].parentRef, status.parents[].parentRef.group, status.parents[].parentRef.kind, status.parents[].parentRef.name, status.parents[].parentRef.namespace, status.parents[].parentRef.port, status.parents[].parentRef.sectionName


## xbackends.gateway.networking.x-k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.x-k8s.io_xbackends.yaml)

Experimental-only resource; profile membership is not implied.

**v1alpha1 fields**: apiVersion, kind, metadata, spec, spec.endpointSelector, spec.endpointSelector.matchLabels, spec.externalHostname, spec.externalHostname.hostname, spec.port, spec.port.number, spec.protocol, spec.sessionPersistence, spec.sessionPersistence.absoluteTimeout, spec.sessionPersistence.cookie, spec.sessionPersistence.cookie.lifetimeType, spec.sessionPersistence.cookie.name, spec.sessionPersistence.cookie.path, spec.sessionPersistence.header, spec.sessionPersistence.header.name, spec.sessionPersistence.type, spec.tls, spec.tls.clientCertificateRef, spec.tls.clientCertificateRef.group, spec.tls.clientCertificateRef.kind, spec.tls.clientCertificateRef.name, spec.tls.clientCertificateRef.namespace, spec.tls.mode, spec.tls.validation, spec.tls.validation.caCertificateRefs, spec.tls.validation.caCertificateRefs[].group, spec.tls.validation.caCertificateRefs[].kind, spec.tls.validation.caCertificateRefs[].name, spec.tls.validation.hostname, spec.tls.validation.subjectAltNames, spec.tls.validation.subjectAltNames[].hostname, spec.tls.validation.subjectAltNames[].type, spec.tls.validation.subjectAltNames[].uri, spec.tls.validation.wellKnownCACertificates, spec.type, status, status.ancestors, status.ancestors[].ancestorRef, status.ancestors[].ancestorRef.group, status.ancestors[].ancestorRef.kind, status.ancestors[].ancestorRef.name, status.ancestors[].ancestorRef.namespace, status.ancestors[].ancestorRef.port, status.ancestors[].ancestorRef.sectionName, status.ancestors[].conditions, status.ancestors[].conditions[].lastTransitionTime, status.ancestors[].conditions[].message, status.ancestors[].conditions[].observedGeneration, status.ancestors[].conditions[].reason, status.ancestors[].conditions[].status, status.ancestors[].conditions[].type, status.ancestors[].controllerName


## xbackendtrafficpolicies.gateway.networking.x-k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.x-k8s.io_xbackendtrafficpolicies.yaml)

Experimental-only resource; profile membership is not implied.

**v1alpha1 fields**: apiVersion, kind, metadata, spec, spec.retryConstraint, spec.retryConstraint.budget, spec.retryConstraint.budget.interval, spec.retryConstraint.budget.percent, spec.retryConstraint.minRetryRate, spec.retryConstraint.minRetryRate.count, spec.retryConstraint.minRetryRate.interval, spec.sessionPersistence, spec.sessionPersistence.absoluteTimeout, spec.sessionPersistence.cookie, spec.sessionPersistence.cookie.lifetimeType, spec.sessionPersistence.cookie.name, spec.sessionPersistence.cookie.path, spec.sessionPersistence.header, spec.sessionPersistence.header.name, spec.sessionPersistence.type, spec.targetRefs, spec.targetRefs[].group, spec.targetRefs[].kind, spec.targetRefs[].name, status, status.ancestors, status.ancestors[].ancestorRef, status.ancestors[].ancestorRef.group, status.ancestors[].ancestorRef.kind, status.ancestors[].ancestorRef.name, status.ancestors[].ancestorRef.namespace, status.ancestors[].ancestorRef.port, status.ancestors[].ancestorRef.sectionName, status.ancestors[].conditions, status.ancestors[].conditions[].lastTransitionTime, status.ancestors[].conditions[].message, status.ancestors[].conditions[].observedGeneration, status.ancestors[].conditions[].reason, status.ancestors[].conditions[].status, status.ancestors[].conditions[].type, status.ancestors[].controllerName


## xmeshes.gateway.networking.x-k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.x-k8s.io_xmeshes.yaml)

Experimental-only resource; profile membership is not implied.

**v1alpha1 fields**: apiVersion, kind, metadata, spec, spec.controllerName, spec.description, spec.parametersRef, spec.parametersRef.group, spec.parametersRef.kind, spec.parametersRef.name, spec.parametersRef.namespace, status, status.conditions, status.conditions[].lastTransitionTime, status.conditions[].message, status.conditions[].observedGeneration, status.conditions[].reason, status.conditions[].status, status.conditions[].type, status.supportedFeatures, status.supportedFeatures[].name


## xtelemetrypolicies.gateway.networking.x-k8s.io

[Experimental CRD](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/config/crd/experimental/gateway.networking.x-k8s.io_xtelemetrypolicies.yaml)

Experimental-only resource; profile membership is not implied.

**v1alpha1 fields**: apiVersion, kind, metadata, spec, spec.targetRefs, spec.targetRefs[].group, spec.targetRefs[].kind, spec.targetRefs[].name, spec.tracing, spec.tracing.attributes, spec.tracing.attributes[].attributeKey, spec.tracing.attributes[].headerName, spec.tracing.attributes[].literalValue, spec.tracing.attributes[].name, spec.tracing.attributes[].sourceType, spec.tracing.mode, spec.tracing.parentBasedSampling, spec.tracing.parentBasedSampling.mode, spec.tracing.parentBasedSampling.samplingRate, spec.tracing.parentBasedSampling.samplingRate.denominator, spec.tracing.parentBasedSampling.samplingRate.numerator, spec.tracing.provider, spec.tracing.provider.backendRef, spec.tracing.provider.backendRef.group, spec.tracing.provider.backendRef.kind, spec.tracing.provider.backendRef.name, spec.tracing.provider.backendRef.namespace, spec.tracing.provider.backendRef.port, spec.tracing.provider.headers, spec.tracing.provider.headers[].name, spec.tracing.provider.headers[].value, spec.tracing.samplingRate, spec.tracing.samplingRate.denominator, spec.tracing.samplingRate.numerator, spec.tracing.serviceName, spec.tracing.spanName, status, status.ancestors, status.ancestors[].ancestorRef, status.ancestors[].ancestorRef.group, status.ancestors[].ancestorRef.kind, status.ancestors[].ancestorRef.name, status.ancestors[].ancestorRef.namespace, status.ancestors[].ancestorRef.port, status.ancestors[].ancestorRef.sectionName, status.ancestors[].conditions, status.ancestors[].conditions[].lastTransitionTime, status.ancestors[].conditions[].message, status.ancestors[].conditions[].observedGeneration, status.ancestors[].conditions[].reason, status.ancestors[].conditions[].status, status.ancestors[].conditions[].type, status.ancestors[].controllerName

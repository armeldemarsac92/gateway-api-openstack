# Experimental backend TLS conflict resolution

The upstream Gateway API v1.7.0 case `BackendTLSPolicyConflictResolution` passed on controller `71d946b5` and Octavia `300b2638`. The four HTTP requests reached the backend selected by the accepted policy, including the cases where policies selected individual Service ports. The upstream Extended report records one pass and 63 skipped cases. [Official report](experimental.yaml)

After the suite requested deletion of the HTTPRoute, a Kubernetes observer first received `NotFound` 8.954 seconds later. The upstream deadline was 30 seconds. Octavia accepted four conditional removals and the worker confirmed the four policies and their rules were gone. The local run used a kind cluster, Octavia’s API with SQLite, and HAProxy. [Runtime versions, timestamps and evidence hashes](results.json)

An earlier Experimental run on controller `13a6870c` and Octavia `e62a57c4` passed the same four HTTP checks and exceeded the 30-second deadline for deleting the route. The two runs used the same upstream conformance binary and deadline. The controller, provider, enabled conditional removal and selection differed, so the contribution of each change remains unmeasured. [Earlier report](../../../common-gateway/13a6870c-e62a57c4/targeted/backendtls-experimental-3.yaml)

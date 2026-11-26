# Targeted replays on the common controller and Octavia revisions

These local runs selected individual cases after the original Gateway profile campaign. The controller was built from signed revision `13a6870cb63a` and Octavia from `e62a57c49177`. Each link opens the unmodified YAML written by the pinned Gateway API v1.7.0 conformance executable. The receipt records its digest, the selected cases and the upstream exit.

| Channel | Profile | Subset | Selected upstream results | Upstream exit |
| --- | --- | --- | --- | ---: |
| Standard | [GATEWAY-HTTP](backendtls-standard-1.yaml) | extended | BackendTLSPolicyConflictResolution PASS, GatewayFrontendClientCertificateValidationInsecureFallback PASS | 0 |
| Experimental | [GATEWAY-GRPC](grpc-backendtls-experimental-1.yaml) | extended | BackendTLSPolicyGRPCRoute PASS | 0 |
| Experimental | [GATEWAY-GRPC](grpc-cleanup-experimental-1.yaml) | core | GRPCRouteHeaderMatching PASS | 0 |
| Experimental | [GATEWAY-TLS](tls-backendtls-experimental-1.yaml) | extended | BackendTLSPolicyTLSRouteTerminate PASS | 0 |
| Standard | [GATEWAY-TLS](tls-backendtls-standard-1.yaml) | extended | BackendTLSPolicyTLSRouteTerminate PASS | 0 |
| Experimental | [GATEWAY-TLS](tls-hostname-experimental-1.yaml) | core | TLSRouteHostnameIntersection PASS | 0 |
| Standard | [GATEWAY-UDP](udp-standard-1.yaml) | core | UDPRouteParentRefAttachAllListeners PASS | 0 |
| Experimental | [GATEWAY-HTTP](backendtls-experimental-3.yaml) | extended | BackendTLSPolicyConflictResolution FAIL, GatewayFrontendClientCertificateValidationInsecureFallback PASS | 1 |
| Standard | [GATEWAY-HTTP](http-hostname-standard-3.yaml) | core | HTTPRouteHostnameIntersection PASS | 0 |
| Standard | [GATEWAY-HTTP](request-mirror-standard.yaml) | extended | HTTPRouteRequestMirror PASS, HTTPRouteRequestMultipleMirrors PASS, HTTPRouteRequestPercentageMirror PASS | 0 |
| Experimental | [GATEWAY-HTTP](request-mirror-experimental.yaml) | extended | HTTPRouteRequestMirror PASS, HTTPRouteRequestMultipleMirrors PASS, HTTPRouteRequestPercentageMirror PASS | 0 |

In the Experimental backend TLS replay, the frontend certificate fallback case passed. The conflict-resolution case completed its four HTTP probes and then exceeded the upstream 30-second deadline while deleting the HTTPRoute. Its upstream result is FAIL.

A focused Experimental replay on controller `71d946b5` and Octavia `300b2638` passed all four backend TLS conflict HTTP checks. Kubernetes confirmed route deletion after 8.954 seconds within the upstream 30-second deadline. [Replay on the released revisions](../../../provider/conditional-l7-removal/backendtls-conflict/README.md)

Kubernetes and the request paths ran locally. The Octavia fixtures supplied cloud resources and worker completion. The original full-profile results retain their own pass, fail and skip counts. [Original campaign](../README.md) · [Inputs, attempt history and report hashes](results.json)

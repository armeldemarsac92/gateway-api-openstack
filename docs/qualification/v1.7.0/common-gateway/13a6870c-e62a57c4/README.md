# Gateway profiles on the same controller and Octavia source revisions

These local runs use Gateway API v1.7.0 and the signed Git source revisions for controller `13a6870cb63a` and Octavia `e62a57c49177`. The 22 original attempts produced 17 upstream reports.

| Gateway profile | Channel | Subset | Listener mode | Upstream pass / fail / skip | Per-case pass / selected | Suite exit | Fixture exit | Upstream report |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GATEWAY-HTTP | standard | core | default | 39 / 1 / 0 | 39 / 40 | 1 | 1 | [Report](gateway-http-standard-core-default.yaml) |
| GATEWAY-HTTP | standard | extended | default | 39 / 24 / 0 | 39 / 63 | 1 | 1 | [Report](gateway-http-standard-extended-default.yaml) |
| GATEWAY-HTTP | experimental | core | default | 40 / 0 / 0 | 40 / 40 | 0 | 0 | [Report](gateway-http-experimental-core-default.yaml) |
| GATEWAY-HTTP | experimental | extended | default | 40 / 24 / 0 | 40 / 64 | 1 | 1 | [Report](gateway-http-experimental-extended-default.yaml) |
| GATEWAY-GRPC | standard | core | default | 17 / 0 / 0 | 17 / 17 | 0 | 0 | [Report](gateway-grpc-standard-core-default.yaml) |
| GATEWAY-GRPC | standard | extended | default | 12 / 1 / 0 | 12 / 13 | 1 | 1 | [Report](gateway-grpc-standard-extended-default.yaml) |
| GATEWAY-GRPC | experimental | core | default | 16 / 1 / 0 | 16 / 17 | 1 | 1 | [Report](gateway-grpc-experimental-core-default.yaml) |
| GATEWAY-GRPC | experimental | extended | default | 12 / 1 / 0 | 12 / 13 | 1 | 1 | [Report](gateway-grpc-experimental-extended-default.yaml) |
| GATEWAY-TCP | standard | core | default | 20 / 0 / 0 | 20 / 20 | 0 | 0 | [Report](gateway-tcp-standard-core-default.yaml) |
| GATEWAY-TCP | standard | extended | default | 11 / 0 / 0 | 11 / 11 | 0 | 1 | [Report](gateway-tcp-standard-extended-default.yaml) |
| GATEWAY-TCP | experimental | core | default | 20 / 0 / 0 | 20 / 20 | 0 | 0 | [Report](gateway-tcp-experimental-core-default.yaml) |
| GATEWAY-TCP | experimental | extended | default | 11 / 0 / 0 | 11 / 11 | 0 | 1 | [Report](gateway-tcp-experimental-extended-default.yaml) |
| GATEWAY-TLS | standard | core | default | 21 / 0 / 0 | 21 / 21 | 0 | 0 | [Report](gateway-tls-standard-core-default.yaml) |
| GATEWAY-TLS | standard | extended | default | 16 / 1 / 1 | 16 / 17 | 1 | 1 | [Report](gateway-tls-standard-extended-default.yaml) |
| GATEWAY-TLS | standard | extended | terminate-only | 1 / 0 / 2 | 1 / 1 | 0 | 0 | [Report](gateway-tls-standard-extended-terminate-only.yaml) |
| GATEWAY-TLS | experimental | core | default | 20 / 1 / 0 | 20 / 21 | 1 | 1 | [Report](gateway-tls-experimental-core-default.yaml) |
| GATEWAY-TLS | experimental | extended | default | Unavailable | Not run (17 selected) | Unavailable | 1 | Unavailable |
| GATEWAY-TLS | experimental | extended | terminate-only | 1 / 0 / 2 | 1 / 1 | 0 | 0 | [Report](gateway-tls-experimental-extended-terminate-only.yaml) |
| GATEWAY-UDP | standard | core | default | Unavailable | Not run (21 selected) | Unavailable | 1 | Unavailable |
| GATEWAY-UDP | standard | extended | default | Unavailable | Not run (11 selected) | Unavailable | 1 | Unavailable |
| GATEWAY-UDP | experimental | core | default | Unavailable | Not run (21 selected) | Unavailable | 1 | Unavailable |
| GATEWAY-UDP | experimental | extended | default | Unavailable | Not run (11 selected) | Unavailable | 1 | Unavailable |

Each row reports the requested subset of its upstream profile. Other subsets in the same YAML have separate statistics. The per-case count uses the case log and any required nested assertions. Static address allocator events are listed separately from the upstream case verdict. A failure named in the upstream report counts as a failure even if the log parser missed its final line. The results retain the original receipt counts wherever they differ. Selected cases can occur in multiple rows. [Inputs and per-case outcomes](results.json)

The campaign ran against local Kubernetes, Octavia and native HAProxy. Qualification on a deployed Hibiscus region remains open.

## Attempts that stopped before the upstream suite

| Gateway profile | Channel | Subset | Last observed failure |
| --- | --- | --- | --- |
| GATEWAY-TLS | experimental | extended | kubeadm timed out creating the administrator ClusterRoleBinding during cluster bootstrap. |
| GATEWAY-UDP | standard | core | The UDP fixture referenced an obsolete selection variable before launching the upstream executable. |
| GATEWAY-UDP | standard | extended | The UDP fixture referenced an obsolete selection variable before launching the upstream executable. |
| GATEWAY-UDP | experimental | core | The UDP fixture referenced an obsolete selection variable before launching the upstream executable. |
| GATEWAY-UDP | experimental | extended | The UDP fixture referenced an obsolete selection variable before launching the upstream executable. |

On Experimental, TLS Core recorded 20 passes and one failure. All 11 traffic subtests in `TLSRouteHostnameIntersection` passed. The suite reached its 30-second deadline waiting to confirm deletion of the Gateway with no listener hostname. [Observation and source log digest](results.json)

The five follow-up attempts produced 5 upstream reports with the same controller and Octavia revisions. The UDP fixture was corrected before running the original selections. [Follow-up results](recovery/README.md)

11 targeted upstream reports on the same controller and Octavia revisions record 16 passes and 1 failure across the selected cases and channels. The Experimental backend TLS conflict case completed its HTTP probes, then timed out while deleting its HTTPRoute. [Targeted case reports](targeted/README.md)

# Gateway profiles on the same controller and Octavia source revisions

These local runs use Gateway API v1.7.0 and the signed Git source revisions for controller `13a6870cb63a` and Octavia `e62a57c49177`. 4 of 22 planned profile, channel and transport combinations finished.

| Gateway profile | Channel | Subset | Listener mode | Upstream pass / fail / skip | Per-case pass / selected | Suite exit | Fixture exit | Upstream report |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| GATEWAY-HTTP | standard | core | default | 39 / 1 / 0 | 39 / 40 | 1 | 1 | [Report](gateway-http-standard-core-default.yaml) |
| GATEWAY-HTTP | standard | extended | default | 39 / 24 / 0 | 39 / 63 | 1 | 1 | [Report](gateway-http-standard-extended-default.yaml) |
| GATEWAY-HTTP | experimental | core | default | 40 / 0 / 0 | 40 / 40 | 0 | 0 | [Report](gateway-http-experimental-core-default.yaml) |
| GATEWAY-HTTP | experimental | extended | default | 40 / 24 / 0 | 40 / 64 | 1 | 1 | [Report](gateway-http-experimental-extended-default.yaml) |

Each row reports the requested subset of its upstream profile. In both Extended-only HTTP reports the separate Core section lists 40 cases skipped by selection. The Extended sections each record zero skips. The per-case count uses the case log and any required nested assertions. Static address allocator events are listed separately from the upstream case verdict. A failure named in the upstream report counts as a failure even if the log parser missed its final line. The results retain the original receipt counts wherever they differ. Selected cases can occur in multiple rows. [Inputs and per-case outcomes](results.json)

The campaign ran against local Kubernetes, Octavia and native HAProxy. Qualification on a deployed Hibiscus region remains open.

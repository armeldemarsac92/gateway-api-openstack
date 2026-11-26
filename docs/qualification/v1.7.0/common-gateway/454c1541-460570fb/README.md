# HTTP Extended with closed-listener policy removal

The Gateway API v1.7.0 GATEWAY-HTTP Extended suite passed 51 of 63 selected cases on Standard and 57 of 64 on Experimental. Each run used the same controller executable and Octavia packages. [Controller source](https://github.com/armeldemarsac92/gateway-api-openstack/commit/454c154167a521956cd043967cf6d27e8755b292) · [Octavia source](https://github.com/armeldemarsac92/octavia/commit/460570fb9060fafcedba2907f7a24dc6ab83e7e8) · [Gateway API source](https://github.com/kubernetes-sigs/gateway-api/tree/e26d183dbbc1bb782b78addbd26faba03955c152)

| Channel | Previous pair, passed | Current pair, passed | Failed | Skipped | Suite / fixture exit | Original report | Measurements |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| Standard | 50/63 | 51/63 | 12 | 0 | 1 / 1 | [YAML](http-standard-extended.yaml) | [Cases and digests](http-standard-extended.json) |
| Experimental | 53/64 | 57/64 | 7 | 0 | 1 / 1 | [YAML](http-experimental-extended.yaml) | [Cases and digests](http-experimental-extended.json) |

On Standard, `HTTPRouteCORS`, `HTTPRouteRedirectPortAndScheme` and `ListenerSetAllowedNamespaceSelector` went from fail to pass. `GatewayInvalidTLSBackendConfiguration` and `GatewayStaticAddresses` went from pass to fail. On Experimental, `BackendTLSPolicyInvalidKind`, `GatewayHTTPListenerIsolation`, `GatewayTLSBackendClientCertificate` and `HTTPRouteRedirectPortAndScheme` went from fail to pass. Every previous Experimental pass also passed on this pair. [Previous runs with default test parallelism](../597d921f-300b2638/README.md)

On Standard, all 32 traffic checks for `GatewayHTTPListenerIsolation` and all 11 for `ListenerSetAllowedRoutesNamespaces` passed. The suite recorded both cases as failures when cleanup exceeded its deadline. On Experimental, the seven failures covered backend TLS policy, SAN validation, frontend client certificate validation and ListenerSet routing or namespace restrictions. Both channels list the failed cases in the original YAML. [Standard report](http-standard-extended.yaml) · [Experimental report](http-experimental-extended.yaml)

## Test environment

The fixture labels both YAML reports `local-octavia-amphora-signed`. GitHub verified the signature on the controller source commit. Both runs used the same locally built controller image, `sha256:aa63196f0ecf38d26dcfcd28b4e298177b009e8cd0684475e99b42a7ace82e19`. Both fixture receipts record `ociSignatureVerified=false` for that image. [Controller commit](https://github.com/armeldemarsac92/gateway-api-openstack/commit/454c154167a521956cd043967cf6d27e8755b292)

Each run used a disposable kind cluster, the upstream conformance executable, a local Octavia 19.0.1.dev112 WSGI service backed by SQLite, Barbican over HTTPS and native HAProxy. The fixture enabled closed-listener policy removal for Amphora. Both runs in the table above used a two-second controller OpenStack resynchronization interval and kept the upstream case deadlines. The Experimental fixture also recorded Kubernetes object changes and TLS routing state. Further regional qualification requires a deployed Hibiscus environment. [Closed-listener behavior](../../provider/closed-listener-policy-removal/README.md) · [Regional qualification](../../live-runs.md)

The original Go log recorded the start of every selected case. Two Standard cases and one Experimental case lacked an individual final Go test line. The suite's YAML counts each as a failure. The measurements preserve both the official verdict and the local parser's counts. [Standard measurements](http-standard-extended.json) · [Experimental measurements](http-experimental-extended.json)

## Standard HTTP Extended with the operator resync default

The Standard run passed 60 of the same 63 GATEWAY-HTTP Extended cases, up from 51. The suite and fixture each exited with 1 in both runs. The selected subset had zero skips. [Reference report](http-standard-extended.yaml) [Operator resync report](http-standard-extended-operator-resync.yaml) [Receipt and source digests](http-standard-extended-operator-resync.json)

Both runs used the same locally built controller image and running manager executable from `454c1541`, the same Octavia and octavia-lib wheels from `460570fb` and `735564bb` respectively, and the same upstream conformance executable. GitHub verified the two source commit signatures. The fixture records `ociSignatureVerified=false` for the controller image. Amphora closed-listener removal stayed enabled. The sole operator setting change was removing `--openstack-resync-interval=2s`. The launcher supplied no `GATEWAY_OPENSTACK_RESYNC_INTERVAL` override. The source and running manager’s help both specify a ten-minute default. [Controller default](https://github.com/armeldemarsac92/gateway-api-openstack/blob/454c154167a521956cd043967cf6d27e8755b292/internal/controller/config.go#L38-L40)

Nine cases went from fail to pass. Every reference pass also passed in the operator resync run.

- `GatewayFrontendClientCertificateValidation`
- `GatewayFrontendClientCertificateValidationInsecureFallback`
- `GatewayHTTPListenerIsolation`
- `GatewayInvalidFrontendClientCertificateValidation`
- `GatewayInvalidTLSBackendConfiguration`
- `GatewayStaticAddresses`
- `GatewayTLSBackendClientCertificate`
- `ListenerSetAllowedRoutesNamespaces`
- `ListenerSetHTTPRouting`

`BackendTLSPolicy`, `BackendTLSPolicyInvalidKind`, and `BackendTLSPolicySANValidation` failed in both runs. All 63 selected cases in the operator resync run had final Go test lines, with 60 passes and 3 failures. The reference had 51 pass lines and 10 fail lines. The reference YAML also counts `GatewayInvalidTLSBackendConfiguration` and `ListenerSetHTTPRouting` as failures. Both started and lacked final Go test lines in the reference log. Each full log has 107 skip lines for cases outside the selected subset.

| Measurement | Two-second override | Ten-minute default |
| --- | ---: | ---: |
| Fixture-measured upstream suite elapsed time | 2,292.40 s | 1,001.56 s |
| Go `TestConformance` duration from the parent test line | 623.55 s | 566.56 s |
| Octavia API GET requests during Extended | 47,991 | 30,827 |
| Native mutations recorded during Extended | 1,619 | 1,706 |
| Native HAProxy reloads recorded during Extended | 412 | 437 |

The Octavia counts cover the Extended phase. Each run also recorded three GET requests during setup. The reference report is dated 07:52:01 UTC on November 26, 2026, and the operator resync report is dated 08:57:42 UTC that day. The original reports preserve their `+01:00` offsets. The activity and elapsed times describe these two local runs. Establishing a reproducible performance effect would require repeated measurements in the same fixture.

# Reproduce the Hibiscus development image

The linux/amd64 controller image reproduced across normal and no-cache builds. In the local Experimental run, the manager and Amphora exported a matching OTLP server span, withdrew export when the collector grant was removed, and resumed export after restoration. Both Gateways continued serving HTTP 200. The Standard baseline also reached both applications. [Build record](image.json) · [Trace lifecycle](provider/gateway-telemetry-evidence/receipt.json) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-9a6a31d2572c34ca878f79e814c4481344f0a77b)

The linux/amd64 Service retry budget image reproduced across normal and no-cache builds. Its executable ran the Experimental policy lifecycle. Once the target Service exhausted its budget, HAProxy returned 503 after one backend attempt. The other Service continued to receive its normal retry. Removing the policy restored retries for the first Service. The Standard run verified ordinary HTTP retries on both routes. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-c3f4f691197ca3187de2de0b04ea6e234d12dca4/image.json) · [Traffic receipt](provider/backend-retry-budget-evidence/receipt.json) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-c3f4f691197ca3187de2de0b04ea6e234d12dca4)

The linux/amd64 controller image reproduced across ordinary and no-cache builds. Its executable ran the Experimental listener authorization grant cycle. The protected hostname returned HTTP 200, 403 and 200. A second hostname on the same port reached its own Service in every settled phase. The Standard run exercised both ordinary HTTP listeners. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-7287dd6140b9009f76120d52121a1412d041cb1d/image.json) · [Traffic receipt](provider/gateway-listener-filters-evidence/receipt.json) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-7287dd6140b9009f76120d52121a1412d041cb1d)

The linux/amd64 frontend validation image reproduced byte for byte. With that executable, the surviving application and its EndpointSlice remained available in the sampled shared-port grant transitions on both channels. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-6aaf2c448232d92f0e746674d60dd542e238e611/image.json) · [Traffic samples](provider/frontend-client-validation-evidence/shared-443.json) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-6aaf2c448232d92f0e746674d60dd542e238e611)

The linux/amd64 Gateway infrastructure image reproduced byte for byte. Its executable passed the registered metadata case and the DNS, HTTP and VIP withdrawal checks on both release channels. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-ae93701ffbdde389ba4fac3172fd5f4e1202629f/image.json) · [Qualification](provider/gateway-infrastructure.md) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-ae93701ffbdde389ba4fac3172fd5f4e1202629f)

The ordinary and independent no-cache linux/amd64 builds of the grouped L7 activation revision produced identical OCI manifests and executables. The extracted executable passed the 36 upstream ListenerSet HTTP assertions on both channels. The initial Experimental attempt reached 34 of 36. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0cde3dcd7e494e3d9986ff3c5650353e5f6a3def/image.json) · [Qualification](provider/listenerset-positive.md#grouped-policy-activation) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-0cde3dcd7e494e3d9986ff3c5650353e5f6a3def)

The ordinary and independent no-cache linux/amd64 builds of the Gateway backend client certificate revision produced the same manifest and executable. The published echo returned the expected client identity on both Gateway API channels. The local builder mounted an empty compiler cache separately for each build. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-7ac426fa239784dccab5a28851dd4d71094d7e04/image.json) · [Backend authentication cases](provider/backend-tls-policy.md#gateway-client-certificate) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-7ac426fa239784dccab5a28851dd4d71094d7e04)

The ordinary and no-cache linux/amd64 builds of the gRPC backend TLS revision produced the same OCI manifest and manager executable. The archived executable passed all three subtests of `BackendTLSPolicyGRPCRoute` on Standard and Experimental. [Previous build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982/image.json) · [gRPC qualification](provider/backend-tls-policy.md#grpc-with-required-backend-http2) · [Release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982) · [Evidence pack](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-51ad8cc6316f6aac929961f70866f88053896982/backendtlspolicy-grpcroute-51ad8cc-evidence.tar.gz)

The ordinary and no-cache linux/amd64 builds produced the same OCI manifest. The previous multiport image passed both original GatewayModifyListeners subtests on Standard and Experimental. Separate traffic probes checked the serving ports during listener and certificate changes. [Build record](https://github.com/armeldemarsac92/gateway-api-openstack/releases/download/hibiscus-dev-0d89da1a8b42d331d130ab11e86903850de7b685/image.json) · [Listener results](provider/http-distinct-ports.md#adding-and-removing-listeners)

On a linux/amd64 host, use a separate checkout of the recorded revision. The Dockerfile pins the Go 1.27.4 builder and distroless base by digest. Podman uses a fixed timestamp, and the build writes the full commit into the binary and the OCI revision label. The verification command exports gzip-compressed layers.

```sh
revision=6aaf2c448232d92f0e746674d60dd542e238e611
test "$(git rev-parse HEAD)" = "$revision"
image="localhost/hibiscus-controller:$revision"
make container-build CONTAINER_TOOL=podman VERSION="$revision" IMAGE="$image"
skopeo copy --format oci --dest-compress-format gzip --dest-compress-level 5 \
  "containers-storage:$image" oci-archive:controller-linux-amd64.oci.tar:hibiscus
skopeo inspect --raw oci-archive:controller-linux-amd64.oci.tar:hibiscus | sha256sum
```

To compare a second build without layer cache, run the following in Bash. The image config ID, uncompressed local storage manifest and archive file checksum identify different objects.

```bash
image_rebuilt="${image}-rebuilt"
podman build --no-cache --timestamp=0 --identity-label=false \
  --build-arg VERSION="$revision" -t "$image_rebuilt" .
skopeo copy --format oci --dest-compress-format gzip --dest-compress-level 5 \
  "containers-storage:$image_rebuilt" oci:rebuilt-oci:hibiscus
cmp <(skopeo inspect --raw oci-archive:controller-linux-amd64.oci.tar:hibiscus) \
    <(skopeo inspect --raw oci:rebuilt-oci:hibiscus)
```

The publication job reads the prerelease archive, checks its manifest digest, platform and source revision, and copies the manifest to GHCR with digest preservation. It reads the result back by digest. Operators promoting the archive to another registry can use the same command. [Publication workflow](../../../.github/workflows/hibiscus-image.yml)

```sh
skopeo copy --preserve-digests oci-archive:controller-linux-amd64.oci.tar:hibiscus \
  "docker://$DESTINATION_REPOSITORY:hibiscus-dev-$revision"
```

# Reproduce the Hibiscus development image

The ordinary and no-cache linux/amd64 builds produced the same OCI manifest. The archived executable passed all 15 traffic checks in the combined port-and-scheme redirect case on Standard and Experimental. [Build record](image.json) · [Redirect results](provider/http-request-redirect.md#combined-port-and-scheme-redirects)

On a linux/amd64 host, use a separate checkout of the recorded revision. The Dockerfile pins the Go 1.27.4 builder and distroless base by digest. Podman uses a fixed timestamp, and the build writes the full commit into the binary and the OCI revision label. The verification command exports gzip-compressed layers.

```sh
revision=7ed5e90f086200258d1aa0f3b6af3244c8655b81
test "$(git rev-parse HEAD)" = "$revision"
image="localhost/hibiscus-controller:$revision"
make container-build CONTAINER_TOOL=podman VERSION="$revision" IMAGE="$image"
skopeo copy --dest-compress-format gzip --dest-compress-level 5 \
  "containers-storage:$image" oci-archive:controller-linux-amd64.oci.tar:hibiscus
skopeo inspect --raw oci-archive:controller-linux-amd64.oci.tar:hibiscus | sha256sum
```

To compare a second build without layer cache, run the following in Bash. The image config ID, uncompressed local storage manifest and archive file checksum identify different objects.

```bash
image_rebuilt="${image}-rebuilt"
podman build --no-cache --timestamp=0 --identity-label=false \
  --build-arg VERSION="$revision" -t "$image_rebuilt" .
skopeo copy --dest-compress-format gzip --dest-compress-level 5 \
  "containers-storage:$image_rebuilt" oci:rebuilt-oci:hibiscus
cmp <(skopeo inspect --raw oci-archive:controller-linux-amd64.oci.tar:hibiscus) \
    <(skopeo inspect --raw oci:rebuilt-oci:hibiscus)
```

The publication job reads the prerelease archive, checks its manifest digest, platform and source revision, and copies the manifest to GHCR with digest preservation. It reads the result back by digest. Operators promoting the archive to another registry can use the same command. [Publication workflow](../../../.github/workflows/hibiscus-image.yml)

```sh
skopeo copy --preserve-digests oci-archive:controller-linux-amd64.oci.tar:hibiscus \
  "docker://$DESTINATION_REPOSITORY:hibiscus-dev-$revision"
```

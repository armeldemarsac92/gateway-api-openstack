# Reproduce the Hibiscus development image

Use the [local build record](image.json) for the exact source revision, linux/amd64 OCI manifest, archive digest and embedded manager digest. The normal and no-cache builds produced the same registry manifest. The OCI archive was built and checked locally.

On a linux/amd64 host, use a separate checkout of the recorded revision. The Dockerfile pins the Go 1.27.4 builder and distroless base by digest. Podman uses a fixed timestamp, and the build writes the full commit into the binary and the OCI revision label. The verification command exports gzip-compressed layers.

```sh
revision=adb6f0fc44dee7d9016bca79442e68f94eb6e15e
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

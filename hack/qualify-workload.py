#!/usr/bin/env python3
"""Bind a qualification pass to its live workload and immutable build input."""

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml


def require(condition, message):
    if not condition:
        raise ValueError(message)


def resource(kind, name, namespace=None):
    for value in [name] + ([namespace] if namespace is not None else []):
        require(isinstance(value, str) and re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", value), "invalid Kubernetes resource name")
    args = ["kubectl", "--request-timeout=30s", "get", kind, name, "-o", "json"]
    if namespace is not None:
        args.extend(["--namespace", namespace])
    return json.loads(subprocess.check_output(args))


def container(items, name):
    return next(item for item in items if item["name"] == name)


def owner(obj, kind):
    return next(ref for ref in obj["metadata"]["ownerReferences"] if ref.get("controller") and ref["kind"] == kind)


def manifest(reference):
    data = subprocess.check_output(["skopeo", "inspect", "--raw", "docker://" + reference])
    require(reference.split("@")[1] == "sha256:" + hashlib.sha256(data).hexdigest(), "registry manifest digest mismatch")
    return json.loads(data)


def running_manifest(image, image_id, node_name):
    """Runtimes report either the pull digest or the image config digest."""
    digest = re.search(r"sha256:[0-9a-f]{64}$", image_id)
    require(digest, "running container has no verifiable image ID")
    repository, root_digest = image.split("@")
    root = manifest(image)
    if "manifests" not in root:
        require(digest[0] in (root_digest, root["config"]["digest"]), "running image differs from the published image")
        return image
    if digest[0] == root_digest:
        node = resource("node", node_name)["status"]["nodeInfo"]
        matches = [child for child in root["manifests"]
                   if child.get("platform", {}).get("os") == node["operatingSystem"]
                   and child["platform"].get("architecture") == node["architecture"]]
        require(len(matches) == 1, "cannot identify the running platform manifest from this node")
        return repository + "@" + matches[0]["digest"]
    for child in root["manifests"]:
        if child.get("platform", {}).get("os") == "unknown":
            continue
        reference = repository + "@" + child["digest"]
        if digest[0] in (child["digest"], manifest(reference).get("config", {}).get("digest")):
            return reference
    raise ValueError("running image is absent from the published platform manifests")


def check(mode, revision):
    inventory = yaml.safe_load(Path("inventory.yaml").read_text())
    options = yaml.safe_load(Path("options.yaml").read_text())
    target = inventory.get(mode + "Workload")
    require(target, "no " + mode + " workload has been designated for this pass")
    expected = inventory["controllerImage"] if mode == "gateway" else target["image"]
    version = revision if mode == "gateway" else target["revision"]
    require(re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", expected), "expected a digest-addressed build")
    require(re.fullmatch(r"[a-f0-9]{40}", version), "expected a full source revision")
    selected_resource = None
    if mode == "gateway" or options.get("meshName"):
        kind, name = (("gatewayclass.gateway.networking.k8s.io", options["gatewayClassName"]) if mode == "gateway"
                      else ("xmesh.gateway.networking.x-k8s.io", options["meshName"]))
        selected = resource(kind, name)
        require(selected["spec"]["controllerName"] == target["controllerName"] and not selected["metadata"].get("deletionTimestamp"),
                "selected API resource belongs to another controller or is terminating")
        selected_resource = dict(kind=kind, name=name, uid=selected["metadata"]["uid"], controllerName=target["controllerName"])

    ns, name = target["namespace"], target["container"]
    deployment = resource("deployment.apps", target["deployment"], ns)
    pod = resource("pod", target["pod"], ns)
    pod_owner = owner(pod, "ReplicaSet")
    replica = resource("replicaset.apps", pod_owner["name"], ns)
    require(replica["metadata"]["uid"] == pod_owner["uid"] and owner(replica, "Deployment")["uid"] == deployment["metadata"]["uid"],
            "selected Pod does not belong to the controller Deployment")
    desired = deployment["spec"]["replicas"]
    status = deployment["status"]
    require(desired > 0 and status["observedGeneration"] == deployment["metadata"]["generation"] and
            all(status.get(key) == desired for key in ("replicas", "updatedReplicas", "availableReplicas")),
            "controller Deployment is still rolling out")
    require(not any(obj["metadata"].get("deletionTimestamp") for obj in (deployment, replica, pod)), "workload is terminating")
    running = container(pod["status"]["containerStatuses"], name)
    require(any(c["type"] == "Ready" and c["status"] == "True" for c in pod["status"].get("conditions", [])),
            "controller Pod is not ready")
    require(running.get("ready") and "running" in running["state"], "controller container is not ready")
    for spec in (deployment["spec"]["template"]["spec"], pod["spec"]):
        require(container(spec["containers"], name)["image"] == expected, "deployed image differs from the build record")
    reference = running_manifest(expected, running["imageID"], pod["spec"]["nodeName"])
    config = json.loads(subprocess.check_output(["skopeo", "inspect", "--config", "docker://" + reference]))
    label = (config["config"].get("Labels") or {}).get("org.opencontainers.image.revision")
    if mode == "gateway" or label is not None:
        require(label == version, "image source revision differs from the build record")
    observed = dict(selectedResource=selected_resource, deploymentUID=deployment["metadata"]["uid"], podUID=pod["metadata"]["uid"], image=expected,
                    imageID=running["imageID"], platformManifest=reference, buildRevision=version, imageRevisionLabel=label)
    Path(mode + "-deployment.json").write_text(json.dumps(observed, indent=2) + "\n")
    if mode == "mesh":
        options["implementation"] = target["implementation"]
    options["implementation"]["version"] = version
    Path(mode + "-options.yaml").write_text(yaml.safe_dump(options))
    Path(mode + "-revision").write_text(version + "\n")


if __name__ == "__main__":
    check(sys.argv[1], sys.argv[2])

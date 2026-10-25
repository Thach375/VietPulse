"""Render the deployment only when actual immutable application images exist."""

import argparse
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DIGEST = re.compile(r"[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}")


def render(objects, images, pull_secret=None):
    if set(images) != {"spark", "crawler"} or any(
        not DIGEST.fullmatch(value or "") for value in images.values()
    ):
        raise ValueError("Supply actual published spark and crawler image references with @sha256 digests")
    if pull_secret and not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", pull_secret):
        raise ValueError("Invalid image pull Secret name")

    def replace(value):
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item) for item in value]
        return {"REQUIRED_SPARK_IMAGE": images["spark"], "REQUIRED_CRAWLER_IMAGE": images["crawler"]}.get(
            value, value
        )

    result = replace(objects)
    for resource in result:
        if resource["kind"] == "SparkApplication":
            for role in ("driver", "executor"):
                resource["spec"][role].setdefault("labels", {}).update(
                    {
                        "vietpulse.io/application": resource["metadata"]["name"],
                        "vietpulse.io/spark-role": role,
                    }
                )
            resource["spec"]["suspend"] = resource["metadata"]["name"] not in (
                "raw-ingest",
                "speed-sentiment",
            )
            if pull_secret:
                resource["spec"]["imagePullSecrets"] = [pull_secret]
        elif resource["kind"] == "CronJob" and pull_secret:
            resource["spec"]["jobTemplate"]["spec"]["template"]["spec"]["imagePullSecrets"] = [
                {"name": pull_secret}
            ]
    result.append(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "vietpulse-image-pins", "namespace": "processing"},
            "data": images,
        }
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--images-file", type=Path, required=True)
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--pull-secret")
    args = parser.parse_args()
    images = yaml.safe_load(args.images_file.read_text())
    # Validate before invoking kubectl; no cluster communication is used here.
    render([], images, args.pull_secret)
    rendered = subprocess.run(
        [args.kubectl, "kustomize", str(ROOT / "k8s/overlays/multi-node")],
        check=True,
        capture_output=True,
        text=True,
    )
    objects = list(yaml.safe_load_all(rendered.stdout))
    print(yaml.safe_dump_all(render(objects, images, args.pull_secret), sort_keys=False), end="")


if __name__ == "__main__":
    main()

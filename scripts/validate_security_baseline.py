#!/usr/bin/env python3
"""Fail closed when the worker-uplift repository security baseline drifts."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
IMMUTABLE_SHA = re.compile(r"[0-9a-f]{40}")
ACTION_USE = re.compile(r"uses:\s*([^\s#]+)@([^\s#]+)")


def main() -> int:
    errors: list[str] = []
    workflows = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))

    for workflow in workflows:
        text = workflow.read_text(encoding="utf-8")
        for action, ref in ACTION_USE.findall(text):
            if action.startswith("./"):
                continue
            if IMMUTABLE_SHA.fullmatch(ref) is None:
                errors.append(f"{workflow.relative_to(ROOT)}: mutable action ref {action}@{ref}")

        if workflow.name.startswith("publish"):
            if re.search(r"(?m)^\s*cache:\s*['\"]?npm['\"]?\s*$", text):
                errors.append(f"{workflow.relative_to(ROOT)}: npm cache restore is forbidden in artifact publication")
            if "actions/cache@" in text:
                errors.append(f"{workflow.relative_to(ROOT)}: action cache restore is forbidden in artifact publication")

    dockerfile = ROOT / "Dockerfile"
    publish_container = WORKFLOWS / "publish-container.yml"

    if dockerfile.exists():
        docker_text = dockerfile.read_text(encoding="utf-8")
        required_docker_fragments = (
            "npm prune --omit=dev --ignore-scripts",
            "rm -rf /usr/local/lib/node_modules/npm",
            "rm -f /usr/local/bin/npm /usr/local/bin/npx",
            "COPY --from=production-dependencies /app/node_modules ./node_modules",
        )
        for fragment in required_docker_fragments:
            if fragment not in docker_text:
                errors.append(f"Dockerfile: missing runtime-minimization control: {fragment}")

        if not publish_container.exists():
            errors.append(".github/workflows/publish-container.yml: missing container publication workflow")
        else:
            publish_text = publish_container.read_text(encoding="utf-8")
            if re.search(r"(?m)^\s*sbom:\s*true\s*$", publish_text) is None:
                errors.append(".github/workflows/publish-container.yml: missing image SBOM attestation")
            if re.search(r"(?m)^\s*provenance:\s*mode=max\s*$", publish_text) is None:
                errors.append(".github/workflows/publish-container.yml: missing maximum provenance attestation")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(f"Security baseline valid: {len(workflows)} workflow files checked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

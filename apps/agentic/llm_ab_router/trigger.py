"""Durable Langfuse rollout triggers for workflow and Recommendation.

Workflow keeps its authenticated webhook and finite Kubernetes dispatch Jobs.
Recommendation uses a one-minute poller and moves ``ab-ready`` through a
parking prompt before it submits Jenkins.  Neither trigger changes serving
routes; uncertain Jenkins submission is reconciled, never blindly retried.
"""
from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
from copy import deepcopy
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import quote

import httpx
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from psycopg.types.json import Jsonb

from .database import DDL as ROUTER_DDL, Database, json_label
from jenkins.python.llm_agent_cd.release import (
    digest,
    managed_backend_binding,
    release,
    validate_experiment,
)
from jenkins.python.llm_agent_cd.state import StateStore
from jenkins.python.llm_agent_cd.workflow import ROLES, diff as workflow_diff
from jenkins.python.model_cd.storage import s3_client

PROMPT_TEXT = "Immutable RecSys agent rollout request. Edit config, then apply ab-ready."
LEGACY_PROMPT_TEXT = "Immutable RecSys workflow rollout request. Edit config, then apply ab-ready."
PARKING_PROMPT_TEXT = "Internal RecSys A/B label parking version. Never dispatch."
PARKING_LABEL = "ab-label-parking"
ACTIVE_LABEL = "ab-running"
READY_LABEL = "ab-ready"
TERMINAL_LABELS = {
    "COMPLETED": "ab-done",
    "REJECTED": "ab-fail",
    "ROLLED_BACK": "ab-fail",
    "ROLLBACK_FAILED": "ab-fail",
}

TARGETS = {
    "workflow": {
        "prefix": "wf",
        "state_env": "AB_WORKFLOW_STATE_URI",
        "state_default": "s3://recsys-llm-ab/workflow/state.json",
        "jenkins_env": "AB_WORKFLOW_JENKINS_JOB",
        "jenkins_default": "RecSys-LLM-Workflow-CD",
        "catalog_prefix": "workflow/catalog/",
        "candidate_prefix": "workflow/candidates/",
        "policy_refs": {
            "workflow-production": "configs/llm-ab/workflow-production-policy.json",
            "workflow-live-test": "configs/llm-ab/workflow-live-test-policy.json",
        },
    },
    "recommendation": {
        "prefix": "rec",
        "state_env": "AB_RECOMMENDATION_STATE_URI",
        "state_default": "s3://recsys-llm-ab/recommendation/state.json",
        "jenkins_env": "AB_RECOMMENDATION_JENKINS_JOB",
        "jenkins_default": "RecSys-LLM-Agent-CD",
        "catalog_prefix": "recommendation/catalog/",
        "candidate_prefix": "recommendation/candidates/",
        "policy_refs": {
            "recommendation-live-test": "configs/llm-ab/recommendation-live-test-policy.json",
        },
    },
}
DDL = """
CREATE SCHEMA IF NOT EXISTS recsys_ab;
CREATE TABLE IF NOT EXISTS recsys_ab.trigger_requests (
 experiment_id text PRIMARY KEY, prompt_name text NOT NULL, prompt_version integer NOT NULL,
 config jsonb NOT NULL, status text NOT NULL DEFAULT 'QUEUED', reason text,
 candidate_uri text, candidate jsonb, config_diff jsonb, job_name text,
 queue_id text, build_url text, submitted_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(prompt_name,prompt_version)
);
CREATE TABLE IF NOT EXISTS recsys_ab.trigger_events (
 event_id text PRIMARY KEY, payload_hash text NOT NULL,
 experiment_id text NOT NULL REFERENCES recsys_ab.trigger_requests,
 created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS prompt_snapshot jsonb;
ALTER TABLE recsys_ab.trigger_events ADD COLUMN IF NOT EXISTS payload jsonb;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS project_id text;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS prompt_checksum text;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS label_state jsonb;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS label_sync_status text;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS label_sync_error text;
ALTER TABLE recsys_ab.trigger_requests ADD COLUMN IF NOT EXISTS labels_updated_at timestamptz;
CREATE UNIQUE INDEX IF NOT EXISTS trigger_prompt_snapshot_identity
  ON recsys_ab.trigger_requests(project_id,prompt_name,prompt_version,prompt_checksum)
  WHERE project_id IS NOT NULL AND prompt_checksum IS NOT NULL;
"""


def polling_mode():
    return (os.environ.get("AB_TRIGGER_MODE") == "poll"
            and os.environ.get("AB_TRIGGER_SCOPE") == "recommendation")


def canonical_prompt_snapshot(project_id, prompt):
    """Exclude mutable Langfuse labels from the immutable request identity."""
    return {
        "project_id": project_id,
        "name": prompt.get("name"),
        "version": prompt.get("version"),
        "type": prompt.get("type", "text"),
        "prompt": prompt.get("prompt"),
        "config": prompt.get("config"),
    }


def prompt_checksum(project_id, prompt):
    return digest(canonical_prompt_snapshot(project_id, prompt))


def signature_valid(raw, signature, secret):
    try:
        fields = dict(part.strip().split("=", 1) for part in signature.split(","))
        timestamp = int(fields["t"])
        expected = hmac.new(secret.encode(), fields["t"].encode() + b"." + raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, fields["v1"]), timestamp
    except (ValueError, KeyError):
        return False, 0


def target(scope):
    if scope not in TARGETS:
        raise ValueError("invalid rollout scope")
    return TARGETS[scope]


def scope_from_experiment(experiment_id):
    matches = [scope for scope, item in TARGETS.items()
               if re.fullmatch(item["prefix"] + r"-[0-9a-f]{32}", experiment_id)]
    if len(matches) != 1:
        raise ValueError("invalid experiment ID")
    return matches[0]


def state_uri(scope):
    item = target(scope)
    # AB_STATE_URI is retained as the workflow deployment's legacy setting.
    legacy = os.environ.get("AB_STATE_URI") if scope == "workflow" else None
    return os.environ.get(item["state_env"]) or legacy or item["state_default"]


def jenkins_job(scope):
    item = target(scope)
    legacy = os.environ.get("AB_JENKINS_JOB") if scope == "workflow" else None
    return os.environ.get(item["jenkins_env"]) or legacy or item["jenkins_default"]


def configured_experiment_pattern():
    """Limit a dedicated receiver/recovery loop to its authorized scope."""
    scope = os.environ.get("AB_TRIGGER_SCOPE")
    return target(scope)["prefix"] + "-%" if scope else "%"


def baseline_field(scope):
    return "baseline_workflow_release_id" if scope == "workflow" else "baseline_release_id"


def generation_field(scope):
    return "global_generation" if scope == "workflow" else "generation"


def validate_config(c, allow_live_test=None):
    if not isinstance(c, dict) or c.get("scope") not in TARGETS:
        raise ValueError("invalid rollout request schema")
    scope = c["scope"]
    required = {"schema_version", "scope", baseline_field(scope), generation_field(scope),
                "llm_release_ref", "experiment_type", "policy_ref"}
    allowed = required | ({"target_role"} if scope == "workflow" else set())
    if (not required <= set(c) or set(c) - allowed or c["schema_version"] != 1):
        raise ValueError("invalid " + scope + " request schema")
    if not re.fullmatch(r"[0-9a-f]{64}", c[baseline_field(scope)]):
        raise ValueError("invalid baseline ID")
    if not re.fullmatch(r"[0-9a-f]{64}", c["llm_release_ref"]):
        raise ValueError("LLM reference must be a content-addressed catalog ID")
    if c["experiment_type"] not in {"config_only", "llm_only", "combined"}:
        raise ValueError("invalid experiment type")
    if scope == "workflow" and c.get("target_role", "workflow") not in {"workflow", "coordinator"}:
        raise ValueError("invalid experiment target role")
    if scope == "workflow" and c.get("target_role") == "coordinator" and c["experiment_type"] != "llm_only":
        raise ValueError("coordinator target currently supports llm_only")
    if c["policy_ref"] not in target(scope)["policy_refs"]:
        raise ValueError("unknown policy")
    if (c["policy_ref"].endswith("live-test")
            and allow_live_test is not True
            and os.environ.get("AB_ALLOW_LIVE_TEST") != "true"):
        raise ValueError("live-test policy not enabled")
    if not isinstance(c[generation_field(scope)], dict):
        raise ValueError("generation must be an object")
    return c


def candidate_from_config(baseline, config, llm, namespace="kagent", allow_live_test=None):
    c = validate_config(config, allow_live_test=allow_live_test)
    baseline = release(baseline)
    scope = c["scope"]
    if (baseline.get("scope") == "workflow") != (scope == "workflow"):
        raise ValueError("baseline scope mismatch")
    if baseline["release_id"] != c[baseline_field(scope)]:
        raise ValueError("STALE_BASELINE: explicit new version required")
    if digest(llm) != c["llm_release_ref"]:
        raise ValueError("catalog identity mismatch")
    if scope == "recommendation":
        candidate = {key: deepcopy(baseline[key]) for key in
                     ("config", "llm", "agent", "binding")}
        candidate.update(config=deepcopy(c["generation"]), llm=deepcopy(llm))
        if digest(llm) != baseline["llm_version_id"]:
            llm_id = digest(llm)
            candidate["binding"] = managed_backend_binding(
                candidate["binding"], llm_id, namespace
            )
        candidate = release(candidate)
        validate_experiment(baseline, candidate, c["experiment_type"])
        return candidate
    candidate = {k: deepcopy(baseline[k]) for k in (
        "schema_version", "scope", "global_generation", "agent_overrides", "llm", "agents", "bindings")}
    for optional in ("llm_overrides", "change_scope"):
        if optional in baseline:
            candidate[optional] = deepcopy(baseline[optional])
    if "runtime" in baseline:
        candidate["runtime"] = deepcopy(baseline["runtime"])
    candidate.update(global_generation=deepcopy(c["global_generation"]), llm=deepcopy(llm))
    target_role = c.get("target_role", "workflow")
    if target_role == "coordinator":
        from jenkins.python.llm_agent_cd.workflow import role_llms
        frozen = role_llms(baseline)
        candidate["change_scope"] = "coordinator"
        candidate["llm_overrides"] = {
            role: frozen[role] for role in ROLES if role != "coordinator"
        }
    if digest(llm) != baseline["llm_version_id"]:
        host = "rec-llm-" + digest(llm)[:20] + "." + namespace + ".svc.cluster.local"
        changed_roles = ("coordinator",) if target_role == "coordinator" else ROLES
        for role in changed_roles:
            b = candidate["bindings"][role]
            b.update(managed_backend=True, backend_url="http://" + host + ":8000/v1", default_headers={})
            b.pop("health_url", None)
            b.pop("attestation_configmap", None)
            b["allowed_domains"] = sorted(set(b["allowed_domains"]) | {host})
    candidate = release(candidate)
    validate_experiment(baseline, candidate, c["experiment_type"])
    return candidate


def release_diff(a, b):
    if a.get("scope") == "workflow":
        return workflow_diff(a, b)
    return {
        "recommendation": {
            "before": a["config"],
            "after": b["config"],
            "changed": a["config_id"] != b["config_id"],
            "llm_changed": a["llm_version_id"] != b["llm_version_id"],
        }
    }


def jenkins_parameters(scope, config, candidate_uri, current_state_uri=None):
    item = target(scope)
    params = {
        "ACTION": "run",
        "EXPERIMENT_TYPE": config["experiment_type"],
        "CANDIDATE_MANIFEST": candidate_uri,
        "BASELINE_RELEASE_ID": config[baseline_field(scope)],
        "POLICY": item["policy_refs"][config["policy_ref"]],
    }
    if scope == "recommendation":
        params.update({
            "STATE_URI": current_state_uri or state_uri(scope),
            "FIXTURES": "configs/llm-ab/cases.json",
            "ROUTER_IMAGE": os.environ.get("AB_RECOMMENDATION_ROUTER_IMAGE")
            or os.environ["AB_DISPATCH_IMAGE"],
            "SOURCE_MODE": "deployed-image",
        })
    return params


def dispatch_job(experiment_id):
    scope = scope_from_experiment(experiment_id)
    if scope != "workflow":
        raise ValueError("Recommendation polling dispatches Jenkins directly")
    image = os.environ["AB_DISPATCH_IMAGE"]
    if not re.fullmatch(r".+@sha256:[0-9a-f]{64}", image):
        raise ValueError("dispatch image must be pinned")
    overlay = os.environ.get("AB_PROFILE_OVERLAY_CONFIGMAP", "")
    if overlay and not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", overlay):
        raise ValueError("invalid trigger source overlay")
    container = {"name": "dispatch", "image": image,
        "command": ["python", "-m", "apps.agentic.llm_ab_router.trigger", "dispatch", experiment_id],
        "envFrom": [{"secretRef": {"name": os.environ.get("AB_TRIGGER_SECRET", "recsys-workflow-trigger")}}],
        # Values injected into the receiver Deployment are not inherited by a
        # Kubernetes Job. Pin both images explicitly so the submitted Jenkins
        # parameters are reproducible and never fall back to a mutable tag.
        "env": [
            {"name": "AB_DISPATCH_IMAGE", "value": image},
            {"name": "AB_RECOMMENDATION_ROUTER_IMAGE", "value":
             os.environ.get("AB_RECOMMENDATION_ROUTER_IMAGE") or image},
        ],
        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}}}
    pod = {"restartPolicy": "Never", "automountServiceAccountToken": False,
        "nodeSelector": {"recsys.ai/pool": "ml-system"},
        "tolerations": [{"key": "recsys.ai/workload", "operator": "Equal", "value": "ml-system", "effect": "NoSchedule"}],
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [container]}
    if overlay:
        container["volumeMounts"] = [
            {"name": "trigger-code", "mountPath": "/app/apps/agentic/llm_ab_router/trigger.py", "subPath": "trigger.py", "readOnly": True},
            {"name": "trigger-code", "mountPath": "/app/jenkins/python/llm_agent_cd/release.py", "subPath": "release.py", "readOnly": True},
            {"name": "trigger-code", "mountPath": "/app/jenkins/python/llm_agent_cd/workflow.py", "subPath": "workflow.py", "readOnly": True},
            {"name": "trigger-code", "mountPath": "/app/jenkins/python/llm_agent_cd/serving_profiles.py", "subPath": "serving_profiles.py", "readOnly": True},
        ]
        pod["volumes"] = [{"name": "trigger-code", "configMap": {"name": overlay}}]
    app = "recsys-workflow-dispatch"
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
        "name": "ab-dispatch-" + experiment_id, "labels": {"app": app}},
        "spec": {"backoffLimit": 2, "activeDeadlineSeconds": 300, "ttlSecondsAfterFinished": 86400,
        "template": {"metadata": {"labels": {"app": app},
                                   "annotations": {"sidecar.istio.io/inject": "false"}},
        "spec": pod}}}


def dispatch_job_matches(existing, desired):
    """Kubernetes defaults are allowed; an unrelated Job collision is not success."""
    from jenkins.python.llm_agent_cd.driver import contains_spec
    if existing.get("metadata", {}).get("deletionTimestamp"):
        return False
    return contains_spec(existing.get("metadata", {}).get("labels"), desired["metadata"]["labels"]) and contains_spec(
        existing.get("spec"), desired["spec"])

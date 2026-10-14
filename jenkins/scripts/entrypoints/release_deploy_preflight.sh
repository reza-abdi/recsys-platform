#!/usr/bin/env bash
set -euo pipefail

plan_path="${1:-.ci-release-plan.json}"
[[ -s "${plan_path}" ]] || {
  printf 'release plan does not exist: %s\n' "${plan_path}" >&2
  exit 2
}

source jenkins/scripts/lib/release_gate.sh

release_verify_publish_inputs "${plan_path}"
if release_has_agent_registry_units "${plan_path}"; then
  release_validate_agent_registry_lock "${plan_path}"
fi
git rev-parse HEAD >.ci-deploy/preflight-commit

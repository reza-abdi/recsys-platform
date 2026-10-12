from __future__ import annotations

import asyncio
import hmac
import os
import time
import uuid
import json
from copy import deepcopy
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from jenkins.python.llm_agent_cd.evidence import inspect, task_of
from jenkins.python.llm_agent_cd.release import digest
from jenkins.python.llm_agent_cd.state import StateStore

from .database import Database, json_label
from .telemetry import configure, invocation


def render_workflow_result(payload, context_tool, recommendation_tool, *,
                           child_reader=None, output_profile="a2a-results-v1"):
    """Render verified workflow data without another model/tool execution.

    v1 accepts the specialist's A2A result string. v2 deliberately ignores
    that model-authored final string and reads the already-completed child
    task's sole native tool response through the authenticated TaskStore.
    """
    if output_profile not in {"a2a-results-v1", "trusted-child-tool-results-v2"}:
        return payload, False
    task = task_of(payload)
    if task.get("status", {}).get("state") not in {
        "completed", "TASK_STATE_COMPLETED"
    }:
        return payload, False
    from jenkins.python.llm_agent_cd.workflow_evidence import business_json, events
    try:
        calls = events(task, "function_call")
        responses = events(task, "function_response")
    except ValueError:
        return payload, False
    names = [call.get("name") for call in calls]
    if names not in ([recommendation_tool], [context_tool],
                     [recommendation_tool, context_tool]):
        return payload, False
    if len(responses) != len(calls) or {
        call.get("id") for call in calls
    } != {response.get("id") for response in responses}:
        return payload, False
    values = []
    for call in calls:
        response = next(item for item in responses if item["id"] == call["id"])
        wire = response.get("response")
        if (not isinstance(wire, dict) or wire.get("error")
                or wire.get("isError") is True):
            return payload, False
        if output_profile == "trusted-child-tool-results-v2":
            session_id = wire.get("subagent_session_id")
            if child_reader is None or not session_id:
                return payload, False
            try:
                from jenkins.python.llm_agent_cd.workflow_evidence import tool_value
                child = task_of(child_reader(session_id))
                child_calls = events(child, "function_call")
                child_responses = events(child, "function_response")
                if (child.get("status", {}).get("state") not in {
                        "completed", "TASK_STATE_COMPLETED"}
                        or len(child_calls) != 1 or len(child_responses) != 1
                        or child_calls[0].get("id") != child_responses[0].get("id")):
                    return payload, False
                value = tool_value(child_responses[0].get("response"))
            except Exception:
                # TaskStore/read failures are missing evidence, never
                # permission to replay the A2A or dependency call.
                return payload, False
        else:
            if not isinstance(wire.get("result"), str):
                return payload, False
            try:
                value = json.loads(wire["result"])
            except (TypeError, ValueError):
                return payload, False
            if not isinstance(value, dict):
                return payload, False
        # FastMCP's dict return schema has one transport-only output envelope.
        # Strip exactly that shape; never flatten a business object that has
        # any sibling field or a non-object output value.
        values.append(business_json(value))
    rendered = (values[0] if len(values) == 1 else
                {"recommendation": values[0], "context": values[1]})
    result = deepcopy(payload)
    rendered_task = task_of(result)
    text_parts = [part for artifact in rendered_task.get("artifacts", [])
                  for part in artifact.get("parts", []) if "text" in part]
    if not text_parts:
        return payload, False
    for part in text_parts:
        part["text"] = ""
    text_parts[-1]["text"] = json.dumps(
        rendered, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    text_parts[-1].setdefault("metadata", {})["runtime_rendered"] = True
    rendered_task.setdefault("metadata", {})["runtime_rendered"] = True
    rendered_task["metadata"]["runtime_output_profile"] = output_profile
    return result, True


def render_recommendation_result(
    payload,
    output_profile="trusted-tool-result-v1",
    *,
    allow_inflight_terminal=False,
):
    """Render one verified native Recommendation tool response as final JSON."""
    if output_profile != "trusted-tool-result-v1":
        return payload, False
    task = task_of(payload)
    state = task.get("status", {}).get("state")
    if state not in {"completed", "TASK_STATE_COMPLETED"} and not (
        allow_inflight_terminal
        and state
        in {
            "submitted",
            "working",
            "canceled",
            "cancelled",
            "TASK_STATE_SUBMITTED",
            "TASK_STATE_WORKING",
            "TASK_STATE_CANCELED",
        }
    ):
        return payload, False
    from jenkins.python.llm_agent_cd.recommendation_evaluation import _events
    from jenkins.python.llm_agent_cd.evidence import TOOL, recommendation

    calls = _events(task, "function_call")
    responses = _events(task, "function_response")
    if (
        len(calls) != 1
        or len(responses) != 1
        or calls[0].get("name") != TOOL
        or responses[0].get("name") != TOOL
        or not calls[0].get("id")
        or calls[0].get("id") != responses[0].get("id")
    ):
        return payload, False
    rendered = recommendation(responses[0].get("response"))
    if rendered is None:
        return payload, False
    result = deepcopy(payload)
    rendered_task = task_of(result)
    text_parts = [
        part
        for artifact in rendered_task.get("artifacts", [])
        for part in artifact.get("parts", [])
        if "text" in part
    ]
    if not text_parts:
        text_parts = [
            part
            for part in rendered_task.get("status", {}).get("message", {}).get("parts", [])
            if "text" in part
        ]
    if not text_parts:
        text_parts = next(
            (
                [part for part in message.get("parts", []) if "text" in part]
                for message in reversed(rendered_task.get("history", []))
                if message.get("role") in {"agent", "ROLE_AGENT"}
                and any("text" in part for part in message.get("parts", []))
            ),
            [],
        )
    if not text_parts:
        artifact = {
            "artifactId": "runtime-"
            + digest([rendered_task.get("id"), calls[0]["id"]])[:24],
            "parts": [{"text": ""}],
            "metadata": {"runtime_rendered": True},
        }
        rendered_task.setdefault("artifacts", []).append(artifact)
        text_parts = artifact["parts"]
    for part in text_parts:
        part["text"] = ""
    text_parts[-1]["text"] = json.dumps(
        rendered, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    text_parts[-1].setdefault("metadata", {})["runtime_rendered"] = True
    rendered_task.setdefault("metadata", {})["runtime_rendered"] = True
    rendered_task["metadata"]["runtime_output_profile"] = output_profile
    if allow_inflight_terminal:
        # The streaming transport is closed immediately after the first
        # complete native tool result. Present that verified result as the
        # terminal A2A response; no model or dependency call is replayed.
        rendered_task.setdefault("status", {})["state"] = "TASK_STATE_COMPLETED"
        rendered_task["metadata"]["native_stream_closed_after_tool_result"] = True
    return result, True


def _merge_recommendation_stream_event(task, result):
    """Fold one A2A SSE result into the task snapshot used for verification."""
    if isinstance(result.get("task"), dict):
        return deepcopy(result["task"])

    # kagent 0.10.0-rc1 proxies the pinned A2A v1 wire shape unchanged.  Its
    # stream events are the result object itself (``kind: status-update`` /
    # ``kind: artifact-update``), while newer A2A bindings wrap those objects
    # in ``statusUpdate`` / ``artifactUpdate``.  Accept both native shapes but
    # always fold them into one task snapshot before evaluating tool evidence.
    if result.get("kind") == "task" and result.get("id"):
        return deepcopy(result)

    legacy_status = result if result.get("kind") == "status-update" else None
    legacy_artifact = result if result.get("kind") == "artifact-update" else None
    update = result.get("statusUpdate") or legacy_status
    event_task_id = (update or legacy_artifact or {}).get("taskId")
    event_context_id = (update or legacy_artifact or {}).get("contextId")
    if not isinstance(task, dict):
        if not event_task_id:
            return task
        task = {
            "id": event_task_id,
            "contextId": event_context_id,
            "status": {},
            "history": [],
            "artifacts": [],
        }
    if event_context_id and task.get("contextId") not in {None, event_context_id}:
        raise ValueError("A2A stream context id changed")
    if event_context_id:
        task["contextId"] = event_context_id

    if isinstance(update, dict):
        if update.get("taskId") and update["taskId"] != task.get("id"):
            raise ValueError("A2A stream task id changed")
        task["status"] = deepcopy(update.get("status", task.get("status", {})))
        if isinstance(update.get("metadata"), dict):
            task.setdefault("metadata", {}).update(deepcopy(update["metadata"]))
        message = task.get("status", {}).get("message")
        if isinstance(message, dict):
            history = task.setdefault("history", [])
            message_id = message.get("messageId")
            if not message_id or all(
                item.get("messageId") != message_id for item in history
            ):
                history.append(deepcopy(message))

    update = result.get("artifactUpdate") or legacy_artifact
    if isinstance(update, dict) and isinstance(update.get("artifact"), dict):
        if update.get("taskId") and update["taskId"] != task.get("id"):
            raise ValueError("A2A stream task id changed")
        artifact = deepcopy(update["artifact"])
        artifacts = task.setdefault("artifacts", [])
        prior = next(
            (item for item in artifacts if item.get("artifactId") == artifact.get("artifactId")),
            None,
        )
        if prior is not None and update.get("append"):
            prior.setdefault("parts", []).extend(artifact.get("parts", []))
            prior.update({k: v for k, v in artifact.items() if k != "parts"})
        elif prior is not None:
            artifacts[artifacts.index(prior)] = artifact
        else:
            artifacts.append(artifact)
    return task


def _stream_recommendation_result(
    http, upstream, body, upstream_headers, timeout, output_profile
):
    """Read native A2A events until one verified tool response, then close SSE.

    kagent's Substrate transport routes by the message contextId. A2A v1
    GetTask/CancelTask requests do not carry that field, so polling them through
    the controller cannot address a session actor. SendStreamingMessage keeps
    the original message and context on the one native execution. Closing the
    stream cancels that execution context; it never sends another agent/tool
    request.
    """
    request = deepcopy(body)
    # Use the JSON-RPC method emitted by kagent's own pinned UI client.  The
    # controller accepts aliases today, but ``message/stream`` is the native
    # A2A v1 wire contract for this release.
    request["method"] = "message/stream"
    params = request.setdefault("params", {})
    params.pop("configuration", None)
    message = params.get("message")
    if isinstance(message, dict):
        # Router-facing callers still use the older protobuf-style enum names,
        # while kagent's pinned JSON-RPC v1 binding expects lowercase roles.
        # This is a wire-format conversion only; content and identity stay
        # unchanged.
        message["role"] = {
            "ROLE_USER": "user",
            "ROLE_AGENT": "agent",
        }.get(message.get("role"), message.get("role"))
    # ``A2A-Version: 1.0`` selects kagent's newer protocol negotiation path,
    # whose method names are different from this pinned JSON-RPC endpoint. The
    # adapter owns this internal hop, so it must not forward the external
    # version hint into the native v1 SandboxAgent transport.
    native_headers = {
        key: value for key, value in upstream_headers.items()
        if key.lower() != "a2a-version"
    }
    deadline = time.monotonic() + timeout
    task = None
    with http.stream(
        "POST",
        upstream,
        json=request,
        headers={**native_headers, "accept": "text/event-stream"},
        timeout=timeout,
    ) as response:
        response.raise_for_status()
        if "text/event-stream" not in response.headers.get("content-type", ""):
            raise ValueError("native Recommendation endpoint did not return SSE")
        for line in response.iter_lines():
            if time.monotonic() >= deadline:
                raise HTTPException(504, "task incomplete; do not replay")
            if not line.startswith("data:"):
                continue
            event = json.loads(line[5:].strip())
            if event.get("error"):
                return event
            result = event.get("result", {})
            task = _merge_recommendation_stream_event(task, result)
            if not isinstance(task, dict):
                continue
            payload = {
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "result": {"task": task},
            }
            rendered, ready = render_recommendation_result(
                payload, output_profile, allow_inflight_terminal=True
            )
            if ready:
                return rendered
            if task.get("status", {}).get("state") in {
                "completed",
                "failed",
                "canceled",
                "cancelled",
                "rejected",
                "input-required",
                "auth-required",
                "TASK_STATE_COMPLETED",
                "TASK_STATE_FAILED",
                "TASK_STATE_CANCELED",
                "TASK_STATE_REJECTED",
                "TASK_STATE_INPUT_REQUIRED",
                "TASK_STATE_AUTH_REQUIRED",
            }:
                return payload
    raise HTTPException(504, "task stream ended without terminal evidence; do not replay")


def create_app(mode=None, database=None, store=None, client=None):
    mode = mode or os.environ.get("MODE", "router")
    scope = os.environ.get("AB_SCOPE", "recommendation")
    if scope not in {"recommendation", "workflow"}:
        raise ValueError("unsupported router scope")
    token = os.environ.get("AB_INTERNAL_TOKEN", "")
    if not token:
        raise ValueError("AB_INTERNAL_TOKEN is required")
    test_session_ttl = int(os.environ.get("AB_TEST_SESSION_TTL_SECONDS", "86400"))
    if not 3600 <= test_session_ttl <= 604800:
        raise ValueError("AB_TEST_SESSION_TTL_SECONDS must be between 1 hour and 7 days")
    if mode == "edge" and (
        len(os.environ.get("AB_CASE_TICKET_KEY", "").encode("utf-8")) < 32
        or not os.environ.get("AB_EDGE_REVISION")
        or not os.environ.get("AB_ROUTER_URL")
    ):
        raise ValueError("edge ticket key, revision and private router URL are required")
    db = database or (
        Database(os.environ["AB_DATABASE_URL"]) if mode in {"router", "edge"} else None
    )
    state_store = store or (
        StateStore(os.environ["AB_STATE_URI"]) if mode != "facade" else None
    )
    http = client or httpx.Client(
        timeout=600, follow_redirects=False, transport=httpx.HTTPTransport(retries=0)
    )

    @asynccontextmanager
    async def lifespan(app):
        configure()

        async def heartbeat():
            emitted = set()
            projected_archives = set()
            experiment = None

            def beat():
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO recsys_ab.heartbeat(instance) VALUES (%s) ON CONFLICT(instance) DO UPDATE SET seen_at=now()",
                        (os.environ.get("HOSTNAME", "router"),),
                    )

            while True:
                # Router liveness is independent from optional dashboard/log
                # projection. A newly catalogued immutable release must not
                # make production telemetry stale merely because an older
                # projection path cannot render its diff yet.
                try:
                    await asyncio.to_thread(beat)
                except Exception:
                    pass  # Missing heartbeat is a HOLD; never fabricate it.
                try:
                    snapshot, _ = await asyncio.to_thread(state_store.read)
                    from .workflow_events import snapshot_events
                    if experiment != snapshot.get("experiment_id"):
                        emitted.clear()
                        experiment = snapshot.get("experiment_id")
                    for event in snapshot_events(snapshot):
                        if event["event_id"] not in emitted:
                            print(json.dumps(event), flush=True)
                            emitted.add(event["event_id"])
                    # Re-project durable historical evidence after a restart,
                    # without running monitor gates or reading LLM/tool APIs.
                    pending = [r for r in snapshot.get("history", []) if r["checksum"] not in projected_archives]
                    for reference in pending[:5]:
                        archived = await asyncio.to_thread(state_store.read_archive, reference)
                        for event in snapshot_events(archived):
                            print(json.dumps(event), flush=True)
                        projected_archives.add(reference["checksum"])

                except Exception:
                    # Projection is evidence-only and at-least-once. It cannot
                    # suppress the independently recorded router heartbeat.
                    pass
                await asyncio.sleep(30)

        if db:
            db.migrate()
        # Only the private router projects state/history and represents serving
        # telemetry freshness. The public edge has DB health but must not
        # masquerade as a healthy model-routing data plane.
        task = asyncio.create_task(heartbeat()) if mode == "router" else None
        yield
        if task:
            task.cancel()
        http.close()

    app = FastAPI(lifespan=lifespan)

    def auth(request):
        if not hmac.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token
        ):
            raise HTTPException(403, "internal caller required")

    def headers(request):
        # Never trust client-supplied routing/source/experiment headers.
        return {
            **{
                k: request.headers[k]
                for k in ("traceparent", "tracestate", "x-user-id", "a2a-version")
                if k in request.headers
            },
            "authorization": "Bearer " + token,
            "content-type": "application/json",
        }

    def edge_headers(request, traffic_kind="synthetic_case"):
        # NGINX authenticates the caller. Never forward Basic Auth, user/source,
        # release, variant or experiment headers supplied at the public edge.
        result = {
            **{
                key: request.headers[key]
                for key in ("traceparent", "tracestate", "a2a-version")
                if key in request.headers
            },
            "authorization": "Bearer " + token,
            "content-type": "application/json",
            "x-recsys-source": (
                "live_test" if traffic_kind == "live_test" else "synthetic"
            ),
            "x-user-id": (
                "llm-ab-live" if traffic_kind == "live_test" else "llm-ab-suite"
            ),
            "a2a-version": request.headers.get("a2a-version", "1.0"),
        }
        if traffic_kind == "live_test":
            result["x-recsys-test-token"] = os.environ["AB_LIVE_TEST_TOKEN"]
        return result

    @app.get("/healthz")
    def health():
        if state_store:
            state_store.read()
        if db:
            with db.connect() as c:
                c.execute("SELECT 1")
        return {"ok": True}

    @app.get("/.well-known/agent-card.json")
    def card():
        return {
            "name": "Workflow release router" if os.environ.get("AB_SCOPE") == "workflow" else "Recommendation release router",
            "description": "Stable, immutable release A2A entrypoint",
            "url": os.environ.get(
                "PUBLIC_A2A_URL",
                "http://recsys-ab-router.kagent.svc.cluster.local/",
            ),
            "version": "1.0.0",
            "protocolVersion": "1.0",
            "capabilities": {"streaming": False},
            "defaultInputModes": ["text/plain"],
            "defaultOutputModes": ["text/plain"],
            "skills": [
                {
                    "id": "personalized-recommendations",
                    "name": "Recommendations",
                    "description": "Present model-ranked recommendations",
                    "tags": ["recommendation"],
                }
            ],
        }

    @app.get("/allocate")
    @app.get("/identity")
    def allocate(request: Request):
        auth(request)
        if mode != "adapter":
            raise HTTPException(404)
        state, _ = state_store.read()
        rid = os.environ["RELEASE_ID"]
        if rid in state.get("disabled", []):
            raise HTTPException(409, "release quarantined; start a new session")
        return {"release_id": rid}

    @app.get("/internal/verify")
    def verify(request: Request, release_id: str):
        auth(request)
        if mode != "router":
            raise HTTPException(404)
        response = http.get(
            os.environ["AB_GATEWAY_URL"].rstrip("/") + "/identity",
            headers={**headers(request), "x-recsys-release": release_id},
        )
        return JSONResponse(response.json(), status_code=response.status_code)

    @app.get("/internal/experiments/{eid}/evidence")
    def experiment_evidence(eid: str, request: Request):
        auth(request)
        if mode != "router" or scope != "workflow":
            raise HTTPException(404)
        snapshot, _ = state_store.read()
        if snapshot.get("experiment_id", "baseline") != eid:
            reference = next((r for r in reversed(snapshot.get("history", [])) if r["experiment_id"] == eid), None)
            if reference is None:
                raise HTTPException(404, "unknown workflow experiment")
            snapshot = state_store.read_archive(reference)
        from .workflow_events import snapshot_events
        return {"experiment_id": eid, "phase": snapshot["phase"],
                "gate": snapshot.get("gate"), "policy_checksum": snapshot.get("policy_checksum"),
                "gate_windows": snapshot.get("gate_windows", []), "events": snapshot_events(snapshot),
                "post_promotion_monitoring": False}


    @app.post("/a2a/recommendation-ab/v1")
    async def public_recommendation_ab(request: Request):
        if mode != "edge" or scope != "recommendation":
            raise HTTPException(404)
        if int(request.headers.get("content-length", "0") or 0) > 262_144:
            raise HTTPException(413, "request is too large")
        from .tickets import bind_body, verify

        secret = os.environ.get("AB_CASE_TICKET_KEY", "")
        if len(secret.encode("utf-8")) < 32:
            raise HTTPException(503, "ticket verifier is unavailable")
        try:
            claims = verify(request.headers.get("x-recsys-ab-ticket", ""), secret)
        except ValueError:
            raise HTTPException(403, "invalid A/B ticket")
        traffic_kind = claims.get("traffic_kind", "synthetic_case")
        try:
            # Claim before body/state validation: a valid ticket presented with
            # a wrong request is consumed and cannot later be retried correctly.
            if traffic_kind == "live_test":
                db.claim_live_ticket(claims)
            else:
                db.claim_external_ticket(claims)
        except ValueError:
            raise HTTPException(409, "ticket already used or not dispatchable")
        try:
            body = await request.json()
            prompt = bind_body(claims, body)
            snapshot, _ = state_store.read()
            if traffic_kind == "live_test":
                if (
                    snapshot.get("phase") != claims.get("phase")
                    or snapshot.get("phase") not in {"CANARY", "AB", "VERIFY"}
                    or snapshot.get("experiment_id") != claims["experiment_id"]
                ):
                    raise ValueError("live ticket does not match active phase")
            else:
                fixture = next(
                    (row for row in snapshot.get("fixtures", []) if row["id"] == claims["case_id"]),
                    None,
                )
                if (
                    snapshot.get("phase") != "AB"
                    or snapshot.get("experiment_id") != claims["experiment_id"]
                    or snapshot.get("fixture_checksum") != claims["fixture_checksum"]
                    or not fixture
                    or fixture.get("prompt") != prompt
                ):
                    raise ValueError("ticket does not match active immutable suite")
        except Exception as exc:
            if traffic_kind == "live_test":
                db.finish_live_ticket(
                    claims["request_id"], "REJECTED", http_status=403,
                    last_error=type(exc).__name__,
                )
            else:
                db.finish_external_ticket(
                    claims["experiment_id"], claims["case_id"], "REJECTED",
                    http_status=403, last_error=type(exc).__name__,
                )
            raise HTTPException(403, "ticket does not match the active suite")

        def forward_public_case():
            revision = os.environ.get("AB_EDGE_REVISION", "")
            if not revision:
                raise RuntimeError("edge revision is not configured")
            started = time.monotonic()
            try:
                response = http.post(
                    os.environ["AB_ROUTER_URL"].rstrip("/") + "/",
                    json=body,
                    headers=edge_headers(request, traffic_kind),
                )
                payload = response.json()
                if response.status_code != 200 or payload.get("id") != claims["request_id"]:
                    raise RuntimeError("private router response is ambiguous")
            except Exception as exc:
                if traffic_kind == "live_test":
                    db.finish_live_ticket(
                        claims["request_id"], "AMBIGUOUS",
                        http_status=getattr(locals().get("response"), "status_code", None),
                        last_error=type(exc).__name__,
                    )
                else:
                    db.finish_external_ticket(
                        claims["experiment_id"], claims["case_id"], "AMBIGUOUS",
                        http_status=getattr(locals().get("response"), "status_code", None),
                        last_error=type(exc).__name__,
                    )
                raise HTTPException(502, "private execution result is ambiguous")
            if traffic_kind == "live_test":
                db.finish_live_ticket(
                    claims["request_id"], "COMPLETED", http_status=200,
                    response_checksum=digest(payload),
                    latency_seconds=time.monotonic() - started,
                    edge_revision=revision,
                )
            else:
                db.finish_external_ticket(
                    claims["experiment_id"], claims["case_id"], "COMPLETED",
                    http_status=200, response_checksum=digest(payload),
                    latency_seconds=time.monotonic() - started,
                    edge_revision=revision,
                )
            return JSONResponse(
                payload, status_code=200,
                headers={"X-RecSys-AB-Edge-Revision": revision},
            )

        return await asyncio.to_thread(forward_public_case)

    return app

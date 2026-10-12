from __future__ import annotations

import json
import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DDL = """
CREATE SCHEMA IF NOT EXISTS recsys_ab;
CREATE TABLE IF NOT EXISTS recsys_ab.sessions (
  session_key text PRIMARY KEY, release_id text NOT NULL,
  backend_context text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE recsys_ab.sessions ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE recsys_ab.sessions ADD COLUMN IF NOT EXISTS last_seen_at timestamptz;
ALTER TABLE recsys_ab.sessions ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE recsys_ab.sessions ADD COLUMN IF NOT EXISTS closed_at timestamptz;
ALTER TABLE recsys_ab.sessions ADD COLUMN IF NOT EXISTS close_reason text;
UPDATE recsys_ab.sessions SET last_seen_at=created_at WHERE last_seen_at IS NULL;
ALTER TABLE recsys_ab.sessions ALTER COLUMN last_seen_at SET DEFAULT now();
ALTER TABLE recsys_ab.sessions ALTER COLUMN last_seen_at SET NOT NULL;
ALTER TABLE recsys_ab.sessions DROP CONSTRAINT IF EXISTS sessions_source_check;
ALTER TABLE recsys_ab.sessions ADD CONSTRAINT sessions_source_check
  CHECK(source IS NULL OR source IN ('production','synthetic','live_test'));
CREATE INDEX IF NOT EXISTS ab_active_sessions_by_release
  ON recsys_ab.sessions(release_id) WHERE closed_at IS NULL;
CREATE TABLE IF NOT EXISTS recsys_ab.invocations (
  request_key text PRIMARY KEY, session_key text NOT NULL REFERENCES recsys_ab.sessions,
  experiment_id text NOT NULL, source text NOT NULL CHECK(source IN ('production','synthetic')),
  release_id text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz, result jsonb, response jsonb, task_id text
);
ALTER TABLE recsys_ab.invocations ADD COLUMN IF NOT EXISTS request_hash text;
CREATE INDEX IF NOT EXISTS ab_observation ON recsys_ab.invocations(experiment_id, source, started_at);
ALTER TABLE recsys_ab.invocations DROP CONSTRAINT IF EXISTS invocations_source_check;
ALTER TABLE recsys_ab.invocations ADD CONSTRAINT invocations_source_check CHECK(source IN ('production','synthetic','live_test'));
CREATE TABLE IF NOT EXISTS recsys_ab.heartbeat (
  instance text PRIMARY KEY, seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS recsys_ab.evaluation_outbox (
  request_key text PRIMARY KEY, experiment_id text NOT NULL,
  snapshot jsonb NOT NULL, metadata jsonb NOT NULL,
  evaluation jsonb, confirmed_at timestamptz,
  attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ab_evaluation_pending ON recsys_ab.evaluation_outbox(next_attempt_at)
  WHERE confirmed_at IS NULL;
CREATE TABLE IF NOT EXISTS recsys_ab.execution_claims (
  release_id text NOT NULL, session_id text NOT NULL, turn_id text NOT NULL,
  role text NOT NULL, tool_name text NOT NULL, call_id text NOT NULL,
  args jsonb NOT NULL, result jsonb, status text NOT NULL CHECK(status IN ('CLAIMED','COMPLETED','FAILED')),
  claimed_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
  PRIMARY KEY(release_id,session_id,turn_id,role,tool_name)
);
CREATE TABLE IF NOT EXISTS recsys_ab.execution_violations (
 release_id text NOT NULL, session_id text NOT NULL, turn_id text NOT NULL,
 role text NOT NULL, reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS execution_violations_turn ON recsys_ab.execution_violations(release_id,session_id,turn_id,role);
CREATE TABLE IF NOT EXISTS recsys_ab.compatibility_requests (
 request_key text PRIMARY KEY, experiment_id text NOT NULL, variant text NOT NULL,
 case_id text NOT NULL, release_id text NOT NULL, fixture_checksum text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, result jsonb,
 UNIQUE(experiment_id,variant,case_id)
);
CREATE TABLE IF NOT EXISTS recsys_ab.live_load_runs (
 experiment_id text PRIMARY KEY, claimed_at timestamptz NOT NULL DEFAULT now(),
 submitted integer NOT NULL DEFAULT 0 CHECK(submitted BETWEEN 0 AND 360)
);
CREATE TABLE IF NOT EXISTS recsys_ab.controller_actions (
 action_key text PRIMARY KEY, experiment_id text NOT NULL,
 action text NOT NULL CHECK(action IN ('prepare','route','promote','rollback','cleanup')),
 target_weight integer CHECK(target_weight IN (0,10,50,100)),
 expected_phase text NOT NULL, expected_state_etag text NOT NULL,
 status text NOT NULL CHECK(status IN
   ('INTENT','SUBMITTED','RUNNING','SUCCEEDED','FAILED','NEEDS_ATTENTION')),
 queue_id text, build_number integer, build_url text, reason text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ab_controller_actions_experiment
 ON recsys_ab.controller_actions(experiment_id,created_at);
CREATE TABLE IF NOT EXISTS recsys_ab.controller_status (
 experiment_id text PRIMARY KEY, decision text NOT NULL, reason text,
 last_tick_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS recsys_ab.traffic_runs (
 experiment_id text PRIMARY KEY, job_name text NOT NULL UNIQUE,
 manifest_checksum text NOT NULL, status text NOT NULL CHECK(status IN
   ('CREATED','RUNNING','COMPLETED','FAILED','LOST','NEEDS_ATTENTION')),
 live_submitted integer NOT NULL DEFAULT 0 CHECK(live_submitted BETWEEN 0 AND 360),
 synthetic_submitted integer NOT NULL DEFAULT 0 CHECK(synthetic_submitted BETWEEN 0 AND 20),
 phase text, reason text, created_at timestamptz NOT NULL DEFAULT now(),
 started_at timestamptz, heartbeat_at timestamptz, finished_at timestamptz,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS recsys_ab.live_traffic_tickets (
 request_id text PRIMARY KEY, experiment_id text NOT NULL REFERENCES recsys_ab.traffic_runs,
 sequence integer NOT NULL, phase text NOT NULL CHECK(phase IN ('CANARY','AB','VERIFY')),
 nonce text NOT NULL UNIQUE, prompt_sha256 text NOT NULL, expires_at timestamptz NOT NULL,
 status text NOT NULL CHECK(status IN
   ('DISPATCH_INTENT','CLAIMED','COMPLETED','AMBIGUOUS','REJECTED')),
 issued_at timestamptz NOT NULL DEFAULT now(), claimed_at timestamptz,
 finished_at timestamptz, http_status integer, response_checksum text,
 latency_seconds double precision CHECK(latency_seconds >= 0), edge_revision text,
 last_error text, UNIQUE(experiment_id,sequence)
);
CREATE INDEX IF NOT EXISTS ab_live_traffic_ticket_status
 ON recsys_ab.live_traffic_tickets(experiment_id,status);
CREATE TABLE IF NOT EXISTS recsys_ab.external_case_runs (
 experiment_id text PRIMARY KEY, fixture_checksum text NOT NULL,
 status text NOT NULL CHECK(status IN ('DISPATCHING','COMPLETED','HOLD')),
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS recsys_ab.external_case_tickets (
 experiment_id text NOT NULL REFERENCES recsys_ab.external_case_runs(experiment_id),
 case_id text NOT NULL, request_id text NOT NULL UNIQUE, nonce text NOT NULL UNIQUE,
 fixture_checksum text NOT NULL, prompt_sha256 text NOT NULL, expires_at timestamptz NOT NULL,
 status text NOT NULL CHECK(status IN ('ISSUED','DISPATCH_INTENT','CLAIMED','COMPLETED','AMBIGUOUS','REJECTED')),
 issued_at timestamptz NOT NULL DEFAULT now(), intent_at timestamptz, claimed_at timestamptz,
 finished_at timestamptz, http_status integer, response_checksum text,
 latency_seconds double precision CHECK(latency_seconds >= 0), edge_revision text,
 last_error text,
 PRIMARY KEY(experiment_id,case_id)
);
CREATE INDEX IF NOT EXISTS ab_external_ticket_status
 ON recsys_ab.external_case_tickets(experiment_id,status);
ALTER TABLE recsys_ab.external_case_tickets
 ADD COLUMN IF NOT EXISTS latency_seconds double precision;
ALTER TABLE recsys_ab.external_case_tickets
 ADD COLUMN IF NOT EXISTS edge_revision text;
CREATE TABLE IF NOT EXISTS recsys_ab.model_onboardings (
 onboarding_id text PRIMARY KEY, scope text NOT NULL, model_alias text NOT NULL,
 artifact_identity jsonb NOT NULL, policy_checksum text NOT NULL,
 llm_release_ref text NOT NULL, candidate_release_id text NOT NULL,
 intent_uri text NOT NULL, status text NOT NULL,
 reason text, build_url text, evidence_uri text,
 prepared_at timestamptz, expires_at timestamptz,
 cleanup_requested_at timestamptz, cleaned_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(scope,model_alias)
);
CREATE INDEX IF NOT EXISTS ab_ready_onboardings_expiry
 ON recsys_ab.model_onboardings(expires_at)
 WHERE status='READY';
CREATE TABLE IF NOT EXISTS recsys_ab.onboarding_compatibility (
 onboarding_id text NOT NULL REFERENCES recsys_ab.model_onboardings,
 case_id text NOT NULL, status text NOT NULL,
 result jsonb, latency_seconds double precision,
 created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 PRIMARY KEY(onboarding_id,case_id)
);
"""


class Database:
    def __init__(self, dsn):
        self.dsn = dsn

    def connect(self):
        return psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)

    def claim_live_load(self, experiment_id):
        # A replacement Pod/Job cannot obtain a fresh 360-request budget.
        # An ambiguous previous owner is deliberately not resumed or replayed.
        with self.connect() as c:
            return c.execute("""INSERT INTO recsys_ab.live_load_runs(experiment_id)
                VALUES (%s) ON CONFLICT DO NOTHING RETURNING experiment_id""",
                (experiment_id,)).fetchone() is not None

    def claim_live_request(self, experiment_id):
        with self.connect() as c:
            row = c.execute("""UPDATE recsys_ab.live_load_runs SET submitted=submitted+1
                WHERE experiment_id=%s AND submitted<360 RETURNING submitted""",
                (experiment_id,)).fetchone()
            return row['submitted'] if row else None

    def create_controller_action(self, row):
        """Persist one immutable controller decision before Jenkins delivery."""
        required = {
            "action_key", "experiment_id", "action", "target_weight",
            "expected_phase", "expected_state_etag",
        }
        if set(row) != required:
            raise ValueError("invalid controller action")
        with self.connect() as c:
            created = c.execute(
                """INSERT INTO recsys_ab.controller_actions
                   (action_key,experiment_id,action,target_weight,expected_phase,
                    expected_state_etag,status)
                   VALUES (%s,%s,%s,%s,%s,%s,'INTENT')
                   ON CONFLICT DO NOTHING RETURNING *""",
                tuple(row[key] for key in (
                    "action_key", "experiment_id", "action", "target_weight",
                    "expected_phase", "expected_state_etag",
                )),
            ).fetchone()
            if created:
                return dict(created), True
            existing = c.execute(
                "SELECT * FROM recsys_ab.controller_actions WHERE action_key=%s",
                (row["action_key"],),
            ).fetchone()
            if not existing or any(
                existing[key] != row[key] for key in required
            ):
                raise ValueError("controller action identity conflict")
            return dict(existing), False

    def controller_action(self, action_key):
        with self.connect() as c:
            row = c.execute(
                "SELECT * FROM recsys_ab.controller_actions WHERE action_key=%s",
                (action_key,),
            ).fetchone()
        return dict(row) if row else None

    def latest_controller_action(self, experiment_id):
        with self.connect() as c:
            row = c.execute(
                """SELECT * FROM recsys_ab.controller_actions
                   WHERE experiment_id=%s ORDER BY created_at DESC LIMIT 1""",
                (experiment_id,),
            ).fetchone()
        return dict(row) if row else None

    def record_controller_tick(self, experiment_id, decision, reason=None):
        with self.connect() as c:
            c.execute(
                """INSERT INTO recsys_ab.controller_status
                   (experiment_id,decision,reason,last_tick_at) VALUES (%s,%s,%s,now())
                   ON CONFLICT(experiment_id) DO UPDATE SET decision=excluded.decision,
                   reason=excluded.reason,last_tick_at=excluded.last_tick_at""",
                (experiment_id, str(decision)[:64], (reason or "")[:160] or None),
            )

    def update_controller_action(self, action_key, status, **values):
        allowed = {"queue_id", "build_number", "build_url", "reason"}
        if set(values) - allowed:
            raise ValueError("invalid controller action update")
        fields = ["status=%s", "updated_at=now()"] + [key + "=%s" for key in values]
        with self.connect() as c:
            row = c.execute(
                "UPDATE recsys_ab.controller_actions SET " + ",".join(fields)
                + " WHERE action_key=%s RETURNING *",
                (status, *values.values(), action_key),
            ).fetchone()
        if not row:
            raise ValueError("unknown controller action")
        return dict(row)

    def begin_traffic_run(self, experiment_id, job_name, manifest_checksum):
        """Claim the one lifetime Traffic Job for an experiment."""
        with self.connect() as c:
            created = c.execute(
                """INSERT INTO recsys_ab.traffic_runs
                   (experiment_id,job_name,manifest_checksum,status)
                   VALUES (%s,%s,%s,'CREATED') ON CONFLICT DO NOTHING RETURNING *""",
                (experiment_id, job_name, manifest_checksum),
            ).fetchone()
            if created:
                return dict(created), True
            existing = c.execute(
                "SELECT * FROM recsys_ab.traffic_runs WHERE experiment_id=%s",
                (experiment_id,),
            ).fetchone()
            if (
                not existing
                or existing["job_name"] != job_name
                or existing["manifest_checksum"] != manifest_checksum
            ):
                raise ValueError("traffic run immutable identity conflict")
            return dict(existing), False

    def traffic_run(self, experiment_id):
        with self.connect() as c:
            row = c.execute(
                "SELECT * FROM recsys_ab.traffic_runs WHERE experiment_id=%s",
                (experiment_id,),
            ).fetchone()
        return dict(row) if row else None

    def trigger_status(self, experiment_id):
        """Return controller-owned trigger lifecycle state for a Traffic Job."""
        with self.connect() as c:
            row = c.execute(
                "SELECT status FROM recsys_ab.trigger_requests WHERE experiment_id=%s",
                (experiment_id,),
            ).fetchone()
        return row["status"] if row else None

    def heartbeat_traffic_run(self, experiment_id, phase):
        with self.connect() as c:
            row = c.execute(
                """UPDATE recsys_ab.traffic_runs SET status='RUNNING',phase=%s,
                   started_at=coalesce(started_at,now()),heartbeat_at=now(),updated_at=now()
                   WHERE experiment_id=%s AND status IN ('CREATED','RUNNING')
                   RETURNING *""",
                (phase, experiment_id),
            ).fetchone()
        if not row:
            raise ValueError("traffic run is not runnable")
        return dict(row)

    def finish_traffic_run(self, experiment_id, status, reason=None):
        if status not in {"COMPLETED", "FAILED", "LOST", "NEEDS_ATTENTION"}:
            raise ValueError("invalid traffic run terminal status")
        with self.connect() as c:
            c.execute(
                """UPDATE recsys_ab.traffic_runs SET status=%s,reason=%s,
                   finished_at=now(),updated_at=now() WHERE experiment_id=%s""",
                (status, (reason or "")[:160] or None, experiment_id),
            )





    def traffic_evidence(self, experiment_id):
        with self.connect() as c:
            run = c.execute(
                "SELECT * FROM recsys_ab.traffic_runs WHERE experiment_id=%s",
                (experiment_id,),
            ).fetchone()
            rows = c.execute(
                """SELECT status,count(*) AS count FROM recsys_ab.live_traffic_tickets
                   WHERE experiment_id=%s GROUP BY status""",
                (experiment_id,),
            ).fetchall()
        return {
            "run": dict(run) if run else None,
            "live_tickets": {row["status"]: row["count"] for row in rows},
            "external": self.external_suite(experiment_id),
        }










    def migrate(self):
        with self.connect() as c:
            # IF NOT EXISTS alone does not serialize concurrent catalog inserts.
            c.execute(
                "SELECT pg_advisory_lock(hashtextextended('recsys-ab-schema', 0))"
            )
            try:
                c.execute(DDL)
            finally:
                c.execute(
                    "SELECT pg_advisory_unlock(hashtextextended('recsys-ab-schema', 0))"
                )

"""
FastAPI backend for AI Cargo Monitoring.

Serves the risk-scored data to the React dashboard and provides
tool-execution endpoints that the orchestrator will call.

Includes an embedded Supabase Realtime stream listener that
automatically detects new window_features rows, scores them, and
triggers orchestration — no separate process or hardcoded URLs needed.

Run:  uvicorn backend.app:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.models import (
    ApprovalDecision,
    ApprovalRequest,
    AuditRecord,
    RiskOverview,
    ShipmentSummary,
    WindowRisk,
)
from tools.approval_workflow import _PENDING_APPROVALS, decide as approve_decide, get_pending, get_all as get_all_approvals
from tools.triage_agent import _execute as triage_execute, _enrich_shipment
from tools import TOOL_MAP
from orchestrator.graph import run_orchestrator, get_graph_mermaid, get_mode
from orchestrator.llm_provider import get_llm, get_provider_name, get_model_name
from src.context_assembler import build_window_context
from src.data_loader import load_product_profiles

logger = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
SCORED_CSV = BASE / "artifacts" / "scored_windows.csv"
AUDIT_DIR = BASE / "audit_logs"


# ── Embedded Supabase stream listener ─────────────────────────────

_TIERS_TO_ORCHESTRATE = {"MEDIUM", "HIGH", "CRITICAL"}
_stream_stats = {"ingested": 0, "orchestrated": 0, "errors": 0}


async def _process_stream_record(record: dict):
    """Score a streamed row and trigger orchestration if risky."""
    from src.feature_engineering import engineer_features
    from src.deterministic_engine import score_row
    from src.risk_fusion import fuse_scores

    window_id = record.get("window_id", "?")
    try:
        profiles = _get_profiles()
        row_df = pd.DataFrame([record])
        for col in ("window_start", "window_end"):
            if col in row_df.columns:
                row_df[col] = pd.to_datetime(row_df[col], errors="coerce")
        row_df = engineer_features(row_df, profiles)
        row = row_df.iloc[0]

        det_score, det_results = score_row(row, profiles)
        rules_fired = [r.rule_name for r in det_results if r.fired]
        ml_score = float(record.get("ml_score", det_score * 0.8))
        final_score, risk_tier, actions, requires_human = fuse_scores(det_score, ml_score)

        scored = {
            "window_id": window_id,
            "shipment_id": record.get("shipment_id"),
            "risk_score": round(final_score, 4),
            "risk_tier": risk_tier,
            "rules_fired": rules_fired,
        }
        await _broadcast({"type": "ingest_scored", "result": scored})
        _stream_stats["ingested"] += 1

        logger.info("STREAM_SCORED  %s tier=%s score=%.4f", window_id, risk_tier, final_score)

        if risk_tier in _TIERS_TO_ORCHESTRATE:
            try:
                risk_data = score_window(window_id)
            except HTTPException:
                risk_data = _build_risk_input_from_record(record, final_score, risk_tier, rules_fired, ml_score)

            decision = run_orchestrator(risk_data)
            decision["_window_id"] = window_id
            _orchestrator_history.append(decision)
            if len(_orchestrator_history) > _MAX_HISTORY:
                _orchestrator_history[:] = _orchestrator_history[-_MAX_HISTORY:]
            await _broadcast({"type": "orchestrator_decision", "decision": decision})
            _stream_stats["orchestrated"] += 1
            logger.info("STREAM_ORCH   %s tier=%s actions=%d",
                        window_id, risk_tier, len(decision.get("actions_taken", [])))

    except Exception as e:
        _stream_stats["errors"] += 1
        logger.warning("Stream processing failed for %s: %s", window_id, e)


def _build_risk_input_from_record(record, final_score, risk_tier, rules_fired, ml_score):
    """Build a minimal risk_input when the window isn't in the scored CSV."""
    return {
        "window_id": record.get("window_id"),
        "shipment_id": record.get("shipment_id"),
        "container_id": record.get("container_id"),
        "product_id": record.get("product_id"),
        "leg_id": record.get("leg_id", ""),
        "product_type": record.get("product_id", ""),
        "transit_phase": record.get("transit_phase", ""),
        "risk_tier": risk_tier,
        "fused_risk_score": final_score,
        "ml_spoilage_probability": ml_score * 0.7,
        "deterministic_rule_flags": rules_fired,
        "avg_temp_c": record.get("avg_temp_c"),
        "temp_slope_c_per_hr": record.get("temp_slope_c_per_hr"),
        "current_delay_min": record.get("current_delay_min", 0),
        "delay_class": "developing" if record.get("current_delay_min", 0) > 30 else "stable",
        "key_drivers": [],
        "facility": {},
        "product_cost": {},
    }


async def _stream_listener_loop():
    """Background task: subscribe to Supabase Realtime and process INSERTs."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.info("Stream listener disabled — no Supabase credentials")
        return

    try:
        from supabase._async.client import AsyncClient, create_client as acreate
    except ImportError:
        logger.warning("Stream listener disabled — supabase async client not installed")
        return

    await asyncio.sleep(2)

    try:
        sb: AsyncClient = await acreate(url, key)

        def _on_insert(payload: dict):
            record = (
                payload.get("data", {}).get("record")
                or payload.get("record")
                or {}
            )
            if not record.get("window_id"):
                return
            logger.info("STREAM  new row: %s | shipment=%s",
                        record.get("window_id"), record.get("shipment_id"))
            asyncio.get_running_loop().create_task(_process_stream_record(record))

        channel = sb.channel("window-stream")
        channel.on_postgres_changes(
            event="INSERT",
            schema="public",
            table="window_features",
            callback=_on_insert,
        )
        await channel.subscribe()
        logger.info("Stream listener active — subscribed to window_features INSERT")

        while True:
            await asyncio.sleep(60)
            logger.info("STREAM_STATS  ingested=%d orchestrated=%d errors=%d",
                        _stream_stats["ingested"], _stream_stats["orchestrated"],
                        _stream_stats["errors"])

    except asyncio.CancelledError:
        logger.info("Stream listener shutting down")
    except Exception as e:
        logger.error("Stream listener error: %s", e)


@asynccontextmanager
async def lifespan(app_instance):
    task = asyncio.create_task(_stream_listener_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AI Cargo Monitor", version="1.0.0", lifespan=lifespan)

# Extra origins via env (comma-separated), e.g. custom Vercel domain:
#   CORS_ORIGINS=https://aicargo.vercel.app,https://www.yourdomain.com
_extra_origins = [
    o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        *_extra_origins,
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory caches ─────────────────────────────────────────────────

_df: Optional[pd.DataFrame] = None
_profiles: Optional[dict] = None


def _get_df() -> pd.DataFrame:
    global _df
    if _df is None:
        if not SCORED_CSV.exists():
            raise HTTPException(503, "Run `python pipeline.py train` first")
        _df = pd.read_csv(SCORED_CSV)
    return _df


def _get_profiles() -> dict:
    global _profiles
    if _profiles is None:
        _profiles = load_product_profiles()
    return _profiles


# ── WebSocket connections ────────────────────────────────────────────

_ws_clients: List[WebSocket] = []


async def _broadcast(event: dict):
    for ws in list(_ws_clients):
        try:
            await ws.send_json(event)
        except Exception:
            _ws_clients.remove(ws)


@app.websocket("/ws/events")
async def ws_events(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        _ws_clients.remove(websocket)


# ── Risk overview ────────────────────────────────────────────────────

@app.get("/api/risk/overview", response_model=RiskOverview)
def risk_overview():
    df = _get_df()
    tier_counts = df["risk_tier"].value_counts().to_dict()
    total = len(df)
    tier_pcts = {k: round(v / total * 100, 1) for k, v in tier_counts.items()}

    top = _build_shipment_summaries(df, top_n=10)
    return RiskOverview(
        total_windows=total,
        total_shipments=df["shipment_id"].nunique(),
        tier_counts=tier_counts,
        tier_pcts=tier_pcts,
        top_risky_shipments=top,
    )


# ── Shipments ────────────────────────────────────────────────────────

@app.get("/api/shipments", response_model=List[ShipmentSummary])
def list_shipments(risk_tier: Optional[str] = Query(None)):
    df = _get_df()
    summaries = _build_shipment_summaries(df, top_n=None)
    if risk_tier:
        summaries = [s for s in summaries if s.latest_risk_tier == risk_tier]
    return summaries


@app.get("/api/shipments/{shipment_id}/windows", response_model=List[WindowRisk])
def shipment_windows(shipment_id: str):
    df = _get_df()
    sub = df[df["shipment_id"] == shipment_id]
    if sub.empty:
        raise HTTPException(404, f"Shipment {shipment_id} not found")
    return [_row_to_window(row) for _, row in sub.iterrows()]


# ── Windows ──────────────────────────────────────────────────────────

@app.get("/api/windows", response_model=List[WindowRisk])
def list_windows(
    risk_tier: Optional[str] = Query(None),
    product_id: Optional[str] = Query(None),
    limit: int = Query(200, le=2000),
    offset: int = Query(0),
):
    df = _get_df()
    if risk_tier:
        df = df[df["risk_tier"] == risk_tier]
    if product_id:
        df = df[df["product_id"] == product_id]
    df = df.sort_values("final_score", ascending=False)
    page = df.iloc[offset : offset + limit]
    return [_row_to_window(row) for _, row in page.iterrows()]


@app.get("/api/windows/{window_id}", response_model=WindowRisk)
def get_window(window_id: str):
    df = _get_df()
    row = df[df["window_id"] == window_id]
    if row.empty:
        raise HTTPException(404, f"Window {window_id} not found")
    return _row_to_window(row.iloc[0])


# ── Risk engine output (for orchestrator) ────────────────────────────

@app.get("/api/risk/score-window/{window_id}")
def score_window(window_id: str):
    """
    Return the enriched risk engine output for a single window in the format
    expected by the orchestrator (system_prompt.md input contract).

    Extends the base risk fields with cascade context:
      delay_ratio, delay_class, hours_to_breach, facility, product_cost,
      window_end (for ETA computation in the cascade).
    """
    df = _get_df()
    profiles = _get_profiles()

    try:
        ctx = build_window_context(window_id, df, profiles)
    except KeyError:
        raise HTTPException(404, f"Window {window_id} not found")

    return {
        # Core identity
        "shipment_id": ctx["shipment_id"],
        "container_id": ctx["container_id"],
        "window_id": ctx["window_id"],
        "leg_id": ctx["leg_id"],
        "product_type": ctx["product_id"],
        "transit_phase": ctx["transit_phase"],
        "window_end": ctx["window_end"],

        # Risk scores
        "risk_tier": ctx["risk_tier"],
        "fused_risk_score": ctx["final_score"],
        "ml_spoilage_probability": ctx["ml_score"],
        "deterministic_rule_flags": ctx["det_rules_fired"],
        "key_drivers": [],
        "recommended_actions_from_risk_engine": ctx["recommended_actions"],
        "confidence_score": round(1.0 - abs(ctx["det_score"] - ctx["ml_score"]), 4),

        # Cascade context fields
        "delay_ratio": ctx["delay_ratio"],
        "delay_class": ctx["delay_class"],
        "hours_to_breach": ctx["hours_to_breach"],
        "current_delay_min": ctx["current_delay_min"],
        "facility": ctx["facility"],
        "product_cost": ctx["product_cost"],

        # Telemetry fields used by cold_storage_agent (temp trend context)
        "avg_temp_c": ctx["avg_temp_c"],
        "temp_slope_c_per_hr": ctx["temp_slope_c_per_hr"],

        "operational_constraints": [],
        "available_tools": list(TOOL_MAP.keys()),
    }


# ── Audit logs ───────────────────────────────────────────────────────

@app.get("/api/audit-logs", response_model=List[AuditRecord])
def list_audit_logs(
    shipment_id: Optional[str] = Query(None),
    risk_tier: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
):
    records = _load_audit_records()
    if shipment_id:
        records = [r for r in records if r.get("shipment_id") == shipment_id]
    if risk_tier:
        records = [r for r in records if r.get("risk_tier") == risk_tier]
    return records[:limit]


# ── Tool execution ───────────────────────────────────────────────────

@app.post("/api/tools/{tool_name}/execute")
async def execute_tool(tool_name: str, payload: Dict[str, Any]):
    if tool_name not in TOOL_MAP:
        raise HTTPException(404, f"Tool '{tool_name}' not found. Available: {list(TOOL_MAP.keys())}")
    tool = TOOL_MAP[tool_name]
    result = tool.invoke(payload)
    await _broadcast({"type": "tool_executed", "tool": tool_name, "result": result})
    return result


# ── Approval workflow ────────────────────────────────────────────────

@app.get("/api/approvals/pending", response_model=List[ApprovalRequest])
def pending_approvals():
    return get_pending()


@app.get("/api/approvals/all")
def all_approvals():
    """Return ALL approvals (pending, approved, rejected, executed)."""
    return get_all_approvals()


@app.delete("/api/approvals")
def clear_approvals():
    """Clear all approval records."""
    from tools.approval_workflow import _PENDING_APPROVALS
    count = len(_PENDING_APPROVALS)
    _PENDING_APPROVALS.clear()
    return {"cleared": count}


@app.post("/api/approvals/{approval_id}/decide")
async def decide_approval(approval_id: str, body: ApprovalDecision):
    result = approve_decide(approval_id, body.decision, body.decided_by)
    if "error" in result:
        raise HTTPException(404, result["error"])

    window_id = result.get("window_id") or result.get("shipment_id", "")
    for entry in _orchestrator_history:
        entry_wid = entry.get("_window_id") or entry.get("window_id", "")
        if entry_wid == window_id and entry.get("requires_approval"):
            entry["_approval_status"] = body.decision
            entry["_approved_by"] = body.decided_by
            break

    await _broadcast({"type": "approval_decided", "result": result})
    return result


@app.post("/api/approvals/{approval_id}/confirm")
async def confirm_approved(approval_id: str, body: Dict[str, Any] = None):
    """Confirm that first-pass execution was sufficient — no re-execution.

    The human reviewed the results and decided corrections are not needed.
    Closes the review without running any additional tools.
    """
    from tools.approval_workflow import _PENDING_APPROVALS
    record = _PENDING_APPROVALS.get(approval_id)
    if not record:
        raise HTTPException(404, f"Approval {approval_id} not found")
    if record.get("status") not in ("pending", "approved"):
        raise HTTPException(400, f"Approval {approval_id} cannot be confirmed (status={record.get('status')})")

    body = body or {}
    record["status"] = "confirmed"
    record["decided_at"] = datetime.now(timezone.utc).isoformat()
    record["decided_by"] = body.get("decided_by", "operator")
    record["decision"] = "confirmed"
    record["executed_tools"] = []

    window_id = record.get("window_id") or record.get("shipment_id", "")

    for i, old in enumerate(_orchestrator_history):
        old_wid = old.get("_window_id") or old.get("window_id", "")
        old_aid = old.get("approval_id")
        if old_aid == approval_id or (old_wid == window_id and old.get("awaiting_approval")):
            old["awaiting_approval"] = False
            old["_execution_mode"] = "confirmed"
            old["_approved_by"] = record["decided_by"]
            old["_approved_at"] = record["decided_at"]
            old["review_status"] = "confirmed"
            old["decision_summary"] = old.get("decision_summary", "").replace(
                "Awaiting human review.", "Human confirmed — first-pass response adequate."
            ).replace(
                "Awaiting human confirmation.", "Human confirmed — response adequate."
            )
            _orchestrator_history[i] = old
            break

    await _broadcast({"type": "approval_confirmed", "approval_id": approval_id, "record": record})
    return record


@app.post("/api/approvals/{approval_id}/execute")
async def execute_approved(approval_id: str, body: Dict[str, Any] = None):
    """Execute corrective/additional tools after human review.

    The human selects which tools to run. Replaces the original history entry
    with the post-approval execution result, preserving the planning data.
    """
    from tools.approval_workflow import _PENDING_APPROVALS
    from orchestrator.graph import run_orchestrator_selective
    record = _PENDING_APPROVALS.get(approval_id)
    if not record:
        raise HTTPException(404, f"Approval {approval_id} not found")
    if record.get("status") not in ("pending", "approved"):
        raise HTTPException(400, f"Approval {approval_id} is not approved (status={record.get('status')})")

    window_id = record.get("window_id") or record.get("shipment_id", "")
    body = body or {}
    selected_tools = body.get("selected_tools", [])
    selected_tools = [t for t in selected_tools if t != "approval_workflow"]

    if not selected_tools:
        proposed = record.get("proposed_corrections", [])
        deferred = record.get("proposed_deferred", [])
        combined = [t for t in (proposed + deferred) if t != "approval_workflow"]
        if combined:
            selected_tools = combined
        else:
            return await confirm_approved(approval_id, body)

    try:
        risk_data = score_window(window_id)
    except Exception:
        risk_data = {
            "shipment_id": record.get("shipment_id"),
            "window_id": window_id,
            "container_id": record.get("container_id"),
            "risk_tier": record.get("risk_tier", "HIGH"),
        }

    decision = run_orchestrator_selective(risk_data, selected_tools)

    record["status"] = "executed"
    record["executed_at"] = datetime.now(timezone.utc).isoformat()
    record["executed_tools"] = selected_tools

    decision["_window_id"] = window_id
    decision["_approval_id"] = approval_id
    decision["_execution_mode"] = "post_approval"
    decision["_approved_by"] = record.get("decided_by", "operator")
    decision["_approved_at"] = record.get("decided_at", "")
    decision["awaiting_approval"] = False
    decision["review_status"] = "executed"

    first_pass_actions = record.get("first_pass_actions", [])
    post_approval_actions = decision.get("actions_taken", [])
    for a in post_approval_actions:
        if isinstance(a, dict):
            a["_pass"] = "post_approval"
    decision["actions_taken"] = first_pass_actions + post_approval_actions
    decision["first_pass_actions"] = first_pass_actions
    decision["post_approval_actions"] = post_approval_actions

    saved_cascade = record.get("cascade_context", {})
    if saved_cascade:
        merged_cascade = dict(saved_cascade)
        merged_cascade.update(decision.get("cascade_context", {}))
        decision["cascade_context"] = merged_cascade
        decision["cascade_summary"] = {
            k: str(v)[:200] for k, v in merged_cascade.items()
        }

    PLAN_KEYS = ("draft_plan", "reflection_notes", "revised_plan",
                  "llm_reasoning", "proposed_tools", "observation",
                  "observation_issues", "observation_actions")
    orig = record.get("original_plan", {})
    if orig:
        for key in PLAN_KEYS:
            if orig.get(key):
                decision[key] = orig[key]

    replaced = False
    for i, old in enumerate(_orchestrator_history):
        old_aid = old.get("approval_id")
        old_wid = old.get("_window_id") or old.get("window_id", "")
        if old_aid == approval_id or (old_wid == window_id and old.get("awaiting_approval")):
            if not orig:
                for key in PLAN_KEYS:
                    if key in old and old[key]:
                        decision[key] = old[key]
            _orchestrator_history[i] = decision
            replaced = True
            break

    fp_tools = [a["tool"] for a in first_pass_actions if isinstance(a, dict)]
    pa_tools = [a["tool"] for a in post_approval_actions if isinstance(a, dict)]
    tier = decision.get("risk_tier", record.get("risk_tier", ""))
    decision["decision_summary"] = (
        f"{tier} risk: {len(fp_tools)} tools executed in first pass "
        f"({', '.join(fp_tools)}). Human approved — "
        f"{len(pa_tools)} post-approval tool(s) executed "
        f"({', '.join(pa_tools)})."
    )
    decision["confidence"] = max(
        orig.get("confidence", 0) if orig else decision.get("confidence", 0),
        0.85
    )

    if not replaced:
        _orchestrator_history.append(decision)

    await _broadcast({"type": "approval_executed", "approval_id": approval_id, "decision": decision})
    return decision


@app.post("/api/orchestrator/run-selective/{window_id}")
async def orchestrate_selective(window_id: str, body: Dict[str, Any]):
    """Run orchestration with human-selected tools only."""
    selected_tools = body.get("selected_tools", [])
    if not selected_tools:
        raise HTTPException(400, "selected_tools list is required")

    from orchestrator.graph import run_orchestrator_selective
    risk_data = score_window(window_id)
    decision = run_orchestrator_selective(risk_data, selected_tools)
    decision["_window_id"] = window_id
    decision["_execution_mode"] = "human_selective"
    _orchestrator_history.append(decision)
    if len(_orchestrator_history) > _MAX_HISTORY:
        _orchestrator_history[:] = _orchestrator_history[-_MAX_HISTORY:]
    await _broadcast({"type": "orchestrator_decision", "decision": decision})
    return decision


# ── Orchestrator ─────────────────────────────────────────────────────

_MAX_HISTORY = 500
_orchestrator_history: List[Dict[str, Any]] = []


@app.post("/api/orchestrator/run/{window_id}")
async def orchestrate_window(window_id: str):
    """Feed a window's risk output through the full orchestration agent."""
    risk_data = score_window(window_id)
    decision = run_orchestrator(risk_data)
    decision["_window_id"] = window_id
    _orchestrator_history.append(decision)
    if len(_orchestrator_history) > _MAX_HISTORY:
        _orchestrator_history[:] = _orchestrator_history[-_MAX_HISTORY:]
    await _broadcast({"type": "orchestrator_decision", "decision": decision})
    return decision


@app.post("/api/orchestrator/run-batch")
async def orchestrate_batch(window_ids: List[str]):
    """Orchestrate multiple windows (e.g. all CRITICAL windows)."""
    results = []
    for wid in window_ids[:20]:
        try:
            risk_data = score_window(wid)
            decision = run_orchestrator(risk_data)
            decision["_window_id"] = wid
            _orchestrator_history.append(decision)
            results.append(decision)
        except Exception as exc:
            results.append({"_window_id": wid, "error": str(exc)})
    await _broadcast({"type": "orchestrator_batch", "count": len(results)})
    return results


@app.get("/api/orchestrator/history")
def orchestrator_history(limit: int = Query(50, le=200)):
    return list(reversed(_orchestrator_history[-limit:]))


@app.delete("/api/orchestrator/history")
def clear_orchestrator_history():
    """Clear all orchestration history from memory."""
    count = len(_orchestrator_history)
    _orchestrator_history.clear()
    return {"cleared": count}


@app.get("/api/graph/mermaid")
def graph_mermaid():
    """Return the Mermaid diagram of the orchestration graph."""
    return {"mermaid": get_graph_mermaid()}


@app.get("/api/orchestrator/mode")
def orchestrator_mode():
    """Return the orchestrator's active LLM provider, model, and mode."""
    return get_mode()


@app.get("/api/llm/status")
def llm_status():
    """Full LLM provider status: active provider, available providers, and config."""
    import orchestrator.llm_provider as prov
    available = []
    for name in ["groq", "ollama", "openai", "anthropic"]:
        factory = prov._PROVIDERS.get(name)
        if factory:
            try:
                result = factory()
                available.append({"provider": name, "available": result is not None})
            except Exception:
                available.append({"provider": name, "available": False})

    return {
        "active_provider": get_provider_name(),
        "active_model": get_model_name(),
        "mode": "agentic" if get_llm() is not None else "deterministic",
        "priority": os.environ.get("CARGO_LLM_PRIORITY", "groq,ollama,openai,anthropic"),
        "providers": available,
        "keys_configured": {
            "groq": bool(os.environ.get("GROQ_API_KEY", "")),
            "openai": bool(os.environ.get("OPENAI_API_KEY", "")),
            "anthropic": bool(os.environ.get("ANTHROPIC_API_KEY", "")),
        },
    }


@app.post("/api/llm/configure")
async def configure_llm(config: Dict[str, Any]):
    """
    Hot-configure LLM provider settings without restart.
    Accepts: openai_api_key, anthropic_api_key, priority, ollama_model, openai_model, anthropic_model
    """
    import orchestrator.llm_provider as prov

    changed = []
    if "groq_api_key" in config:
        os.environ["GROQ_API_KEY"] = config["groq_api_key"]
        changed.append("GROQ_API_KEY")
    if "openai_api_key" in config:
        os.environ["OPENAI_API_KEY"] = config["openai_api_key"]
        changed.append("OPENAI_API_KEY")
    if "anthropic_api_key" in config:
        os.environ["ANTHROPIC_API_KEY"] = config["anthropic_api_key"]
        changed.append("ANTHROPIC_API_KEY")
    if "priority" in config:
        os.environ["CARGO_LLM_PRIORITY"] = config["priority"]
        changed.append("CARGO_LLM_PRIORITY")
    if "groq_model" in config:
        os.environ["CARGO_GROQ_MODEL"] = config["groq_model"]
        changed.append("CARGO_GROQ_MODEL")
    if "ollama_model" in config:
        os.environ["CARGO_OLLAMA_MODEL"] = config["ollama_model"]
        changed.append("CARGO_OLLAMA_MODEL")
    if "openai_model" in config:
        os.environ["CARGO_OPENAI_MODEL"] = config["openai_model"]
        changed.append("CARGO_OPENAI_MODEL")
    if "anthropic_model" in config:
        os.environ["CARGO_ANTHROPIC_MODEL"] = config["anthropic_model"]
        changed.append("CARGO_ANTHROPIC_MODEL")

    prov.get_llm(force_refresh=True)

    return {
        "status": "ok",
        "changed": changed,
        "active_provider": prov.get_provider_name(),
        "active_model": prov.get_model_name(),
    }


@app.get("/api/graph/topology")
def graph_topology():
    """Return a JSON description of the full system graph topology."""
    return {
        "layers": [
            {
                "id": "L1", "name": "Data & Ingestion",
                "nodes": [
                    {"id": "sensors", "label": "Smart Containers"},
                    {"id": "ingest", "label": "Window Aggregation"},
                ],
                "edges": [{"from": "sensors", "to": "ingest"}],
            },
            {
                "id": "L2", "name": "Risk Scoring Engine",
                "nodes": [
                    {"id": "features", "label": "Feature Engineering"},
                    {"id": "det", "label": "Deterministic Rules"},
                    {"id": "ml", "label": "XGBoost Predictor"},
                    {"id": "fusion", "label": "Risk Fusion"},
                ],
                "edges": [
                    {"from": "features", "to": "det"},
                    {"from": "features", "to": "ml"},
                    {"from": "det", "to": "fusion"},
                    {"from": "ml", "to": "fusion"},
                ],
            },
            {
                "id": "L3", "name": "Orchestration Agent",
                "nodes": [
                    {"id": "interpret", "label": "Interpret Risk"},
                    {"id": "plan", "label": "Generate Plan"},
                    {"id": "reflect", "label": "Self-Critique"},
                    {"id": "revise", "label": "Revise Plan"},
                    {"id": "execute", "label": "Execute Tools"},
                    {"id": "output", "label": "Compile Decision"},
                ],
                "edges": [
                    {"from": "interpret", "to": "plan"},
                    {"from": "plan", "to": "reflect"},
                    {"from": "reflect", "to": "revise", "label": "has gaps"},
                    {"from": "reflect", "to": "execute", "label": "plan OK"},
                    {"from": "revise", "to": "execute"},
                    {"from": "execute", "to": "output"},
                ],
            },
            {
                "id": "L4", "name": "Agent Tools",
                "nodes": [
                    {"id": "t_route", "label": "Route Agent"},
                    {"id": "t_cold", "label": "Cold Storage"},
                    {"id": "t_notify", "label": "Notification"},
                    {"id": "t_compliance", "label": "Compliance"},
                    {"id": "t_schedule", "label": "Scheduling"},
                    {"id": "t_insurance", "label": "Insurance"},
                    {"id": "t_triage", "label": "Triage"},
                    {"id": "t_approval", "label": "Approval"},
                ],
                "edges": [],
            },
            {
                "id": "L5", "name": "Human-in-the-Loop",
                "nodes": [
                    {"id": "dashboard", "label": "Ops Dashboard"},
                    {"id": "approve", "label": "Approval Queue"},
                ],
                "edges": [{"from": "approve", "to": "dashboard"}],
            },
        ],
        "cross_layer_edges": [
            {"from": "ingest", "to": "features"},
            {"from": "fusion", "to": "interpret"},
            {"from": "execute", "to": "t_route"},
            {"from": "execute", "to": "t_cold"},
            {"from": "execute", "to": "t_notify"},
            {"from": "execute", "to": "t_compliance"},
            {"from": "execute", "to": "t_insurance"},
            {"from": "execute", "to": "t_approval"},
            {"from": "t_approval", "to": "approve"},
            {"from": "output", "to": "dashboard"},
        ],
    }


# ── Triage ────────────────────────────────────────────────────────────

@app.get("/api/triage/critical-shipments")
async def triage_critical_shipments(limit: int = Query(20, le=100)):
    """
    Auto-triage: pull all CRITICAL+HIGH windows, find worst per shipment,
    rank with enrichment, return priority list.
    """
    df = _get_df()
    critical = df[df["risk_tier"].isin(["CRITICAL", "HIGH"])]
    if critical.empty:
        return {"priority_list": [], "total_shipments": 0}

    worst = critical.sort_values("final_score", ascending=False).groupby("shipment_id").first().reset_index()
    shipments = [
        {
            "shipment_id": row["shipment_id"],
            "risk_tier": row["risk_tier"],
            "fused_risk_score": float(row["final_score"]),
            "product_id": row["product_id"],
            "container_id": row.get("container_id", ""),
            "transit_phase": str(row.get("transit_phase", "")),
        }
        for _, row in worst.head(limit).iterrows()
    ]
    result = triage_execute(shipments=shipments, enrich=True)
    await _broadcast({"type": "triage_ranked", "count": len(shipments)})
    return result


@app.post("/api/triage/rank")
async def triage_rank(payload: Dict[str, Any]):
    """Rank a caller-supplied list of shipment dicts."""
    shipments = payload.get("shipments", [])
    enrich = payload.get("enrich", True)
    result = triage_execute(shipments=shipments, enrich=enrich)
    await _broadcast({"type": "triage_ranked", "count": len(shipments)})
    return result


# ── Data Ingest (Karthik's Supabase pipeline) ────────────────────────

@app.post("/api/ingest")
async def ingest_window(payload: Dict[str, Any]):
    """
    Receive a single window_features row (from Supabase stream_listener
    or direct POST) and score it through the risk engine in real time.
    Returns the risk assessment and optionally triggers orchestration.
    """
    from src.feature_engineering import engineer_features
    from src.deterministic_engine import score_row
    from src.risk_fusion import fuse_scores

    profiles = _get_profiles()
    row_df = pd.DataFrame([payload])
    for col in ("window_start", "window_end"):
        if col in row_df.columns:
            row_df[col] = pd.to_datetime(row_df[col], errors="coerce")
    row_df = engineer_features(row_df, profiles)
    row = row_df.iloc[0]

    det_score, det_results = score_row(row, profiles)
    rules_fired = [r.rule_name for r in det_results if r.fired]

    ml_score = float(payload.get("ml_score", det_score * 0.8))

    final_score, risk_tier, actions, requires_human = fuse_scores(det_score, ml_score)

    result = {
        "window_id": payload.get("window_id"),
        "shipment_id": payload.get("shipment_id"),
        "risk_score": round(final_score, 4),
        "risk_tier": risk_tier,
        "det_score": round(det_score, 4),
        "ml_score": round(ml_score, 4),
        "rules_fired": rules_fired,
        "recommended_actions": actions,
        "requires_human_approval": requires_human,
    }

    await _broadcast({"type": "ingest_scored", "result": result})
    return result


# ── Analytics (chart-ready aggregations) ──────────────────────────────

@app.get("/api/analytics")
def analytics():
    """Pre-computed distributions for dashboard charts."""
    import numpy as np

    df = _get_df()

    # 1. Tier counts by transit phase
    tier_by_phase = (
        df.groupby(["transit_phase", "risk_tier"])
        .size()
        .reset_index(name="count")
        .to_dict(orient="records")
    )

    # 2. Score distribution (histogram bins)
    bins = np.linspace(0, 1, 21)
    hist_vals, _ = np.histogram(df["final_score"].dropna(), bins=bins)
    score_histogram = [
        {"bin_start": round(bins[i], 2), "bin_end": round(bins[i + 1], 2), "count": int(hist_vals[i])}
        for i in range(len(hist_vals))
    ]

    # 3. Temperature stats by product
    temp_by_product = []
    for pid, grp in df.groupby("product_id"):
        temp_by_product.append({
            "product_id": pid,
            "avg_temp": round(float(grp["avg_temp_c"].mean()), 2),
            "min_temp": round(float(grp["avg_temp_c"].min()), 2),
            "max_temp": round(float(grp["avg_temp_c"].max()), 2),
            "std_temp": round(float(grp["avg_temp_c"].std()), 2),
            "windows": len(grp),
            "critical_pct": round(float((grp["risk_tier"] == "CRITICAL").sum() / len(grp) * 100), 1),
        })

    # 4. Phase distribution with risk breakdown
    phase_stats = []
    for phase, grp in df.groupby("transit_phase"):
        tier_counts = grp["risk_tier"].value_counts().to_dict()
        phase_stats.append({
            "phase": str(phase),
            "total": len(grp),
            "critical": tier_counts.get("CRITICAL", 0),
            "high": tier_counts.get("HIGH", 0),
            "medium": tier_counts.get("MEDIUM", 0),
            "low": tier_counts.get("LOW", 0),
            "avg_score": round(float(grp["final_score"].mean()), 4),
        })

    # 5. Container-level aggregations
    container_stats = []
    for (sid, cid), grp in df.groupby(["shipment_id", "container_id"]):
        container_stats.append({
            "shipment_id": sid,
            "container_id": cid,
            "product_id": grp["product_id"].iloc[0],
            "windows": len(grp),
            "max_score": round(float(grp["final_score"].max()), 4),
            "avg_score": round(float(grp["final_score"].mean()), 4),
            "avg_temp": round(float(grp["avg_temp_c"].mean()), 2),
            "risk_tier": grp.sort_values("final_score", ascending=False).iloc[0]["risk_tier"],
            "critical_windows": int((grp["risk_tier"] == "CRITICAL").sum()),
            "high_windows": int((grp["risk_tier"] == "HIGH").sum()),
            "phases": grp["transit_phase"].unique().tolist(),
        })
    container_stats.sort(key=lambda c: c["max_score"], reverse=True)

    return {
        "tier_by_phase": tier_by_phase,
        "score_histogram": score_histogram,
        "temp_by_product": temp_by_product,
        "phase_stats": phase_stats,
        "container_stats": container_stats[:200],
    }


# ── Helpers ──────────────────────────────────────────────────────────

def _build_shipment_summaries(
    df: pd.DataFrame, top_n: Optional[int] = 10,
) -> List[ShipmentSummary]:
    groups = df.groupby("shipment_id")
    summaries = []
    for sid, grp in groups:
        tier_vc = grp["risk_tier"].value_counts()
        total = len(grp)
        summaries.append(ShipmentSummary(
            shipment_id=sid,
            containers=grp["container_id"].unique().tolist(),
            products=grp["product_id"].unique().tolist(),
            total_windows=total,
            latest_risk_tier=grp.sort_values("window_start" if "window_start" in grp.columns else "window_id").iloc[-1]["risk_tier"],
            max_fused_score=round(float(grp["final_score"].max()), 4),
            pct_critical=round(tier_vc.get("CRITICAL", 0) / total * 100, 1),
            pct_high=round(tier_vc.get("HIGH", 0) / total * 100, 1),
        ))
    summaries.sort(key=lambda s: s.max_fused_score, reverse=True)
    if top_n:
        return summaries[:top_n]
    return summaries


def _row_to_window(row) -> WindowRisk:
    return WindowRisk(
        window_id=row["window_id"],
        shipment_id=row["shipment_id"],
        container_id=row["container_id"],
        product_id=row["product_id"],
        leg_id=row["leg_id"],
        window_start=str(row.get("window_start", "")),
        window_end=str(row.get("window_end", "")),
        transit_phase=str(row.get("transit_phase", "")),
        avg_temp_c=round(float(row.get("avg_temp_c", 0)), 2),
        det_score=round(float(row.get("det_score", 0)), 4),
        ml_score=round(float(row.get("ml_score", 0)), 4),
        final_score=round(float(row.get("final_score", 0)), 4),
        risk_tier=row.get("risk_tier", "LOW"),
        det_rules_fired=str(row.get("det_rules_fired", "")),
        recommended_actions=str(row.get("recommended_actions", "")),
        requires_human_approval=bool(row.get("requires_human_approval", False)),
    )


def _load_audit_records() -> List[dict]:
    records = []
    all_paths = sorted(AUDIT_DIR.glob("audit_*.jsonl")) + sorted(AUDIT_DIR.glob("compliance_events.jsonl"))
    for path in all_paths:
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read audit file %s: %s", path, exc)
    return records

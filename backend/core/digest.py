"""
core/digest.py — Nightly Digest Engine.

Generates an AI-powered nightly brief using Claude and sends it via:
  1. Email (Resend)
  2. Slack (webhook)

Schedule: called from a background thread or RQ job.
"""
import json
import os
import threading
from datetime import datetime

import httpx

from core.config import RESEND_API_KEY, RESEND_FROM, APP_URL
from core.database import get_db
from core.health_score import compute_health_score


_ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def _call_claude(prompt: str, max_tokens: int = 800) -> str:
    """Call Claude API and return the text response."""
    if not _ANTHROPIC_API_KEY:
        return ""
    try:
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         _ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-3-5-haiku-20241022",
                "max_tokens": max_tokens,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        data = resp.json()
        return data["content"][0]["text"] if data.get("content") else ""
    except Exception as e:
        print(f"[Digest] Claude call failed: {e}")
        return ""


def _send_email(to: str, subject: str, html_body: str) -> bool:
    """Send email via Resend."""
    if not RESEND_API_KEY:
        print("[Digest] RESEND_API_KEY not set — skipping email")
        return False
    try:
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": RESEND_FROM, "to": [to], "subject": subject, "html": html_body},
            timeout=15,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"[Digest] Email send failed: {e}")
        return False


def _send_slack(webhook_url: str, text: str) -> bool:
    """Post message to Slack webhook."""
    if not webhook_url:
        return False
    try:
        resp = httpx.post(webhook_url, json={"text": text}, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"[Digest] Slack send failed: {e}")
        return False


def _build_digest_prompt(health: dict, company_name: str, period: str) -> str:
    """Build the Claude prompt for digest generation."""
    needs = ", ".join(health.get("needs_attention", [])[:4]) or "none"
    well  = ", ".join(health.get("doing_well", [])[:4]) or "none"
    return f"""You are Anika, an AI CFO analyst. Generate a concise nightly performance brief for {company_name}.

Data as of {period}:
- Health Score: {health['score']}/100 ({health['label']})
- Momentum: {health['momentum']} — {health['momentum_trend']}
- Target Achievement: {health['target_achievement']}%
- Risk Flags Score: {health['risk_flags']}
- KPIs: {health['kpis_green']} green, {health['kpis_yellow']} watch, {health['kpis_red']} critical
- Needs Attention: {needs}
- Doing Well: {well}

Write a brief (3 bullet points maximum, no markdown headers, plain sentences):
1. Overall performance status in one sentence
2. The single most critical item requiring action
3. The strongest positive signal

Be direct, specific, and actionable. Write for a CFO audience. No fluff."""


def _build_email_html(company_name: str, health: dict, brief_text: str, period: str) -> str:
    """Build HTML email body for the nightly digest."""
    score = health["score"]
    color = "#059669" if score >= 70 else ("#d97706" if score >= 50 else "#dc2626")
    bullets = [b.strip().lstrip("•- 123456789.") for b in brief_text.strip().split("\n") if b.strip()]

    bullet_html = "".join(
        f'<li style="margin-bottom:10px;color:#374151;font-size:15px;line-height:1.6">{b}</li>'
        for b in bullets
    )

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"/></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f8fafc;margin:0;padding:40px 20px">
  <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.1)">
    <div style="background:#0f172a;padding:28px 32px">
      <p style="color:#94a3b8;font-size:12px;margin:0 0 6px;text-transform:uppercase;letter-spacing:1px">Nightly Brief</p>
      <h1 style="color:#fff;font-size:22px;margin:0;font-weight:700">{company_name}</h1>
      <p style="color:#64748b;font-size:13px;margin:6px 0 0">{period}</p>
    </div>

    <div style="padding:28px 32px;border-bottom:1px solid #f1f5f9">
      <div style="display:flex;align-items:center;gap:20px">
        <div style="text-align:center;min-width:80px">
          <div style="font-size:44px;font-weight:800;color:{color};line-height:1">{score}</div>
          <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;margin-top:4px">Health Score</div>
        </div>
        <div>
          <div style="font-size:16px;font-weight:600;color:#1e293b">{health['label']}</div>
          <div style="font-size:13px;color:#64748b;margin-top:4px">
            {health['kpis_green']} on target · {health['kpis_yellow']} watch · {health['kpis_red']} critical
          </div>
          <div style="font-size:13px;color:#64748b;margin-top:2px">
            Momentum: {health['momentum_trend'].title()} · Target rate: {health['target_achievement']}%
          </div>
        </div>
      </div>
    </div>

    <div style="padding:28px 32px">
      <h2 style="font-size:14px;font-weight:700;color:#0f172a;margin:0 0 16px;text-transform:uppercase;letter-spacing:.5px">What You Need to Know</h2>
      <ul style="margin:0;padding-left:20px">
        {bullet_html}
      </ul>
    </div>

    <div style="padding:20px 32px;background:#f8fafc;border-top:1px solid #e2e8f0">
      <a href="{APP_URL}" style="color:#0055A4;font-size:13px;text-decoration:none;font-weight:600">Open Axiom Intelligence →</a>
      <p style="color:#94a3b8;font-size:11px;margin:8px 0 0">You are receiving this because digest emails are enabled for your workspace.</p>
    </div>
  </div>
</body>
</html>"""


def run_digest_for_workspace(workspace_id: str) -> dict:
    """
    Run the full digest pipeline for one workspace.
    Returns a summary dict with delivery status.
    """
    conn = get_db()

    # Get health score
    health = compute_health_score(conn, workspace_id)
    if health["months_of_data"] == 0:
        conn.close()
        return {"status": "skipped", "reason": "no data"}

    # Get company settings
    settings_rows = conn.execute(
        "SELECT key, value FROM company_settings WHERE workspace_id=?", [workspace_id]
    ).fetchall()
    settings = {r["key"]: r["value"] for r in settings_rows}
    company_name  = settings.get("company_name", "Your Company")
    digest_emails = settings.get("digest_emails", "")
    slack_webhook = settings.get("slack_webhook", "")
    slack_digest  = settings.get("slack_digest_enabled", "false") == "true"
    conn.close()

    # Generate AI brief
    period = datetime.utcnow().strftime("%B %d, %Y")
    prompt = _build_digest_prompt(health, company_name, period)
    brief  = _call_claude(prompt) or f"Health score: {health['score']}/100 ({health['label']}). {len(health['needs_attention'])} KPIs need attention."

    results: dict = {"email": False, "slack": False, "brief_generated": bool(brief)}

    # Send email(s)
    if digest_emails:
        html = _build_email_html(company_name, health, brief, period)
        subject = f"Nightly Brief: {company_name} · Health {health['score']}/100 — {period}"
        for email in [e.strip() for e in digest_emails.split(",") if e.strip()]:
            ok = _send_email(email, subject, html)
            results["email"] = results["email"] or ok

    # Send Slack
    if slack_digest and slack_webhook:
        plain = f"*{company_name} — Nightly Brief ({period})*\n"
        plain += f"Health Score: *{health['score']}/100* ({health['label']})\n"
        plain += f"{health['kpis_green']} green · {health['kpis_yellow']} watch · {health['kpis_red']} critical\n\n"
        plain += brief
        plain += f"\n\n<{APP_URL}|Open Axiom Intelligence →>"
        results["slack"] = _send_slack(slack_webhook, plain)

    return results


def run_nightly_digest():
    """
    Entry point called by scheduler / RQ job.
    Iterates all workspaces and runs digest for each.
    """
    print(f"[Digest] Starting nightly digest run at {datetime.utcnow().isoformat()}")
    conn = get_db()
    # Get unique workspace IDs with data
    rows = conn.execute(
        "SELECT DISTINCT workspace_id FROM monthly_data WHERE workspace_id != ''"
    ).fetchall()
    conn.close()

    workspace_ids = [r["workspace_id"] for r in rows]
    print(f"[Digest] Processing {len(workspace_ids)} workspaces")

    for wid in workspace_ids:
        try:
            result = run_digest_for_workspace(wid)
            print(f"[Digest] workspace={wid} result={result}")
        except Exception as e:
            print(f"[Digest] ERROR for workspace={wid}: {e}")

    print("[Digest] Nightly digest run complete")

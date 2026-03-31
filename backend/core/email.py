"""
core/email.py — Email sending helpers (magic link + KPI alert).
"""
import httpx
from core.config import RESEND_API_KEY, RESEND_FROM, APP_URL


async def _send_magic_link_email(to_email: str, magic_url: str, subject: str = None, body_override: str = None) -> bool:
    if not RESEND_API_KEY:
        return False
    try:
        email_subject = subject if subject else "Your Axiom sign-in link"
        if body_override:
            # Plain-text body override — wrap in simple HTML
            body_lines = body_override.replace("\n", "<br>")
            email_html = f"""
                    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                                max-width:480px;margin:0 auto;padding:40px 24px;background:#fff">
                      <div style="margin-bottom:32px">
                        <span style="font-size:20px;font-weight:700;color:#6366f1">Axiom</span>
                      </div>
                      <p style="color:#0f172a;line-height:1.6">{body_lines}</p>
                    </div>
                    """
        else:
            email_html = f"""
                    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                                max-width:480px;margin:0 auto;padding:40px 24px;background:#fff">
                      <div style="margin-bottom:32px">
                        <span style="font-size:20px;font-weight:700;color:#6366f1">Axiom</span>
                      </div>
                      <h2 style="color:#0f172a;font-size:22px;margin:0 0 8px">Sign in to Axiom</h2>
                      <p style="color:#64748b;margin:0 0 28px;line-height:1.6">
                        Click the button below to sign in. This link expires in 15 minutes
                        and can only be used once.
                      </p>
                      <a href="{magic_url}"
                         style="background:#6366f1;color:#fff;padding:13px 28px;border-radius:8px;
                                text-decoration:none;display:inline-block;font-weight:600;font-size:15px">
                        Sign in to Axiom
                      </a>
                      <p style="color:#94a3b8;font-size:12px;margin-top:32px;line-height:1.5">
                        If you didn't request this link, you can safely ignore this email.<br>
                        This link will expire automatically.
                      </p>
                    </div>
                    """
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={
                    "from": f"Axiom <{RESEND_FROM}>",
                    "to": [to_email],
                    "subject": email_subject,
                    "html": email_html,
                }
            )
            return resp.status_code in (200, 201)
    except Exception:
        return False


async def _send_kpi_alert_email(to_email: str, alerts: list) -> bool:
    if not RESEND_API_KEY or not alerts:
        return False
    rows_html = "".join([
        f'<tr>'
        f'<td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;color:#1e293b">{a.get("kpi","")}</td>'
        f'<td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;color:#ef4444;font-weight:600">{a.get("status","")}</td>'
        f'<td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;color:#64748b">{a.get("value","")}</td>'
        f'</tr>'
        for a in alerts
    ])
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={
                    "from": f"Axiom Alerts <{RESEND_FROM}>",
                    "to": [to_email],
                    "subject": f"\u26a0\ufe0f {len(alerts)} KPI alert{'s' if len(alerts)>1 else ''} require attention",
                    "html": f"""
                    <div style="font-family:-apple-system,sans-serif;max-width:560px;margin:0 auto;padding:40px 24px">
                      <h2 style="color:#0f172a;margin:0 0 6px">KPI Alert</h2>
                      <p style="color:#64748b;margin:0 0 20px">
                        {len(alerts)} metric{'s' if len(alerts)>1 else ''} require your attention:
                      </p>
                      <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden">
                        <thead>
                          <tr style="background:#f8fafc">
                            <th style="padding:10px 12px;text-align:left;color:#475569;font-size:12px;font-weight:600;text-transform:uppercase">KPI</th>
                            <th style="padding:10px 12px;text-align:left;color:#475569;font-size:12px;font-weight:600;text-transform:uppercase">Status</th>
                            <th style="padding:10px 12px;text-align:left;color:#475569;font-size:12px;font-weight:600;text-transform:uppercase">Value</th>
                          </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                      </table>
                      <a href="{APP_URL}"
                         style="background:#6366f1;color:#fff;padding:11px 22px;border-radius:6px;
                                text-decoration:none;display:inline-block;margin-top:24px;font-weight:600">
                        Open Dashboard \u2192
                      </a>
                    </div>
                    """
                }
            )
        return True
    except Exception:
        return False

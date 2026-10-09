"""
Shared email notification utilities for orchestrators.

Required env vars:
  SMTP_USER  — Gmail address used to authenticate
  SMTP_PASS  — Gmail App Password (16-char, no spaces)
  EMAIL_TO   — Comma-separated recipient addresses

Optional env vars:
  SMTP_HOST  — default: smtp.gmail.com
  SMTP_PORT  — default: 587
  EMAIL_FROM — default: SMTP_USER
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

_STATUS_COLORS = {
    "success": "#2e7d32",
    "failed":  "#c62828",
    "skipped": "#f57f17",
}


def _status_color(status: str) -> str:
    return _STATUS_COLORS.get(status.lower(), "#555555")


def build_report_email(results: List[Any], orchestrator_name: str = "Orchestrator") -> tuple[str, str, str]:
    total = len(results)
    ok    = sum(1 for r in results if r.status == "success")
    fail  = sum(1 for r in results if r.status != "success")
    overall = "SUCCESS" if fail == 0 else "FAILURE"
    header_color = "#2e7d32" if fail == 0 else "#c62828"

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── Subject ──────────────────────────────────────────────────────────────
    subject = f"[{overall}] {orchestrator_name} — {ok}/{total} steps passed ({now_utc})"

    # ── Plain-text body ───────────────────────────────────────────────────────
    lines = [
        f"{orchestrator_name} — {overall}",
        f"{ok}/{total} steps succeeded · {now_utc}",
        "",
        f"{'Step':<45} {'Status':<10} {'Duration':>10}  {'Exit code':>9}",
        "-" * 80,
    ]
    for r in results:
        lines.append(
            f"{r.name:<45} {r.status.upper():<10} {r.duration_sec:>9.1f}s  {str(r.return_code):>9}"
        )

    failed_steps = [r for r in results if r.status != "success"]
    if failed_steps:
        lines.append("")
        lines.append("── Errors ──")
        for r in failed_steps:
            lines.append(f"\n[{r.name}]")
            if r.error:
                lines.append(f"  Error:  {r.error}")
            if r.stderr_tail:
                lines.append(f"  Stderr: {r.stderr_tail[:500]}")

    steps_with_output = [r for r in results if r.stdout_tail and r.stdout_tail.strip()]
    if steps_with_output:
        lines.append("")
        lines.append("── Step Details ──")
        for r in steps_with_output:
            last_lines = [l.strip() for l in r.stdout_tail.splitlines() if l.strip()][-3:]
            if last_lines:
                lines.append(f"\n[{r.name}]")
                for l in last_lines:
                    lines.append(f"  {l}")

    text_body = "\n".join(lines)

    # ── HTML body ─────────────────────────────────────────────────────────────
    rows_html = ""
    for r in results:
        color = _status_color(r.status)
        rows_html += f"""
        <tr>
          <td style="padding:9px 12px;border-bottom:1px solid #eee">{r.name}</td>
          <td style="padding:9px 12px;border-bottom:1px solid #eee;color:{color};font-weight:600">{r.status.upper()}</td>
          <td style="padding:9px 12px;border-bottom:1px solid #eee;text-align:right">{r.duration_sec:.1f}s</td>
          <td style="padding:9px 12px;border-bottom:1px solid #eee;text-align:right">{r.return_code}</td>
        </tr>"""

    # Error section — only rendered when there are failures
    errors_html = ""
    failed_steps = [r for r in results if r.status != "success"]
    if failed_steps:
        error_rows = ""
        for r in failed_steps:
            detail = r.error or ""
            if r.stderr_tail:
                detail += ("\n" if detail else "") + r.stderr_tail[:500]
            error_rows += f"""
            <tr>
              <td style="padding:6px 12px;font-weight:600;color:#c62828;vertical-align:top;white-space:nowrap">{r.name}</td>
              <td style="padding:6px 12px;font-family:monospace;font-size:12px;color:#444;white-space:pre-wrap">{detail}</td>
            </tr>"""
        errors_html = f"""
        <h3 style="margin:28px 0 8px;color:#c62828;font-size:14px;letter-spacing:.5px">ERRORS</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#fff8f8;border:1px solid #f5c6cb;border-radius:4px">
          {error_rows}
        </table>"""

    # Step details section — last 3 stdout lines per step
    details_rows = ""
    for r in results:
        if not r.stdout_tail:
            continue
        lines_out = [l.strip() for l in r.stdout_tail.splitlines() if l.strip()][-3:]
        if not lines_out:
            continue
        preview = "\n".join(lines_out)
        details_rows += f"""
            <tr>
              <td style="padding:6px 12px;font-weight:600;color:#333;vertical-align:top;white-space:nowrap;width:220px">{r.name}</td>
              <td style="padding:6px 12px;font-family:monospace;font-size:12px;color:#555;white-space:pre-wrap">{preview}</td>
            </tr>"""

    details_html = ""
    if details_rows:
        details_html = f"""
        <h3 style="margin:28px 0 8px;color:#555;font-size:14px;letter-spacing:.5px">STEP DETAILS</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#fafafa;border:1px solid #e0e0e0;border-radius:4px">
          {details_rows}
        </table>"""

    html_body = f"""
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;max-width:860px;margin:auto;padding:24px 0">

      <!-- Header banner -->
      <div style="background:{header_color};color:#fff;padding:20px 24px;border-radius:6px 6px 0 0">
        <div style="font-size:20px;font-weight:700;margin-bottom:4px">{orchestrator_name} — {overall}</div>
        <div style="font-size:13px;opacity:.88">{ok}/{total} steps succeeded &nbsp;·&nbsp; {now_utc}</div>
      </div>

      <!-- Step table -->
      <table style="width:100%;border-collapse:collapse;font-size:13px;border:1px solid #e0e0e0;border-top:none">
        <thead>
          <tr style="background:#f5f5f5;color:#555;font-size:12px;letter-spacing:.4px">
            <th style="padding:9px 12px;text-align:left;font-weight:600">STEP</th>
            <th style="padding:9px 12px;text-align:left;font-weight:600">STATUS</th>
            <th style="padding:9px 12px;text-align:right;font-weight:600">DURATION</th>
            <th style="padding:9px 12px;text-align:right;font-weight:600">EXIT CODE</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      {errors_html}

      {details_html}

    </body>
    </html>"""

    return subject, text_body, html_body


def send_email_report(
    results: List[Any],
    orchestrator_name: str = "Orchestrator",
    only_on_failure: bool = False,
    attachments: Optional[List[Tuple[str, bytes]]] = None,
) -> None:
    smtp_user    = os.environ.get("SMTP_USER", "")
    smtp_pass    = os.environ.get("SMTP_PASS", "")
    email_to_raw = os.environ.get("EMAIL_TO", "")

    if not smtp_user or not smtp_pass or not email_to_raw:
        logger.warning("Email not configured: SMTP_USER, SMTP_PASS, EMAIL_TO must all be set. Skipping.")
        return

    fail = sum(1 for r in results if r.status != "success")
    if only_on_failure and fail == 0:
        logger.info("All steps succeeded and only_on_failure=True — skipping email.")
        return

    smtp_host  = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port  = int(os.environ.get("SMTP_PORT", "587"))
    email_from = os.environ.get("EMAIL_FROM", smtp_user)
    email_to   = [addr.strip() for addr in email_to_raw.split(",") if addr.strip()]

    subject, text_body, html_body = build_report_email(results, orchestrator_name)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = email_from
    msg["To"]      = ", ".join(email_to)

    body = MIMEMultipart("alternative")
    body.attach(MIMEText(text_body, "plain"))
    body.attach(MIMEText(html_body, "html"))
    msg.attach(body)

    for filename, content in (attachments or []):
        if isinstance(content, str):
            content = content.encode("utf-8")
        part = MIMEApplication(content, Name=filename)
        part["Content-Disposition"] = f'attachment; filename="{filename}"'
        msg.attach(part)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(email_from, email_to, msg.as_string())
        logger.info("Email report sent to: %s", ", ".join(email_to))
    except Exception:
        logger.exception("Failed to send email report — continuing anyway.")

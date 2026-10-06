"""Numida (Kenya, KES) recurring campaign -- generalized from the one-off
deposits/kudi_email_campaign_kenya_oneoff.py (which was scoped to 26 specific
client_ids) to run against ALL eligible NUMIDA customers, on the shared
campaign_core infrastructure, with real frequency guards instead of a one-shot
dedupe-against-the-main-log check.

Bugs fixed relative to the one-off:
- Account name is now built only from non-empty name parts (build_full_name),
  so a null/empty first_name or surname never produces the literal word
  "None" in the Account Name line, and the line is dropped entirely rather
  than left blank when neither name part is usable.
- "wave off penalties" -> "waive the penalties" (typo).
- The one-off's single template repeated "resolve your loan" twice in one
  sentence ("To help resolve your loan, ... we are willing to wave off
  penalties to help resolve your loan.") -- rewritten without the repeat.
"""
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import campaign_core as core

# -----------------------------
# CONFIG
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent

RUN_LOG_FILE = str(SCRIPT_DIR / "numida_campaign_run.log")
REAL_CAMPAIGN_LOG_FILE = str(SCRIPT_DIR / "numida_real_campaign_log.jsonl")

logger = core.CampaignLogger(RUN_LOG_FILE)

DRY_RUN = True  # True = build/report every email without calling the send API
                # or writing to the dedupe log. Set False only after a
                # reviewed dry run.

MAX_WORKERS = 5

CURRENCY_LABEL = "KES"

# Frequency guards -- same rationale as kuda_email_campaign.py: BigQuery's
# success counters can lag behind real sends, so both BigQuery's
# monthly/last-success data AND this script's own local send log are
# checked, taking whichever signal is more conservative.
RECENCY_DAYS = 10          # skip if emailed in the last 10 days
MONTHLY_CAP = 2            # skip if already emailed 2x this calendar month

# Balance filter -- net_balance_excl_pen (native KES), with a zero-fallback
# to net_balance when balance_excl_pen is 0 but the customer still owes
# money (that remaining balance is then penalty-only). Confirmed in the
# Kenya one-off on 2026-09-24.
BALANCE_FILTER_THRESHOLD = 100

# The "existing discount threshold" from the one-off (DISCOUNT_THRESHOLD =
# 100 there), reused here as the gate on the penalty AMOUNT (net_balance -
# effective_balance) rather than on the balance itself.
PENALTY_THRESHOLD = 100


def resolve_greeting_name(first_name, surname):
    """First name for the email greeting, falling back to surname when
    first_name is null/empty (covers single-token source names that land
    entirely in `surname`)."""
    fn = "" if first_name is None else str(first_name).strip()
    if fn.lower() in ("", "none", "nan"):
        sn = "" if surname is None else str(surname).strip()
        return sn if sn.lower() not in ("", "none", "nan") else "there"
    return fn


# -----------------------------
# CUSTOMERS
# -----------------------------
def get_numida_customers():
    df = core.fetch_recovery_rows(
        institution="NUMIDA",
        extra_select_sql="d.net_balance_excl_pen, d.assigned_agent_number",
    )
    local_summary = core.summarize_local_sends(REAL_CAMPAIGN_LOG_FILE)

    customers = []
    skip_counts = Counter()

    for _, row in df.iterrows():
        client_id = str(row["client_id"])
        email = str(row.get("email", "") or "").strip()

        first_name_raw = row["first_name"]
        surname_raw = row["surname"]
        net_balance_raw = float(row["net_balance"])
        balance_excl_pen = float(row["net_balance_excl_pen"])

        if balance_excl_pen == 0 and net_balance_raw > 0:
            effective_balance = net_balance_raw
            balance_source = "net_balance_fallback"
        else:
            effective_balance = balance_excl_pen
            balance_source = "excl_pen"

        c = {
            "client_id": client_id,
            "first_name": str(first_name_raw),
            "surname": str(surname_raw),
            "greeting_name": resolve_greeting_name(first_name_raw, surname_raw),
            "send_to_email": email,
            "phone": str(row["phone"]),
            "institution": str(row["institution"]),
            "net_balance": net_balance_raw,
            "balance_excl_pen": balance_excl_pen,
            "effective_balance": effective_balance,
            "balance_source": balance_source,
            "payment_account": str(row["payment_account"]),
            "days_overdue": int(row["max_days_in_arrears_running"]),
            "assigned_agent_number": str(row.get("assigned_agent_number", "") or ""),
        }
        c["full_name"] = core.build_full_name(first_name_raw, surname_raw)

        local = local_summary.get(client_id, {"count_this_month": 0, "last_sent_date": None})
        month_count = core.effective_month_count(int(row["bq_success_this_month"]), local["count_this_month"])
        recency_days = core.effective_recency_days(row["bq_last_success_date"], local["last_sent_date"])
        failed_ever = int(row["failed_ever"])
        passes_balance_filter = effective_balance > BALANCE_FILTER_THRESHOLD

        if not email or "@" not in email:
            reason = "no_valid_email"
        elif failed_ever > 0:
            reason = "failed_ever"
        elif not passes_balance_filter:
            reason = "balance_below_threshold"
        elif recency_days is not None and recency_days < RECENCY_DAYS:
            reason = "recent_send"
        elif month_count >= MONTHLY_CAP:
            reason = "monthly_cap"
        else:
            reason = None

        c["skip_reason"] = reason
        c["eligible"] = reason is None
        if reason:
            skip_counts[reason] += 1

        customers.append(c)

    return customers, skip_counts


# -----------------------------
# EMAIL TEMPLATES
# -----------------------------
def select_template(day):
    return 1 if 1 <= day <= 15 else 2


def build_email(first_name, net_balance, effective_balance, payment_account,
                full_name, phone, days_overdue, assigned_agent_number, as_of=None):
    as_of = as_of or datetime.now()
    template_number = select_template(as_of.day)

    first_name = str(first_name).capitalize()
    outstanding_fmt = f"<b>{CURRENCY_LABEL} {net_balance:,.2f}</b>"
    settlement_fmt = f"<b>{CURRENCY_LABEL} {effective_balance:,.2f}</b>"
    has_penalty_waiver = (net_balance - effective_balance) > PENALTY_THRESHOLD

    # Numida routes to a single assigned agent rather than the generic
    # phone-modulo-6 WhatsApp bucket.
    whatsapp = assigned_agent_number
    chatbot = core.get_chatbot_link(phone)
    payment_account_b = f"<b>{payment_account}</b>"

    account_name_value = core.get_account_name("NUMIDA", payment_account, full_name) if full_name else None
    account_name_line = f"Account Name: {account_name_value}<br>" if account_name_value else ""

    payment_block = f"""<p><b>Please pay on your Numida app or via paybill:</b><br>
{payment_account_b}<br>
{account_name_line}</p>"""
    signoff = '<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>Numida</b></p>'
    footer_style = '<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a { font-size: 2px !important; color: #cccccc !important; }</style>'

    if template_number == 1:
        template_label = "Numida Template 1"
        subject = "Penalty waiver available on your Numida loan"
        if has_penalty_waiver:
            penalty_sentence = f"<p>In collaboration with Numida, we are willing to waive the penalties on your loan. This month you can resolve it by paying {settlement_fmt} instead of the full {outstanding_fmt}.</p>"
        else:
            penalty_sentence = "<p>Please make a payment this month to resolve your loan.</p>"
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>FSS is reaching out on behalf of Numida about your overdue loan. Your account is <b>{days_overdue} days past due</b>, with a full outstanding balance of {outstanding_fmt}.</p>
{penalty_sentence}
{payment_block}
<p>Once you've paid, please send proof via WhatsApp <b>{whatsapp}</b> or reply to this email. You can also reach our team here: {chatbot}</p>
{signoff}
{footer_style}
"""

    else:
        template_label = "Numida Template 2"
        subject = "Your Numida penalty waiver expires this month"
        if has_penalty_waiver:
            penalty_sentence = f"<p>Your offer to resolve your loan for {settlement_fmt} expires at the end of this month. After that, the full balance of {outstanding_fmt} will be reinstated and this offer withdrawn.</p>"
        else:
            penalty_sentence = f"<p>Your outstanding balance of {outstanding_fmt} needs urgent attention. Please make a payment before the end of the month.</p>"
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>This is an urgent notice from FSS on behalf of Numida. Your account is <b>{days_overdue} days past due</b>.</p>
{penalty_sentence}
<p>Please act now. If we do not hear from you, we will escalate the recovery process.</p>
{payment_block}
<p>Once you've paid, please send proof via WhatsApp <b>{whatsapp}</b> or reply to this email before the end of the month. You can also reach our team here: {chatbot}</p>
{signoff}
{footer_style}
"""

    return subject, body, template_label, has_penalty_waiver, template_number


# -----------------------------
# RUN
# -----------------------------
def run():
    logger.log("=" * 70)
    logger.log(f"Numida campaign run — {datetime.now().isoformat()}")
    logger.log(f"DRY_RUN = {DRY_RUN}")
    logger.log("=" * 70)

    customers, skip_counts = get_numida_customers()
    logger.log(f"Found {len(customers)} NUMIDA customers with a valid email before guards.")

    for c in customers:
        subject, message, template_label, has_penalty_waiver, template_number = build_email(
            first_name=c["greeting_name"],
            net_balance=c["net_balance"],
            effective_balance=c["effective_balance"],
            payment_account=c["payment_account"],
            full_name=c["full_name"],
            phone=c["phone"],
            days_overdue=c["days_overdue"],
            assigned_agent_number=c["assigned_agent_number"],
        )
        c["subject"] = subject
        c["_message"] = message
        c["template_label"] = template_label
        c["has_penalty_waiver"] = has_penalty_waiver
        c["template_number"] = template_number

    eligible = [c for c in customers if c["eligible"]]
    logger.log("")
    logger.log(f"Eligible to send: {len(eligible)} / {len(customers)}")
    for reason, count in skip_counts.items():
        logger.log(f"  Skipped ({reason}): {count}")

    template_variant_counts = Counter((c["template_label"], c["has_penalty_waiver"]) for c in eligible)
    logger.log("")
    logger.log("Template/variant breakdown (eligible only):")
    for (label, has_penalty), count in sorted(template_variant_counts.items()):
        logger.log(f"  {label} [{'penalty waiver' if has_penalty else 'no penalty waiver'}]: {count}")

    if DRY_RUN:
        logger.log("")
        logger.log("DRY_RUN is True — no emails sent, no dedupe log written.")
        return customers, skip_counts, eligible

    if not eligible:
        logger.log("No eligible customers to email. Exiting.")
        return customers, skip_counts, eligible

    logger.log(f"Sending to {len(eligible)} customers with {MAX_WORKERS} concurrent workers.")

    def send_to_customer(c):
        logger.log("=" * 60)
        logger.log(f"Customer: {c['full_name'] or c['greeting_name']} ({c['client_id']})")
        logger.log(f"Template: {c['template_label']}")
        logger.log(f"Subject: {c['subject']}")
        logger.log(f"Sending to: {c['send_to_email']}")
        try:
            response, actually_sent, _ = core.send_email(
                c["send_to_email"], c["subject"], c["_message"], logger, campaign_name="FSS Numida Campaign"
            )
            status = "sent" if actually_sent else "failed"
            core.record_send(REAL_CAMPAIGN_LOG_FILE, c, c["template_label"], status, response.status_code)
            return actually_sent
        except Exception as e:
            logger.log(f"  ERROR sending to {c['send_to_email']}: {e}")
            core.record_send(REAL_CAMPAIGN_LOG_FILE, c, c["template_label"], "error", None)
            return False

    sent = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_to_customer, c) for c in eligible]
        for future in as_completed(futures):
            if future.result():
                sent += 1
            else:
                failed += 1

    logger.log("=" * 60)
    logger.log(f"DONE. Sent: {sent} | Failed: {failed} | Total attempted: {len(eligible)}")
    return customers, skip_counts, eligible


if __name__ == "__main__":
    run()

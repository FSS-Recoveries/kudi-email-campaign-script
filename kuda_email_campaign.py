import sys
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

RUN_LOG_FILE = str(SCRIPT_DIR / "kuda_campaign_run.log")
REAL_CAMPAIGN_LOG_FILE = str(SCRIPT_DIR / "kuda_real_campaign_log.jsonl")

logger = core.CampaignLogger(RUN_LOG_FILE)

DRY_RUN = True  # True = build/report every email without calling the send API
                # or writing to the dedupe log. Set False only after a
                # reviewed dry run.

MAX_WORKERS = 10

# Frequency guards -- enforced in code (BigQuery's success counters can lag
# behind real sends, see campaign_core.fetch_recovery_rows docstring), so
# both BigQuery's monthly/last-success data AND this script's own local send
# log are checked, taking whichever signal is more conservative.
RECENCY_DAYS = 7          # skip if a successful send happened in the last 7 days
MONTHLY_CAP = 4           # skip if already 4 successful sends this calendar month
ELIGIBILITY_BALANCE_FIELD_THRESHOLD = 1000  # same net_balance_concession > 1000 gate as the original script's KUDA branch


# -----------------------------
# CUSTOMERS
# -----------------------------
def get_kuda_customers():
    df = core.fetch_recovery_rows(
        institution="KUDA",
        extra_where_sql=f"d.net_balance_concession > {ELIGIBILITY_BALANCE_FIELD_THRESHOLD}",
    )
    local_summary = core.summarize_local_sends(REAL_CAMPAIGN_LOG_FILE)

    customers = []
    skip_counts = Counter()

    for _, row in df.iterrows():
        client_id = str(row["client_id"])
        email = str(row.get("email", "") or "").strip()

        c = {
            "client_id": client_id,
            "first_name": str(row["first_name"]),
            "surname": str(row["surname"]),
            "send_to_email": email,
            "phone": str(row["phone"]),
            "institution": str(row["institution"]),
            "net_balance": float(row["net_balance"]),
            "net_balance_concession": float(row["net_balance_concession"]),
            "payment_account": str(row["payment_account"]),
            "days_overdue": int(row["max_days_in_arrears_running"]),
        }
        c["full_name"] = f"{c['first_name']} {c['surname']}".title()

        local = local_summary.get(client_id, {"count_this_month": 0, "last_sent_date": None})
        month_count = core.effective_month_count(int(row["bq_success_this_month"]), local["count_this_month"])
        recency_days = core.effective_recency_days(row["bq_last_success_date"], local["last_sent_date"])
        failed_ever = int(row["failed_ever"])

        if not email or "@" not in email:
            reason = "no_valid_email"
        elif failed_ever > 0:
            reason = "failed_ever"
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
def select_week(day):
    if 1 <= day <= 7:
        return 1
    elif 8 <= day <= 14:
        return 2
    elif 15 <= day <= 21:
        return 3
    else:
        return 4


def build_email(first_name, net_balance, net_balance_concession, payment_account,
                full_name, phone, days_overdue, as_of=None):
    as_of = as_of or datetime.now()
    week = select_week(as_of.day)

    first_name = str(first_name).capitalize()
    outstanding_fmt = f"<b>NGN {net_balance:,.2f}</b>"
    settlement_fmt = f"<b>NGN {net_balance_concession:,.2f}</b>"
    has_discount = net_balance_concession > core.DISCOUNT_THRESHOLD

    if has_discount:
        suggested_fmt = f"<b>NGN {round(net_balance_concession * 0.50):,.0f}</b>"
    else:
        suggested_fmt = f"<b>NGN {round(net_balance * 0.25):,.0f}</b>"

    whatsapp = core.get_whatsapp_number(phone)
    chatbot = core.get_chatbot_link(phone)
    account_name_line = core.get_account_name("KUDA", payment_account, full_name) or full_name
    payment_account_b = f"<b>{payment_account}</b>"

    payment_block = f"""<p><b>Please make payment into the account details below:</b><br>
{payment_account_b}<br>
Account Name: {account_name_line}</p>"""

    proof_line = f'<p>Once you\'ve paid, please send proof via WhatsApp <b>{whatsapp}</b> or reply to this email. If you\'ve already made a payment recently, thank you. Please send us the proof so we can update your account. You can also reach our team here: {chatbot}</p>'
    signoff = '<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>Kuda</b></p>'
    footer_style = '<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a { font-size: 2px !important; color: #cccccc !important; }</style>'

    if week == 1:
        template_label = "Kuda Week 1"
        subject = "Your Kuda loan: start resolving it this month"
        discount_sentence = (
            f"<p>This month you can settle your loan in full for {settlement_fmt} instead of {outstanding_fmt}.</p>"
            if has_discount else ""
        )
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>FSS is working with Kuda on overdue loan accounts, and we're reaching out about yours. Your account is <b>{days_overdue} days past due</b>, with an outstanding balance of {outstanding_fmt}.</p>
{discount_sentence}
<p>If you can't pay it all at once, you can start with {suggested_fmt}. Every payment reduces what you owe. If you'd like to agree a payment plan that works for you, just reply to this email.</p>
{payment_block}
{proof_line}
{signoff}
{footer_style}
"""

    elif week == 2:
        template_label = "Kuda Week 2"
        subject = "Reminder: your Kuda loan is still outstanding"
        discount_sentence = (
            f"<p>Your settlement offer of {settlement_fmt} is still available this month.</p>"
            if has_discount else ""
        )
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>We wrote to you last week about your overdue Kuda loan. It is now <b>{days_overdue} days past due</b>, and {outstanding_fmt} is still outstanding.</p>
{discount_sentence}
<p>If paying in full is difficult, a payment of {suggested_fmt} today is a good start. Reply to this email if you'd like to agree a plan.</p>
{payment_block}
{proof_line}
{signoff}
{footer_style}
"""

    elif week == 3:
        template_label = "Kuda Week 3"
        subject = "Action needed on your Kuda loan"
        if has_discount:
            status_sentence = f"<p>There are two weeks left to settle for {settlement_fmt}. After the month ends, the full balance of {outstanding_fmt} applies.</p>"
        else:
            status_sentence = f"<p>Please pay at least {suggested_fmt} this week.</p>"
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>Your Kuda loan is <b>{days_overdue} days past due</b> with {outstanding_fmt} outstanding, and we have not yet received a payment from you this month.</p>
{status_sentence}
{payment_block}
{proof_line}
{signoff}
{footer_style}
"""

    else:
        template_label = "Kuda Week 4"
        subject = "Final notice this month: your Kuda loan"
        if has_discount:
            status_sentence = f"<p>Your settlement offer of {settlement_fmt} expires at the end of this month. After that, the full balance of {outstanding_fmt} will be reinstated and this offer withdrawn.</p>"
        else:
            status_sentence = f"<p>Please pay at least {suggested_fmt} before the end of the month.</p>"
        body = f"""
{core.FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>This is an urgent notice from FSS on behalf of Kuda. Your account is <b>{days_overdue} days past due</b>.</p>
{status_sentence}
<p>If we do not receive a payment, we will escalate the recovery process.</p>
{payment_block}
<p>Once you've paid, please send proof via WhatsApp <b>{whatsapp}</b> or reply to this email before the end of the month. If you've already made a payment recently, thank you. Please send us the proof so we can update your account. You can also reach our team here: {chatbot}</p>
{signoff}
{footer_style}
"""

    return subject, body, template_label, has_discount, week


# -----------------------------
# RUN
# -----------------------------
def run():
    logger.log("=" * 70)
    logger.log(f"Kuda campaign run — {datetime.now().isoformat()}")
    logger.log(f"DRY_RUN = {DRY_RUN}")
    logger.log("=" * 70)

    customers, skip_counts = get_kuda_customers()
    logger.log(f"Found {len(customers)} KUDA customers (net_balance_concession > {ELIGIBILITY_BALANCE_FIELD_THRESHOLD}) before guards.")

    for c in customers:
        subject, message, template_label, has_discount, week = build_email(
            first_name=c["first_name"],
            net_balance=c["net_balance"],
            net_balance_concession=c["net_balance_concession"],
            payment_account=c["payment_account"],
            full_name=c["full_name"],
            phone=c["phone"],
            days_overdue=c["days_overdue"],
        )
        c["subject"] = subject
        c["_message"] = message
        c["template_label"] = template_label
        c["has_discount"] = has_discount
        c["week"] = week

    eligible = [c for c in customers if c["eligible"]]
    logger.log("")
    logger.log(f"Eligible to send: {len(eligible)} / {len(customers)}")
    for reason, count in skip_counts.items():
        logger.log(f"  Skipped ({reason}): {count}")

    template_variant_counts = Counter((c["template_label"], c["has_discount"]) for c in eligible)
    logger.log("")
    logger.log("Template/variant breakdown (eligible only):")
    for (label, has_discount), count in sorted(template_variant_counts.items()):
        logger.log(f"  {label} [{'discount' if has_discount else 'no discount'}]: {count}")

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
        logger.log(f"Customer: {c['full_name']} ({c['client_id']})")
        logger.log(f"Template: {c['template_label']}")
        logger.log(f"Subject: {c['subject']}")
        logger.log(f"Sending to: {c['send_to_email']}")
        try:
            response, actually_sent, _ = core.send_email(
                c["send_to_email"], c["subject"], c["_message"], logger, campaign_name="FSS Kuda Campaign"
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

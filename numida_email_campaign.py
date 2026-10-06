"""Numida (Kenya, KES) recurring campaign -- generalized from the one-off
deposits/kudi_email_campaign_kenya_oneoff.py (which was scoped to 26 specific
client_ids) to run against ALL eligible NUMIDA customers, with real frequency
guards instead of a one-shot dedupe-against-the-main-log check.

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
import sys
import os
import json
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent

load_dotenv(SCRIPT_DIR / ".env")

# -----------------------------
# CONFIG / CREDENTIALS
# -----------------------------
def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name} (set it in .env locally, or in Render's dashboard)")
    return value


KUDI_API_KEY = _require_env("KUDI_API_KEY")
SENDER_EMAIL = _require_env("SENDER_EMAIL")
SENDER_NAME = _require_env("SENDER_NAME")

FSS_LOGO_HTML = '<img src="https://drive.google.com/uc?export=view&id=1S0toPZnH2eyOr-o6oQ2s_CbC15W-ibNl" alt="FSS Logo" style="max-width:250px;margin-bottom:20px;" />'

KUDI_CAMPAIGN_ENDPOINT = "https://my.kudisms.net/api/campaign"

# Dates whose daily_email_campaign_failed counts are known-bad and must be
# excluded from failed_ever -- e.g. 2026-09-29 was one broken Kuda send
# (~50,192 failures that day) escalated to Kudi, not real bounces. A
# customer whose only recorded failures fall on an ignored date is treated
# as never having failed. Add more dates here as similar incidents are
# confirmed.
FAILED_DATES_TO_IGNORE = ["2026-09-29"]


def get_credentials():
    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        return service_account.Credentials.from_service_account_info(info)

    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path:
        return service_account.Credentials.from_service_account_file(env_path)

    # fallback: local search for service_account.json
    base_dir = SCRIPT_DIR
    filename = "service_account.json"

    for parent in [base_dir] + list(base_dir.parents):
        candidate = parent / filename
        if candidate.exists():
            return service_account.Credentials.from_service_account_file(str(candidate))

    raise FileNotFoundError("No GCP credentials found")


credentials = get_credentials()


def get_bq_client():
    return bigquery.Client(project="fssspark", credentials=credentials)


# -----------------------------
# LOGGING (run log + append-only JSONL send log)
# -----------------------------
class CampaignLogger:
    def __init__(self, log_path):
        self.log_path = str(log_path)
        self._lock = threading.Lock()

    def log(self, msg=""):
        with self._lock:
            print(msg)
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat()} {msg}\n")


RUN_LOG_FILE = str(SCRIPT_DIR / "numida_campaign_run.log")
REAL_CAMPAIGN_LOG_FILE = str(SCRIPT_DIR / "numida_real_campaign_log.jsonl")

logger = CampaignLogger(RUN_LOG_FILE)

_dedupe_lock = threading.Lock()


def load_jsonl(path):
    """Read an append-only JSON Lines log. Skips an unparseable trailing line
    (the only thing a crash mid-write can corrupt) instead of losing the rest."""
    entries = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return entries


def append_jsonl(path, entry):
    with _dedupe_lock:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())


def record_send(path, customer, template_label, status, http_status):
    entry = {
        "client_id": customer["client_id"],
        "name": customer["full_name"],
        "email": customer["send_to_email"],
        "institution": customer["institution"],
        "template": template_label,
        "status": status,
        "http_status": http_status,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "timestamp": datetime.now().isoformat(),
    }
    append_jsonl(path, entry)
    return entry


def summarize_local_sends(path):
    """Group a JSONL send log by client_id -> {count_this_month, last_sent_date}
    counting only status == 'sent' entries. Used as the same-day/same-run
    safety net on top of BigQuery's success counters, which can lag behind
    real sends (see fetch_recovery_rows docstring)."""
    current_month = datetime.now().strftime("%Y-%m")
    summary = {}
    for e in load_jsonl(path):
        if e.get("status") != "sent":
            continue
        cid = e.get("client_id")
        date = e.get("date", "")
        if not cid or not date:
            continue
        s = summary.setdefault(cid, {"count_this_month": 0, "last_sent_date": None})
        if date.startswith(current_month):
            s["count_this_month"] += 1
        if s["last_sent_date"] is None or date > s["last_sent_date"]:
            s["last_sent_date"] = date
    return summary


def days_since(date_str):
    if date_str is None:
        return None
    s = str(date_str)
    # pandas renders a missing BigQuery DATE (NULL -- never sent before) as
    # the string "NaT" once the column round-trips through a DataFrame.
    if not s or s.lower() == "nat":
        return None
    d = datetime.strptime(s[:10], "%Y-%m-%d")
    return (datetime.now() - d).days


def effective_recency_days(bq_last_date, local_last_date):
    """Most conservative (smallest/most-recent) of the two recency signals."""
    candidates = [d for d in (days_since(bq_last_date), days_since(local_last_date)) if d is not None]
    return min(candidates) if candidates else None


def effective_month_count(bq_count, local_count):
    """Most conservative (largest) of the two monthly-count signals."""
    return max(bq_count or 0, local_count or 0)


# -----------------------------
# BIGQUERY CUSTOMER PULL
# -----------------------------
def fetch_recovery_rows(institution, extra_select_sql="", extra_where_sql=""):
    """Pull today's rows for one institution from recovery_dashboard_daily,
    joined with this-month success counts, all-time last-success date, and
    all-time failed count (excluding FAILED_DATES_TO_IGNORE).
    """
    bq = get_bq_client()

    ignored_dates_filter = ""
    if FAILED_DATES_TO_IGNORE:
        ignored_dates_sql = ", ".join(f"'{d}'" for d in FAILED_DATES_TO_IGNORE)
        ignored_dates_filter = f"WHERE date NOT IN ({ignored_dates_sql})"

    query = f"""
    WITH success_counts AS (
        SELECT client_id, SUM(daily_email_campaign_success) AS success_this_month
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        WHERE FORMAT_DATE('%Y-%m', date) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
        GROUP BY client_id
    ),
    last_success AS (
        SELECT client_id, MAX(date) AS last_success_date
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        WHERE daily_email_campaign_success > 0
        GROUP BY client_id
    ),
    failed_counts AS (
        SELECT client_id, SUM(daily_email_campaign_failed) AS failed_ever
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        {ignored_dates_filter}
        GROUP BY client_id
    )
    SELECT
        d.client_id, d.first_name, d.surname, d.email, d.phone,
        d.institution, d.net_balance, d.net_balance_concession,
        d.payment_account, d.max_days_in_arrears_running
        {(", " + extra_select_sql) if extra_select_sql else ""},
        COALESCE(s.success_this_month, 0) AS bq_success_this_month,
        ls.last_success_date AS bq_last_success_date,
        COALESCE(f.failed_ever, 0) AS failed_ever
    FROM fssspark.recovery_methods_data.recovery_dashboard_daily d
    LEFT JOIN success_counts s ON s.client_id = d.client_id
    LEFT JOIN last_success ls ON ls.client_id = d.client_id
    LEFT JOIN failed_counts f ON f.client_id = d.client_id
    WHERE d.date = CURRENT_DATE()
    AND d.institution = '{institution}'
    AND d.email IS NOT NULL AND d.email != ''
    {(" AND " + extra_where_sql) if extra_where_sql else ""}
    ORDER BY d.net_balance DESC
    """
    return bq.query(query).to_dataframe()


# -----------------------------
# NAME / ACCOUNT NAME / CHATBOT
# -----------------------------
def build_full_name(first_name, surname):
    """Join only non-empty name parts (never the literal string 'None'),
    Title-cased. Returns '' if nothing usable is left."""
    parts = []
    for p in (first_name, surname):
        s = "" if p is None else str(p).strip()
        if s and s.lower() not in ("none", "nan"):
            parts.append(s)
    return " ".join(parts).title()


def get_chatbot_link(phone):
    phone_str = str(phone).strip()
    if phone_str.startswith('234'):
        local = '0' + phone_str[3:]
    elif phone_str.startswith('0'):
        local = phone_str
    else:
        local = '0' + phone_str
    return f"https://chat.fsldigital.com/?p={local}"


def get_account_name(institution, payment_account, full_name):
    inst = institution.upper().strip()
    pa = str(payment_account).strip()

    fixed = {
        'GROOMING MFI': 'Grooming People FSL Credit Settlement',
        'NOLT': 'Nolt Finance Company Limited',
        'LUKEFIELD': 'LukeField Finance',
        'LAPO': 'LAPO Microfinance Bank Limited',
        'ROSABON': 'Rosabon Financial Services Limited',
        'VICTORY EMPOWERMENT': 'Victory Empowerment Centre FSL',
        'KESSINGTON': 'Kessington Global Synergy Ltd',
        'KESSINGTON ARCHIVED': 'Kessington Global Synergy Ltd',
    }
    if inst in fixed:
        return fixed[inst]

    if inst in ['KUDA', 'STERLING', 'AB MFB', 'MAINSTREET', 'NUMIDA']:
        return full_name

    if inst in ['RENMONEY', 'RENMONEY ARCHIVED']:
        if pa.upper().startswith('ZENITH'):
            return 'Renmoney Zenith Repayment Account'
        return full_name

    if inst in ['GROOMING MFB', 'GROOMING MFB ARCHIVED']:
        if pa.upper().startswith('GROOMING') or pa.upper().startswith('GROOMINGMFB'):
            return full_name
        return 'Grooming MFB LTD & FINTECH SOLUTION SERVICE'

    if inst in ['REMEDIAL HEALTH', 'REMEDIAL ARCHIVED']:
        if pa.upper().startswith('ACCESS'):
            return 'Remedial Health Plc'
        return None

    if inst == 'BAOBAB':
        if pa.upper().startswith('BAOBABMFB'):
            return full_name
        return 'Baobab Microfinance Bank'

    if inst == 'CREDIT DIRECT':
        parts = pa.split()
        if len(parts) >= 5:
            return 'Credit Direct Limited'
        return full_name

    return None


# -----------------------------
# SEND
# -----------------------------
def send_email(recipient, subject, message, logger, campaign_name="FSS Email Campaign"):
    html_message = message

    payload = {
        "token": KUDI_API_KEY,
        "senderEmail": SENDER_EMAIL,
        "senderName": SENDER_NAME,
        "senderFrom": SENDER_NAME,
        "campaignName": campaign_name,
        "recipient": recipient,
        "subject": subject,
        "templateCode": "",
        "html": html_message,
    }

    # Despite the docs implying html-only is JSON-only, Kudi's own sample
    # curl uses --form (multipart/form-data) with templateCode and html sent
    # together -- templateCode empty rather than omitted. Omitting it causes
    # a server-side "Column 'template' cannot be null" error.
    files = {k: (None, str(v)) for k, v in payload.items()}
    response = requests.post(KUDI_CAMPAIGN_ENDPOINT, files=files)

    console_encoding = sys.stdout.encoding or "ascii"
    raw_text = response.content.decode("utf-8-sig", errors="replace")
    safe_text = raw_text.encode(console_encoding, errors="replace").decode(console_encoding)

    # Kudi can return HTTP 200 with a JSON body reporting its own failure --
    # HTTP 200 alone doesn't mean the email was actually sent.
    try:
        parsed = json.loads(raw_text)
    except Exception:
        parsed = None
    api_status = parsed.get("status") if isinstance(parsed, dict) else None
    api_message = parsed.get("msg") if isinstance(parsed, dict) else None
    actually_sent = response.status_code == 200 and api_status == "success"

    logger.log(f"  HTTP status: {response.status_code}")
    logger.log(f"  Raw response: {safe_text}")

    if actually_sent:
        logger.log(f"  Result: SENT to {recipient} (HTTP 200, Kudi status=success).")
    else:
        reason = api_message or f"HTTP {response.status_code}"
        logger.log(f"  Result: FAILED to send to {recipient} ({reason})")

    return response, actually_sent, api_message


# -----------------------------
# CONFIG (script-specific)
# -----------------------------
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
    df = fetch_recovery_rows(
        institution="NUMIDA",
        extra_select_sql="d.net_balance_excl_pen, d.assigned_agent_number",
    )
    local_summary = summarize_local_sends(REAL_CAMPAIGN_LOG_FILE)

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
        c["full_name"] = build_full_name(first_name_raw, surname_raw)

        local = local_summary.get(client_id, {"count_this_month": 0, "last_sent_date": None})
        month_count = effective_month_count(int(row["bq_success_this_month"]), local["count_this_month"])
        recency_days = effective_recency_days(row["bq_last_success_date"], local["last_sent_date"])
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
    chatbot = get_chatbot_link(phone)
    payment_account_b = f"<b>{payment_account}</b>"

    account_name_value = get_account_name("NUMIDA", payment_account, full_name) if full_name else None
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
{FSS_LOGO_HTML}
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
{FSS_LOGO_HTML}
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
            response, actually_sent, _ = send_email(
                c["send_to_email"], c["subject"], c["_message"], logger, campaign_name="FSS Numida Campaign"
            )
            status = "sent" if actually_sent else "failed"
            record_send(REAL_CAMPAIGN_LOG_FILE, c, c["template_label"], status, response.status_code)
            return actually_sent
        except Exception as e:
            logger.log(f"  ERROR sending to {c['send_to_email']}: {e}")
            record_send(REAL_CAMPAIGN_LOG_FILE, c, c["template_label"], "error", None)
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

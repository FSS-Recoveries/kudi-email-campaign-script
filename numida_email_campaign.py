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
import re
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


def safe_int(value, default=0):
    # BigQuery NULLs come back as NaN once a column round-trips through a
    # DataFrame -- int(nan) raises ValueError and, uncaught in a per-row
    # loop, would abort the whole campaign over a single bad row.
    if value is None or value != value:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_str(value, default=""):
    # Same NaN/None hazard as safe_int, for string fields pulled from a
    # BigQuery DataFrame (e.g. a numeric agent-number column with NULLs) --
    # without this, str(nan) silently becomes the literal text "nan".
    if value is None or (isinstance(value, float) and value != value):
        return default
    s = str(value).strip()
    return s if s and s.lower() not in ("none", "nan") else default


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
# LOGGING (run log + BigQuery send log)
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

logger = CampaignLogger(RUN_LOG_FILE)

# Every send attempt is recorded in one shared BigQuery table across
# kudi/kuda/numida (distinguished by the `campaign` column) rather than a
# local JSONL file -- Render Cron Jobs give each run a fresh, ephemeral
# filesystem, so a local dedupe log written in one run would not exist on
# the next, silently disabling the anti-duplicate-send safety net.
EMAIL_LOG_TABLE = "fssspark.recovery_methods_data.email_campaign_log"
CAMPAIGN_NAME = "numida"


def record_send(customer, template_label, status, http_status):
    now = datetime.now()
    entry = {
        "campaign": CAMPAIGN_NAME,
        "client_id": customer["client_id"],
        "name": customer["full_name"],
        "email": customer["send_to_email"],
        "institution": customer["institution"],
        "template": template_label,
        "status": status,
        "http_status": http_status,
        "send_date": now.strftime("%Y-%m-%d"),
        "sent_at": now.isoformat(),
    }
    errors = get_bq_client().insert_rows_json(EMAIL_LOG_TABLE, [entry])
    if errors:
        logger.log(f"  WARNING: failed to write send-log row to BigQuery: {errors}")
    return entry


def summarize_bq_sends():
    """Group this campaign's BigQuery send log by client_id ->
    {count_this_month, last_sent_date}, counting only status == 'sent' rows.
    Used as the same-day/same-run safety net on top of recovery_dashboard_daily's
    success counters, which can lag behind real sends (see fetch_recovery_rows
    docstring)."""
    query = f"""
    SELECT
        client_id,
        COUNTIF(FORMAT_DATE('%Y-%m', send_date) = FORMAT_DATE('%Y-%m', CURRENT_DATE())) AS count_this_month,
        MAX(send_date) AS last_sent_date
    FROM `{EMAIL_LOG_TABLE}`
    WHERE campaign = '{CAMPAIGN_NAME}' AND status = 'sent'
    GROUP BY client_id
    """
    rows = get_bq_client().query(query).result()
    return {
        r["client_id"]: {
            "count_this_month": int(r["count_this_month"] or 0),
            "last_sent_date": r["last_sent_date"].isoformat() if r["last_sent_date"] else None,
        }
        for r in rows
    }


# -----------------------------
# DO-NOT-CONTACT SUPPRESSION
# -----------------------------
def normalize_phone(phone):
    """Canonicalize a phone number for cross-table matching -- both
    recovery_dashboard_daily and manual_do_not_contact mix 11-digit Nigerian
    local numbers (leading 0), 10-digit Kenyan local numbers, and some with
    a 234/254 country code instead of the leading 0."""
    s = re.sub(r"\D", "", str(phone or ""))
    if s.startswith("234") and len(s) > 10:
        s = "0" + s[3:]
    elif s.startswith("254") and len(s) > 9:
        s = "0" + s[3:]
    if s and not s.startswith("0") and len(s) in (9, 10):
        s = "0" + s
    return s


def get_do_not_contact_phones():
    """Normalized phone numbers to suppress from every campaign, regardless
    of which institution the manual_do_not_contact row names. Active is
    treated as blocking unless explicitly False."""
    query = """
    SELECT phone
    FROM `fssspark.original_cohorts.manual_do_not_contact`
    WHERE COALESCE(Active, TRUE) != FALSE
    """
    rows = get_bq_client().query(query).result()
    return {normalize_phone(r["phone"]) for r in rows if r["phone"]}


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
    # Numida is Kenya-only (see module docstring) -- strip the 254 country
    # code rather than Nigeria's 234, which the original Kuda/Kudi script
    # uses. A Kenyan number like 254712345678 would otherwise fall through
    # to the "else" branch and come out as 0254712345678.
    phone_str = str(phone).strip()
    if phone_str.startswith('254'):
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
DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
# True = build/report every email without calling the send API or writing
# to the dedupe log. Defaults to True (safe) unless DRY_RUN=false is set
# via env var -- flip it in Render's dashboard once a dry run has been
# reviewed; no code change or redeploy needed.

MAX_WORKERS = 5

CURRENCY_LABEL = "KES"

# Fixed send days -- mirrors kudi_email_campaign.py's day-10/day-23 gate, so
# this script can be invoked by a daily cron and still only actually act on
# these two calendar days each month.
FIXED_SEND_DAYS = {10, 23}

# Per-customer frequency guards -- kept as a second safety net alongside
# FIXED_SEND_DAYS, same rationale as kuda_email_campaign.py: BigQuery's
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

# Fallback WhatsApp contact when a customer has no assigned_agent_number in
# BigQuery -- without this, a missing/NULL value (which pandas can surface
# as NaN, not an empty string) would show up in the email as the literal
# text "nan" instead of a usable number.
DEFAULT_ASSIGNED_AGENT = "0797188468"


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
    local_summary = summarize_bq_sends()
    dnc_phones = get_do_not_contact_phones()

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
            "days_overdue": safe_int(row["max_days_in_arrears_running"]),
            "assigned_agent_number": safe_str(row.get("assigned_agent_number", "")) or DEFAULT_ASSIGNED_AGENT,
        }
        c["full_name"] = build_full_name(first_name_raw, surname_raw)

        local = local_summary.get(client_id, {"count_this_month": 0, "last_sent_date": None})
        month_count = effective_month_count(int(row["bq_success_this_month"]), local["count_this_month"])
        recency_days = effective_recency_days(row["bq_last_success_date"], local["last_sent_date"])
        passes_balance_filter = effective_balance > BALANCE_FILTER_THRESHOLD

        # Unlike Kuda/Kudi, Numida retries everyone regardless of past
        # failed_ever -- a prior bounce/failure no longer permanently
        # excludes a customer here.
        if not email or "@" not in email:
            reason = "no_valid_email"
        elif normalize_phone(c["phone"]) in dnc_phones:
            reason = "do_not_contact"
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

    day = datetime.now().day
    if day not in FIXED_SEND_DAYS:
        logger.log(f"Today is day {day} — Numida campaign only sends on days {sorted(FIXED_SEND_DAYS)}. Exiting.")
        return [], Counter(), []

    customers, skip_counts = get_numida_customers()
    logger.log(f"Found {len(customers)} NUMIDA customers with a valid email before guards.")

    eligible = [c for c in customers if c["eligible"]]
    logger.log("")
    logger.log(f"Eligible to send: {len(eligible)} / {len(customers)}")
    for reason, count in skip_counts.items():
        logger.log(f"  Skipped ({reason}): {count}")

    def build_for(c):
        return build_email(
            first_name=c["greeting_name"],
            net_balance=c["net_balance"],
            effective_balance=c["effective_balance"],
            payment_account=c["payment_account"],
            full_name=c["full_name"],
            phone=c["phone"],
            days_overdue=c["days_overdue"],
            assigned_agent_number=c["assigned_agent_number"],
        )

    # Build each eligible customer's email once here just for the
    # template/variant breakdown, discarding the HTML body immediately --
    # holding every built body in memory at once (instead of building
    # lazily per send below) is what caused a prior Out of Memory crash on
    # a 512Mi Render instance (in kuda_email_campaign.py's larger batch).
    template_variant_counts = Counter()
    for c in eligible:
        _, _, template_label, has_penalty_waiver, _ = build_for(c)
        template_variant_counts[(template_label, has_penalty_waiver)] += 1

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
        # Built fresh here rather than reused from a pre-built, stored
        # body -- keeps at most MAX_WORKERS bodies in memory at once.
        subject, message, template_label, has_penalty_waiver, template_number = build_for(c)
        logger.log("=" * 60)
        logger.log(f"Customer: {c['full_name'] or c['greeting_name']} ({c['client_id']})")
        logger.log(f"Template: {template_label}")
        logger.log(f"Subject: {subject}")
        logger.log(f"Sending to: {c['send_to_email']}")
        try:
            response, actually_sent, _ = send_email(
                c["send_to_email"], subject, message, logger, campaign_name="FSS Numida Campaign"
            )
            status = "sent" if actually_sent else "failed"
            record_send(c, template_label, status, response.status_code)
            return actually_sent
        except Exception as e:
            logger.log(f"  ERROR sending to {c['send_to_email']}: {e}")
            record_send(c, template_label, "error", None)
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

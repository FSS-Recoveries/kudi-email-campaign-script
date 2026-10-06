"""Shared infrastructure for the FSS email campaign scripts (original/Kuda/Numida):
credentials, BigQuery customer pull, send_email(), logging, account-name logic,
and the chatbot link -- copied out of kudi_email_campaign.py so the
institution-specific scripts can import it instead of duplicating it.
"""
import os
import json
import threading
import time
import sys
from datetime import datetime
from pathlib import Path

import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

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

DISCOUNT_THRESHOLD = 1000

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
    base_dir = Path(__file__).resolve().parent
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
    """Per-script console+file logger. Each script instantiates its own with
    its own log file path so Kuda/Numida/original logs never interleave."""

    def __init__(self, log_path):
        self.log_path = str(log_path)
        self._lock = threading.Lock()

    def log(self, msg=""):
        with self._lock:
            print(msg)
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat()} {msg}\n")


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
    real sends (see investigation note in kuda/numida scripts)."""
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
    all-time failed count. extra_select_sql/extra_where_sql let callers add
    institution-specific columns/filters (e.g. Numida's net_balance_excl_pen).
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
# ACCOUNT NAME / WHATSAPP / CHATBOT
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


def get_whatsapp_number(phone):
    WHATSAPP_NUMBERS = {
        0: "07026198201",
        1: "09062031008",
        2: "08130331665",
        3: "07079492230",
        4: "09060207537",
        5: "08141916280"  # Itoro - new 6th agent
    }
    try:
        last_two = int(str(phone)[-2:])
        return WHATSAPP_NUMBERS[last_two % 6]
    except Exception:
        return "07026198201"


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

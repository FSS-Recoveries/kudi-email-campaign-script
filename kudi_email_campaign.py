import sys
import os
import json
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
from dotenv import load_dotenv
import json as _json

load_dotenv()

# -----------------------------
# CONFIG
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

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent

TEST_10_LOG_FILE = str(SCRIPT_DIR / "test_campaign_log.json")
# Tracks every real-campaign send attempt so a kill + same-day rerun can skip
# anyone already emailed today, instead of relying on BigQuery's
# daily_email_campaign_success/failed counters (which may lag behind).
REAL_CAMPAIGN_LOG_FILE = str(SCRIPT_DIR / "real_campaign_log.json")
RUN_LOG_FILE = str(SCRIPT_DIR / "campaign_run.log")


_run_log_lock = threading.Lock()


def log_line(msg=""):
    with _run_log_lock:
        print(msg)
        with open(RUN_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} {msg}\n")


def _atomic_write_json(path, data):
    # Writing 'w' truncates the file before the new content is flushed --
    # if the process is killed mid-write, the file is left zero-filled
    # (this happened to real_campaign_log.json once already). Writing to a
    # temp file and renaming over the original is atomic on both platforms,
    # so a kill mid-write leaves either the old file or the new one intact.
    tmp_path = f"{path}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        _json.dump(data, f, indent=2)

    # os.replace can transiently fail with PermissionError (WinError 5) on
    # Windows if another process -- antivirus, an editor, a file-watcher --
    # briefly has the destination open. This already caused real duplicate
    # sends once: a failed rename here left a customer's send unrecorded (or
    # mislabeled), so they weren't excluded on the next run. Retry with
    # backoff instead of losing the write outright.
    last_err = None
    for attempt in range(5):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(0.2 * (attempt + 1))
    raise last_err


def load_real_log():
    try:
        with open(REAL_CAMPAIGN_LOG_FILE, 'r') as f:
            return _json.load(f)
    except:
        return {"sent": []}


def save_real_log(log_data):
    _atomic_write_json(REAL_CAMPAIGN_LOG_FILE, log_data)


def get_real_sent_ids():
    # Excludes anyone with ANY recorded successful send, not just today's --
    # BigQuery's success_this_month counter can lag behind actual sends (e.g.
    # after a crash/restart), so it can't be trusted alone to block a repeat.
    log_data = load_real_log()
    return {
        e["client_id"] for e in log_data.get("sent", [])
        if e.get("status") == "sent"
    }


# Guards the JSON dedupe logs' read-modify-write cycle -- without this,
# concurrent workers can both load the same on-disk state, append their own
# entry in memory, and save, with the second save silently overwriting (and
# losing) the first worker's entry.
_dedupe_lock = threading.Lock()


def record_real_send(customer, template_label, status, http_status):
    with _dedupe_lock:
        log_data = load_real_log()
        log_data.setdefault("sent", []).append({
            "client_id": customer["client_id"],
            "name": customer["full_name"],
            "email": customer["send_to_email"],
            "institution": customer["institution"],
            "template": template_label,
            "status": status,
            "http_status": http_status,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat(),
        })
        save_real_log(log_data)


def load_test_log():
    try:
        with open(TEST_10_LOG_FILE, 'r') as f:
            return _json.load(f)
    except:
        return {"sent_client_ids": [], "sent_at": []}

def save_test_log(log):
    _atomic_write_json(TEST_10_LOG_FILE, log)

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

def get_test_customer_from_bq():
    base_dir = BASE_DIR
    bq = bigquery.Client(project="fssspark", credentials=credentials)

    query = """
    SELECT
        client_id, first_name, surname, email, phone,
        institution, net_balance, net_balance_concession,
        payment_account, max_days_in_arrears_running
    FROM fssspark.recovery_methods_data.recovery_dashboard_daily
    WHERE date = CURRENT_DATE()
    AND first_name = 'jane'
    AND surname = 'ajodo'
    AND institution = 'KUDA'
    LIMIT 1
    """

    results = bq.query(query).to_dataframe()
    if len(results) == 0:
        log_line("Test customer not found in BigQuery - using fallback")
        return None

    row = results.iloc[0]
    return {
        "client_id": str(row["client_id"]),
        "first_name": str(row["first_name"]),
        "surname": str(row["surname"]),
        "send_to_email": "janeajodo@fsldigital.com",
        "phone": str(row["phone"]),
        "institution": str(row["institution"]),
        "net_balance": float(row["net_balance"]),
        "net_balance_concession": float(row["net_balance_concession"]),
        "payment_account": str(row["payment_account"]),
        "days_overdue": int(row["max_days_in_arrears_running"]),
    }


def get_emmanuel_customer_from_bq():
    base_dir = BASE_DIR
    creds = service_account.Credentials.from_service_account_file(
        str(base_dir / "service_account.json")
    )
    bq = bigquery.Client(project="fssspark", credentials=creds)

    query = """
    SELECT
        client_id, first_name, surname, email, phone,
        institution, net_balance, net_balance_concession,
        payment_account, max_days_in_arrears_running
    FROM fssspark.recovery_methods_data.recovery_dashboard_daily
    WHERE date = CURRENT_DATE()
    AND client_id = 'IDS0001'
    LIMIT 1
    """

    results = bq.query(query).to_dataframe()
    if len(results) == 0:
        log_line("Emmanuel test customer not found in BigQuery - using fallback")
        return None

    row = results.iloc[0]
    return {
        "client_id": str(row["client_id"]),
        "first_name": str(row["first_name"]),
        "surname": str(row["surname"]),
        "send_to_email": "emmanuel@fsldigital.com",
        "phone": str(row["phone"]),
        "institution": str(row["institution"]),
        "net_balance": float(row["net_balance"]),
        "net_balance_concession": float(row["net_balance_concession"]),
        "payment_account": str(row["payment_account"]),
        "days_overdue": int(row["max_days_in_arrears_running"]),
    }


def get_whatsapp_number(phone):
    WHATSAPP_NUMBERS = {
        0: "07026198201",
        1: "09062031008",
        2: "08130331665",
        3: "07079492230",
        4: "09060207537"
    }
    try:
        last_two = int(str(phone)[-2:])
        return WHATSAPP_NUMBERS[last_two % 5]
    except:
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


TEST_MODE = False     # True = send only to Jane and Emmanuel
                     # False = send to real customers from BigQuery
FORCE_SEND = True    # True = bypass date window and send on any day
                     # False = only send on day 7 and day 23
TEST_10_MODE = False  # True = send to 10 real customers as pilot test
                      # These 10 will be logged and excluded from real campaign

MAX_WORKERS = 10  # concurrent Kudi API requests in flight at once -- kept
                   # conservative since Kudi's per-account rate limits aren't
                   # documented; raise if sends stay clean at this level.

SMOKE_TEST_LIMIT = None  # TEMPORARY: caps the real-customer query for a live
                        # concurrency smoke test. Set to None before the full run.

KUDI_CAMPAIGN_ENDPOINT = "https://my.kudisms.net/api/campaign"

# -----------------------------
# TEST CUSTOMERS
# -----------------------------
def get_test_customers():
    test1 = get_test_customer_from_bq()
    if test1 is None:
        test1 = {
            "client_id": "KD-2349064075421",
            "first_name": "jane",
            "surname": "ajodo",
            "send_to_email": "janeajodo@fsldigital.com",
            "phone": "9064075421",
            "institution": "KUDA",
            "net_balance": 347143.88,
            "net_balance_concession": 69429.0,
            "payment_account": "Kuda 2008415642",
            "days_overdue": 1511,
        }

    test2 = get_emmanuel_customer_from_bq()
    if test2 is None:
        test2 = {
            "client_id": "IDS0001",
            "first_name": "Emmanuel",
            "surname": "Okorie",
            "send_to_email": "emmanuel@fsldigital.com",
            "phone": "8134073764",
            "institution": "GROOMING MFB",
            "days_overdue": 5,
            "net_balance": 30000.0,
            "net_balance_concession": 0.0,
            "payment_account": "Zenith 1310699393",
        }

    for c in [test1, test2]:
        c["full_name"] = f"{c['first_name']} {c['surname']}".title()
        c["suggested_amount"] = round(c["net_balance"] * 0.25)
        c["settlement_amount"] = c["net_balance_concession"]
        c["account_number"] = c["payment_account"].split()[-1]
        c["chatbot_link"] = get_chatbot_link(c["phone"])

    valid = []
    for c in [test1, test2]:
        email = str(c.get("send_to_email", "") or "").strip()
        if not email or "@" not in email:
            log_line(f"Skipping test customer {c.get('full_name', '?')}: missing/invalid send_to_email.")
            continue
        valid.append(c)

    return valid


# -----------------------------
# REAL CAMPAIGN CUSTOMERS
# -----------------------------
def get_real_customers():
    base_dir = BASE_DIR
    creds = service_account.Credentials.from_service_account_file(
        str(base_dir / "service_account.json")
    )
    bq = bigquery.Client(project="fssspark", credentials=creds)

    log = load_test_log()
    already_tested = log.get("sent_client_ids", [])
    sent_ever = get_real_sent_ids()
    excluded_ids = list(set(already_tested) | sent_ever)

    # Inlining thousands of ids as string literals in the query text hits
    # BigQuery's query-planning resource limit ("too many fields accessed or
    # query is too complex") once the exclusion list gets into the
    # thousands -- an array bind parameter avoids that entirely.
    test_exclusion = "AND d.client_id NOT IN UNNEST(@excluded_ids)" if excluded_ids else ""
    if sent_ever:
        log_line(f"Excluding {len(sent_ever)} client(s) with a prior recorded send (resume safety).")

    query = f"""
    WITH success_counts AS (
        SELECT client_id, SUM(daily_email_campaign_success) AS success_this_month
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        WHERE FORMAT_DATE('%Y-%m', date) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
        GROUP BY client_id
    ),
    failed_counts AS (
        SELECT client_id, SUM(daily_email_campaign_failed) AS failed_ever
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        GROUP BY client_id
    )
    SELECT
        d.client_id, d.first_name, d.surname, d.email, d.phone,
        d.institution, d.net_balance, d.net_balance_concession,
        d.payment_account, d.max_days_in_arrears_running
    FROM fssspark.recovery_methods_data.recovery_dashboard_daily d
    LEFT JOIN success_counts s ON s.client_id = d.client_id
    LEFT JOIN failed_counts f ON f.client_id = d.client_id
    WHERE d.date = CURRENT_DATE()
    AND d.net_balance > 0
    AND d.email IS NOT NULL AND d.email != ''
    AND d.institution NOT IN (
        'LAPO','VICTORY EMPOWERMENT',
        'RENMONEY ARCHIVED','GROOMING MFB ARCHIVED',
        'KESSINGTON ARCHIVED','REMEDIAL ARCHIVED',
        'NUMIDA ARCHIVED','ROSABON ARCHIVED'
    )
    AND COALESCE(s.success_this_month, 0) < 2
    AND COALESCE(f.failed_ever, 0) = 0
    {test_exclusion}
    ORDER BY d.net_balance DESC
    {f"LIMIT {SMOKE_TEST_LIMIT}" if SMOKE_TEST_LIMIT else ""}
    """

    job_config = None
    if excluded_ids:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("excluded_ids", "STRING", excluded_ids)]
        )

    log_line("Fetching real customers from BigQuery...")
    results = bq.query(query, job_config=job_config).to_dataframe()
    log_line(f"Found {len(results)} customers to email.")

    customers = []
    skipped_no_email = 0
    for _, row in results.iterrows():
        email = str(row.get("email", "") or "").strip()
        if not email or "@" not in email:
            skipped_no_email += 1
            continue  # skip customers with no valid email

        c = {
            "client_id": str(row["client_id"]),
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
        c["suggested_amount"] = round(c["net_balance"] * 0.25)
        c["settlement_amount"] = c["net_balance_concession"]
        c["account_number"] = c["payment_account"].split()[-1]
        c["chatbot_link"] = get_chatbot_link(c["phone"])
        customers.append(c)

    if skipped_no_email:
        log_line(f"Skipped {skipped_no_email} row(s) with missing/invalid email.")

    return customers


# -----------------------------
# TEST 10 (PILOT) CUSTOMERS
# -----------------------------
def get_test_10_customers():
    base_dir = BASE_DIR
    creds = service_account.Credentials.from_service_account_file(
        str(base_dir / "service_account.json")
    )
    bq = bigquery.Client(project="fssspark", credentials=creds)

    log = load_test_log()
    already_tested = log.get("sent_client_ids", [])

    exclusion_str = ""
    if already_tested:
        ids = ", ".join(f"'{c}'" for c in already_tested)
        exclusion_str = f"AND d.client_id NOT IN ({ids})"

    query = f"""
    WITH success_counts AS (
        SELECT client_id, SUM(daily_email_campaign_success) AS success_this_month
        FROM fssspark.recovery_methods_data.recovery_dashboard_daily
        WHERE FORMAT_DATE('%Y-%m', date) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
        GROUP BY client_id
    )
    SELECT
        d.client_id, d.first_name, d.surname, d.email, d.phone,
        d.institution, d.net_balance, d.net_balance_concession,
        d.payment_account, d.max_days_in_arrears_running
    FROM fssspark.recovery_methods_data.recovery_dashboard_daily d
    LEFT JOIN success_counts s ON s.client_id = d.client_id
    WHERE d.date = CURRENT_DATE()
    AND d.net_balance > 0
    AND d.email IS NOT NULL AND d.email != ''
    AND d.institution NOT IN (
        'LAPO','VICTORY EMPOWERMENT',
        'RENMONEY ARCHIVED','GROOMING MFB ARCHIVED',
        'KESSINGTON ARCHIVED','REMEDIAL ARCHIVED',
        'NUMIDA ARCHIVED','ROSABON ARCHIVED'
    )
    AND COALESCE(s.success_this_month, 0) < 2
    {exclusion_str}
    ORDER BY d.net_balance DESC
    LIMIT 10
    """

    log_line("Fetching 10 real test customers from BigQuery...")
    results = bq.query(query).to_dataframe()
    log_line(f"Found {len(results)} test customers.")

    customers = []
    for _, row in results.iterrows():
        email = str(row.get("email", "") or "").strip()
        if not email or "@" not in email:
            continue
        c = {
            "client_id": str(row["client_id"]),
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
        c["suggested_amount"] = round(c["net_balance"] * 0.25)
        c["settlement_amount"] = c["net_balance_concession"]
        c["account_number"] = c["payment_account"].split()[-1]
        c["chatbot_link"] = get_chatbot_link(c["phone"])
        customers.append(c)

    return customers


# -----------------------------
# EMAIL TEMPLATES
# -----------------------------
# Institutions excluded from the real campaign: LAPO, GROOMING MFI, VICTORY EMPOWERMENT
# Filter applied in real campaign: email IS NOT NULL AND email != '' (plus a
# Python-side "@" check in get_real_customers() as a second safety net)
DISCOUNT_INSTITUTIONS = ['KUDA', 'RENMONEY', 'CREDIT DIRECT']
DISCOUNT_THRESHOLD = 1000


def build_email(first_name, institution, net_balance, net_balance_concession,
                payment_account, full_name, phone, is_end_of_month, days_overdue):
    first_name = str(first_name).capitalize()
    outstanding_fmt = f"<b>NGN {net_balance:,.2f}</b>"
    suggested_fmt_discount= f"<b>NGN {round(net_balance_concession * 0.50):,.0f}</b>"
    suggested_fmt= f"<b>NGN {round(net_balance * 0.25):,.0f}</b>"
    settlement_fmt = f"<b>NGN {net_balance_concession:,.2f}</b>"
    whatsapp = get_whatsapp_number(phone)
    chatbot = get_chatbot_link(phone)
    has_discount = net_balance_concession > DISCOUNT_THRESHOLD

    # Bolded versions for use in the body only -- subject lines don't render
    # HTML, so `institution` (plain) stays in every subject= line below.
    institution_b = f"<b>{institution}</b>"
    payment_account_b = f"<b>{payment_account}</b>"
    # get_account_name() returns None for a couple of institution/account
    # combinations (e.g. Remedial Health without an Access-prefixed account)
    # -- fall back to full_name rather than leave the line blank.
    account_name_line = get_account_name(institution, payment_account, full_name) or full_name

    if not is_end_of_month and has_discount:
        template_label = "Email 1A"
        subject = f"Urgent: Loan Settlement Offer Available — {institution}"
        body = f"""
{FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>FSS is reaching out on behalf of <b>{institution}</b> regarding your overdue loan account. Your account is currently <b>{days_overdue} days past due</b> with an outstanding balance of {outstanding_fmt} and requires your immediate attention.</p>
<p>A special settlement offer is available to you right now. You can clear your {institution} loan in full for {settlement_fmt} instead of the full outstanding balance of {outstanding_fmt}. This is a significant saving and we urge you to take advantage of it this month.</p>
<p>You can also begin with a minimum payment of {suggested_fmt_discount} if you are unable to pay the full settlement at once. Any payment made now goes directly toward resolving your account.</p>
<p><b>Please make payment into the account details below:</b><br>
{payment_account_b}<br>
Account Name: {account_name_line}</p>
<p>Once you have made a payment please send proof via WhatsApp <b>{whatsapp}</b> or reply directly to this email.</p>
<p>You can also reach our team here: {chatbot}</p>
<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>{institution}</b></p>
<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a {{ font-size: 2px !important; color: #cccccc !important; }}</style>
"""

    elif not is_end_of_month and not has_discount:
        template_label = "Email 1B"
        subject = f"Urgent: Loan Account Action Required — {institution}"
        body = f"""
{FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>FSS is reaching out on behalf of <b>{institution}</b> regarding your overdue loan account. Your account is currently <b>{days_overdue} days past due</b> with an outstanding balance of {outstanding_fmt} and requires your immediate attention.</p>
<p>We urge you to make a payment this month. You can begin with a minimum payment of {suggested_fmt} rather than settling the full balance at once. Every payment you make brings you closer to clearing this debt.</p>
<p><b>Please make payment into the account details below:</b><br>
{payment_account_b}<br>
Account Name: {account_name_line}</p>
<p>Once you have made a payment please send proof via WhatsApp <b>{whatsapp}</b> or reply directly to this email.</p>
<p>You can also reach our team here: {chatbot}</p>
<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>{institution}</b></p>
<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a {{ font-size: 2px !important; color: #cccccc !important; }}</style>
"""

    elif is_end_of_month and has_discount:
        template_label = "Email 2A"
        subject = f"Urgent: Your Loan Discount Offer Expires This Month — {institution}"
        body = f"""
{FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>This is an urgent notice from FSS on behalf of <b>{institution}</b>.</p>
<p>Your account is currently <b>{days_overdue} days past due</b>. Your special settlement offer of {settlement_fmt} expires at the end of this month. Once it expires the full outstanding balance of {outstanding_fmt} will be reinstated and this opportunity will be withdrawn.</p>
<p>You must act now. You can resolve your {institution} loan account at a significantly reduced amount before this offer expires. If we do not hear from you we will proceed with escalating the recovery process and exploring all recovery options without further notice.</p>
<p><b>Please make payment into the account details below:</b><br>
{payment_account_b}<br>
Account Name: {account_name_line}</p>
<p>Once you have made a payment please send proof via WhatsApp <b>{whatsapp}</b> or reply directly to this email before the end of this month.</p>
<p>You can also reach our team here: {chatbot}</p>
<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>{institution}</b></p>
<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a {{ font-size: 2px !important; color: #cccccc !important; }}</style>
"""

    else:
        template_label = "Email 2B"
        subject = f"Urgent: Loan Account Requires Immediate Action — {institution}"
        body = f"""
{FSS_LOGO_HTML}
<p>Hi {first_name},</p>
<p>This is an urgent notice from FSS on behalf of <b>{institution}</b>.</p>
<p>Your account is currently <b>{days_overdue} days past due</b> with an outstanding balance of {outstanding_fmt} that remains unresolved. We are at the end of this month and your account still requires immediate action.</p>
<p>You must make a payment now. You can start with a minimum of {suggested_fmt} rather than settling the full balance at once. Do not allow this to carry into the next month without taking action. If we do not receive payment we will proceed with escalating this matter and exploring all recovery options which might include personal visitation to your residential address or place of work.</p>
<p><b>Please make payment into the account details below:</b><br>
{payment_account_b}<br>
Account Name: {account_name_line}</p>
<p>Once you have made a payment please send proof via WhatsApp <b>{whatsapp}</b> or reply directly to this email.</p>
<p>You can also reach our team here: {chatbot}</p>
<p>Kind regards,<br><b>FSS Recovery Team</b><br>On behalf of <b>{institution}</b></p>
<style>.unsubscribe, .unsub, [class*="unsub"], [class*="footer"] a {{ font-size: 8px !important; color: #cccccc !important; }}</style>
"""

    return subject, body, template_label


# -----------------------------
# SEND
# -----------------------------
def send_email(recipient, subject, message):
    # The templates already use <p>/<br> tags for structure, so the source's
    # own newlines (just formatting whitespace between tags) should NOT also
    # become <br> -- doing so stacks an extra line break on top of each
    # paragraph's own spacing, causing excessive gaps between paragraphs.
    html_message = message

    payload = {
        "token": KUDI_API_KEY,
        "senderEmail": SENDER_EMAIL,
        "senderName": SENDER_NAME,
        "senderFrom": SENDER_NAME,
        "campaignName": "FSS Email Campaign",
        "recipient": recipient,
        "subject": subject,
        "templateCode": "",
        "html": html_message,
    }

    # Despite the docs implying html-only is JSON-only, Kudi's own sample
    # curl uses --form (multipart/form-data) with templateCode and html sent
    # together -- templateCode empty rather than omitted. Confirmed working
    # via a diagnostic send; omitting templateCode causes a server-side
    # "Column 'template' cannot be null" error.
    files = {k: (None, str(v)) for k, v in payload.items()}
    response = requests.post(KUDI_CAMPAIGN_ENDPOINT, files=files)

    # response.content sometimes carries a UTF-8 BOM, and the Windows console
    # (cp1252) can't display arbitrary Unicode either -- decode the BOM away
    # and replace anything else the console can't show rather than crashing.
    console_encoding = sys.stdout.encoding or "ascii"
    raw_text = response.content.decode("utf-8-sig", errors="replace")
    safe_text = raw_text.encode(console_encoding, errors="replace").decode(console_encoding)

    # Kudi can return HTTP 200 with a JSON body reporting its own failure
    # (e.g. "status":"error","msg":"No valid email address provided.") --
    # HTTP 200 alone doesn't mean the email was actually sent.
    try:
        parsed = _json.loads(raw_text)
    except Exception:
        parsed = None
    api_status = parsed.get("status") if isinstance(parsed, dict) else None
    api_message = parsed.get("msg") if isinstance(parsed, dict) else None
    actually_sent = response.status_code == 200 and api_status == "success"

    log_line(f"  HTTP status: {response.status_code}")
    log_line(f"  Raw response: {safe_text}")

    if actually_sent:
        log_line(f"  Result: SENT to {recipient} (HTTP 200, Kudi status=success).")
    else:
        reason = api_message or f"HTTP {response.status_code}"
        log_line(f"  Result: FAILED to send to {recipient} ({reason})")

    return response, actually_sent, api_message


# -----------------------------
# RUN
# -----------------------------
def run():

    # Date window check
    day = datetime.now().day
    is_day_7 = day == 7
    is_day_23 = day == 23
    is_end_of_month = is_day_23

    if not FORCE_SEND and not is_day_7 and not is_day_23:
        log_line(f"Today is day {day} — emails only send on day 7 and day 23.")
        log_line("Set FORCE_SEND = True to send on any other day.")
        return

    if FORCE_SEND:
        log_line(f"FORCE_SEND is True — bypassing date window (day {day}). Sending now.")
        is_end_of_month = day > 15

    # Load customers
    if TEST_MODE:
        log_line("TEST_MODE is ON — sending only to Jane and Emmanuel.")
        customers = get_test_customers()
    elif TEST_10_MODE:
        log_line("TEST_10_MODE is ON — sending to 10 real customers as pilot.")
        customers = get_test_10_customers()
    else:
        log_line("TEST_MODE is OFF — sending to all real customers from BigQuery.")
        customers = get_real_customers()

    if not customers:
        log_line("No customers to email. Exiting.")
        return

    log_line(f"Total customers to email: {len(customers)}\n")
    log_line(f"Sending with {MAX_WORKERS} concurrent workers.\n")

    def send_to_customer(customer):
        subject, message, template_label = build_email(
            first_name=customer["first_name"],
            institution=customer["institution"],
            net_balance=customer["net_balance"],
            net_balance_concession=customer["net_balance_concession"],
            payment_account=customer["payment_account"],
            full_name=customer["full_name"],
            phone=customer["phone"],
            is_end_of_month=is_end_of_month,
            days_overdue=customer.get("days_overdue", 0),
        )

        log_line("=" * 60)
        log_line(f"Customer: {customer['full_name']} ({customer['client_id']})")
        log_line(f"Institution: {customer['institution']}")
        log_line(f"Template: {template_label}")
        log_line(f"Subject: {subject}")
        log_line(f"Sending to: {customer['send_to_email']}")

        try:
            response, actually_sent, _ = send_email(customer["send_to_email"], subject, message)
            status = "sent" if actually_sent else "failed"

            if TEST_10_MODE:
                if actually_sent:
                    with _dedupe_lock:
                        log = load_test_log()
                        if customer["client_id"] not in log["sent_client_ids"]:
                            log["sent_client_ids"].append(customer["client_id"])
                            log["sent_at"].append({
                                "client_id": customer["client_id"],
                                "name": customer["full_name"],
                                "email": customer["send_to_email"],
                                "institution": customer["institution"],
                                "timestamp": datetime.now().isoformat()
                            })
                            save_test_log(log)
                    log_line(f"  Logged to test campaign log.")
            elif not TEST_MODE:
                record_real_send(customer, template_label, status, response.status_code)
                log_line(f"  Logged to real campaign log.")
            log_line()
            return actually_sent
        except Exception as e:
            log_line(f"  ERROR sending to {customer['send_to_email']}: {e}")
            if not TEST_MODE and not TEST_10_MODE:
                record_real_send(customer, template_label, "error", None)
            log_line()
            return False

    sent = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_to_customer, customer) for customer in customers]
        for future in as_completed(futures):
            if future.result():
                sent += 1
            else:
                failed += 1

    log_line("=" * 60)
    log_line(f"DONE. Sent: {sent} | Failed: {failed} | Total: {len(customers)}")


if __name__ == "__main__":
    run()

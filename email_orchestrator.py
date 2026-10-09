#!/usr/bin/env python3
"""
universal_orchestrator.py

Sequentially run a set of scripts/commands with delays, robust logging,
and a final completion summary. Universal template:
- Works with Python or non-Python commands
- Logs stdout/stderr for each step
- Writes rotating log files and a JSON summary

Usage (basic):
  python universal_orchestrator.py

Optional:
  python universal_orchestrator.py --delay 5 --stop-on-failure
  python universal_orchestrator.py --log-dir logs --log-file orchestrator.log
  python universal_orchestrator.py --summary-dir summaries

Edit the STEPS list to point at your scripts.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import io
import json
import logging
import os
import platform
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from utils.email_utils import send_email_report

EMAIL_LOG_TABLE = "fssspark.recovery_methods_data.email_campaign_log"


def _get_bq_credentials():
    # google.cloud.bigquery / google.oauth2 are imported lazily, inside this
    # function rather than at module level -- this file runs as the parent
    # process for subprocess.run([...,"kuda_email_campaign.py"]) etc, and
    # each of those is a SEPARATE process that independently loads its own
    # full copy of bigquery/grpc/protobuf. Importing the same ~130MB stack
    # here too, at module load time, means it sits in memory for the
    # orchestrator's entire lifetime, alongside each subprocess's own copy,
    # all counted against the same container memory limit -- this combo is
    # what caused a real OOM on a 512Mi Render instance. Deferring the
    # import to only when the end-of-run CSV is actually built means the
    # orchestrator stays lightweight while each subprocess (which already
    # needs this stack regardless) is running.
    from google.oauth2 import service_account

    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        return service_account.Credentials.from_service_account_info(info)

    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path:
        return service_account.Credentials.from_service_account_file(env_path)

    base_dir = Path(__file__).resolve().parent
    filename = "service_account.json"
    for parent in [base_dir] + list(base_dir.parents):
        candidate = parent / filename
        if candidate.exists():
            return service_account.Credentials.from_service_account_file(str(candidate))

    raise FileNotFoundError("No GCP credentials found")


def fetch_today_send_log_csv() -> Optional[tuple[str, bytes]]:
    """Query today's rows from the shared email_campaign_log table across
    all three campaigns and return (filename, csv_bytes) to attach to the
    completion email, or None if there's nothing to report or the query
    itself fails -- a BigQuery hiccup here should never block the
    completion email from going out."""
    from google.cloud import bigquery

    logger = logging.getLogger("orchestrator")
    try:
        creds = _get_bq_credentials()
        client = bigquery.Client(project="fssspark", credentials=creds)
        query = f"""
        SELECT campaign, client_id, name, email, institution, template,
               status, http_status, send_date, sent_at
        FROM `{EMAIL_LOG_TABLE}`
        WHERE send_date = CURRENT_DATE()
        ORDER BY campaign, sent_at
        """
        rows = list(client.query(query).result())
    except Exception:
        logger.exception("Could not fetch today's send log for the CSV attachment.")
        return None

    if not rows:
        logger.info("No send-log rows for today -- skipping CSV attachment.")
        return None

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "campaign", "client_id", "name", "email", "institution",
        "template", "status", "http_status", "send_date", "sent_at",
    ])
    for r in rows:
        writer.writerow([
            r["campaign"], r["client_id"], r["name"], r["email"], r["institution"],
            r["template"], r["status"], r["http_status"], r["send_date"], r["sent_at"],
        ])

    filename = f"email_campaign_log_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
    return filename, buf.getvalue().encode("utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURE YOUR STEPS HERE
# Each step: {"name": "...", "cmd": <str or list[str]>, optional "cwd": "<path>", optional "shell": bool}
# Tip: to ensure you use the same Python interpreter, prefer [sys.executable, "your_script.py"]
# ─────────────────────────────────────────────────────────────────────────────
STEPS: List[Dict[str, Any]] = [

    {"name": "Kuda Email Campaign", "cmd": [sys.executable, "kuda_email_campaign.py"]},
    {"name": "Numida Email Campaign", "cmd": [sys.executable, "numida_email_campaign.py"]},
    {"name": "Other Institutions Email Campaign", "cmd": [sys.executable, "kudi_email_campaign.py"]},


    # You can add non-Python tasks too, e.g. a shell command:
    # {"name": "Touch heartbeat file", "cmd": "touch heartbeat.ok", "shell": True},
]

# Default delay between steps (seconds)
DEFAULT_DELAY_S = 5

# Default concurrency cap for --parallel mode (avoids OOM when there are many steps)
DEFAULT_MAX_WORKERS = 1

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
def setup_logging(log_dir: Path, log_file: str) -> Path:
    from logging.handlers import RotatingFileHandler

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear old handlers (if re-running inside interactive)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s • %(message)s")

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file (5 MB × 5)
    fh = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return log_path

# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class StepResult:
    name: str
    command: Union[str, List[str]]
    cwd: Optional[str]
    started_at_utc: str
    finished_at_utc: str
    duration_sec: float
    return_code: Optional[int]
    status: str                # "success" | "failed" | "skipped"
    attempts: int
    error: Optional[str] = None
    stdout_tail: Optional[str] = None
    stderr_tail: Optional[str] = None

# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def _to_cmd_and_shell(cmd: Union[str, List[str]], shell_flag: Optional[bool]) -> tuple[List[str] | str, bool]:
    """
    Normalize command + shell flag.
    - If cmd is a list -> run without shell (recommended).
    - If cmd is a string:
        * If shell_flag provided, honor it.
        * Else, split on POSIX using shlex except on Windows, where we pass the raw string to shell=True by default.
    """
    if isinstance(cmd, list):
        return cmd, False

    if shell_flag is True:
        return cmd, True

    if shell_flag is False:
        # Try to split the string into argv
        posix = os.name != "nt"
        return shlex.split(cmd, posix=posix), False

    # No shell_flag specified for string commands:
    if os.name == "nt":
        # Windows: easier to let CMD parse it
        return cmd, True
    else:
        # POSIX: split into argv
        return shlex.split(cmd), False

def _tail_file(path: str, n: int = 10, max_bytes: int = 65536) -> Optional[str]:
    """Read up to the last `n` lines of a file without loading the whole
    file into memory -- seeks backward from the end instead."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            read_size = min(size, max_bytes)
            f.seek(size - read_size)
            data = f.read()
        text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        return "\n".join(lines[-n:]) if lines else None
    except OSError:
        return None


def run_step(
    name: str,
    cmd: Union[str, List[str]],
    cwd: Optional[Union[str, Path]] = None,
    shell: Optional[bool] = None,
    timeout: Optional[int] = None,
    retries: int = 0,
) -> StepResult:
    logger = logging.getLogger(f"step:{name}")
    normalized_cmd, use_shell = _to_cmd_and_shell(cmd, shell)

    attempts = 0
    start_ts = datetime.now(timezone.utc)
    start_str = start_ts.isoformat()

    stdout_tail = None
    stderr_tail = None
    last_error = None
    rc: Optional[int] = None
    cwd_str = str(cwd) if cwd else None

    while True:
        attempts += 1
        # Subprocess stdout/stderr are written straight to disk rather than
        # captured via subprocess.run(capture_output=True) -- that buffers
        # the ENTIRE child output as a Python string in this (parent)
        # process's memory until the child exits. A step that logs a line
        # or two per customer across tens of thousands of customers can
        # accumulate tens to low-hundreds of MB this way, growing for as
        # long as the child keeps running -- a real contributor to an Out
        # of Memory crash on a 512Mi instance, on top of the child's own
        # memory. Writing to files keeps this process's own footprint flat
        # regardless of how much the child logs; only a small tail is read
        # back afterward for the report.
        stdout_path = stderr_path = None
        try:
            with tempfile.NamedTemporaryFile(prefix="step_stdout_", suffix=".log", delete=False) as f:
                stdout_path = f.name
            with tempfile.NamedTemporaryFile(prefix="step_stderr_", suffix=".log", delete=False) as f:
                stderr_path = f.name

            logger.info("Starting: %s", normalized_cmd if isinstance(normalized_cmd, list) else normalized_cmd)
            with open(stdout_path, "w") as out_f, open(stderr_path, "w") as err_f:
                completed = subprocess.run(
                    normalized_cmd,
                    cwd=cwd_str,
                    shell=use_shell,
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    timeout=timeout,
                    check=False,
                )
            rc = completed.returncode

            stdout_tail = _tail_file(stdout_path, n=10)
            stderr_tail = _tail_file(stderr_path, n=10)
            if stdout_tail:
                logger.info("[stdout tail]\n%s", stdout_tail)
            if stderr_tail:
                logger.error("[stderr tail]\n%s", stderr_tail)

            if rc == 0:
                break  # success

            last_error = f"Non-zero exit code: {rc}"
            logger.error("Run attempt %d failed (rc=%s).", attempts, rc)

        except subprocess.TimeoutExpired as e:
            last_error = f"Timeout after {timeout}s"
            logger.error("Timeout (attempt %d): %s", attempts, last_error)
            rc = None  # timeout has no rc
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            logger.exception("Exception during run (attempt %d).", attempts)
        finally:
            for p in (stdout_path, stderr_path):
                if p:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

        if attempts > retries:
            break
        else:
            logger.info("Retrying (%d/%d)...", attempts, retries)

    end_ts = datetime.now(timezone.utc)
    end_str = end_ts.isoformat()
    duration = (end_ts - start_ts).total_seconds()

    status = "success" if (rc == 0) else "failed"
    return StepResult(
        name=name,
        command=cmd,
        cwd=cwd_str,
        started_at_utc=start_str,
        finished_at_utc=end_str,
        duration_sec=duration,
        return_code=rc,
        status=status,
        attempts=attempts,
        error=last_error,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
    )

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Universal orchestrator (sequential or parallel).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_S, help="Delay (seconds) between steps.")
    parser.add_argument("--stop-on-failure", action="store_true", help="Stop immediately if a step fails.")
    parser.add_argument("--parallel", action="store_true", help="Run all steps in parallel.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max concurrent workers for --parallel (default: number of steps).",
    )
    parser.add_argument("--timeout", type=int, default=None, help="Per-step timeout (seconds).")
    parser.add_argument("--retries", type=int, default=0, help="Retries per step on failure.")
    parser.add_argument("--log-dir", type=str, default="logs", help="Directory for log files.")
    parser.add_argument("--log-file", type=str, default="orchestrator.log", help="Log file name.")
    parser.add_argument("--summary-dir", type=str, default="summaries", help="Directory to write JSON summaries.")

    args = parser.parse_args()

    log_path = setup_logging(Path(args.log_dir), args.log_file)
    logger = logging.getLogger("orchestrator")

    logger.info("Python: %s", sys.executable)
    logger.info("Platform: %s", platform.platform())
    logger.info("Log file: %s", log_path)

    results: List[StepResult] = []
    try:
        if args.parallel:
            total = len(STEPS)
            max_workers = args.max_workers or DEFAULT_MAX_WORKERS
            logger.info("Parallel mode enabled: %d step(s), max_workers=%d (batched)", total, max_workers)
            if args.delay:
                logger.info("--delay is ignored in parallel mode.")
            if args.stop_on_failure:
                logger.info("--stop-on-failure is ignored in parallel mode.")

            indexed_results: list[tuple[int, StepResult]] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_map: dict[concurrent.futures.Future[StepResult], int] = {}
                for i, step in enumerate(STEPS, start=1):
                    name = step.get("name", f"step_{i}")
                    logger.info("(%d/%d) QUEUED: %s", i, total, name)
                    fut = pool.submit(
                        run_step,
                        name,
                        step["cmd"],
                        step.get("cwd"),
                        step.get("shell"),
                        args.timeout,
                        args.retries,
                    )
                    future_map[fut] = i

                for fut in concurrent.futures.as_completed(future_map):
                    i = future_map[fut]
                    res = fut.result()
                    indexed_results.append((i, res))
                    logger.info("(%d/%d) DONE: %s status=%s rc=%s", i, total, res.name, res.status, res.return_code)

            indexed_results.sort(key=lambda x: x[0])
            results = [r for _, r in indexed_results]
        else:
            for i, step in enumerate(STEPS, start=1):
                name = step.get("name", f"step_{i}")
                cmd = step["cmd"]
                cwd = step.get("cwd")
                shell = step.get("shell")

                logger.info("(%d/%d) RUNNING: %s", i, len(STEPS), name)
                res = run_step(
                    name=name,
                    cmd=cmd,
                    cwd=cwd,
                    shell=shell,
                    timeout=args.timeout,
                    retries=args.retries,
                )
                results.append(res)

                # Inter-step delay
                if i < len(STEPS):
                    if res.status == "failed" and args.stop_on_failure:
                        logger.error("Stopping after failure (per --stop-on-failure).")
                        break
                    logger.info("Waiting %.1f seconds before next step...", args.delay)
                    time.sleep(max(0.0, args.delay))

    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Proceeding to summary...")

    # Summary
    ok = sum(1 for r in results if r.status == "success")
    fail = sum(1 for r in results if r.status != "success")

    logger.info("─" * 80)
    logger.info("COMPLETION SUMMARY")
    logger.info("Total steps run: %d | Success: %d | Failed: %d", len(results), ok, fail)
    for r in results:
        logger.info(
            "[%s] status=%s rc=%s duration=%.2fs attempts=%d",
            r.name, r.status, r.return_code, r.duration_sec, r.attempts
        )
    logger.info("─" * 80)

    # Write JSON summary
    summary_dir = Path(args.summary_dir)
    summary_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = summary_dir / f"orchestration_summary_{ts}.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in results], f, indent=2)
    logger.info("Summary written to: %s", summary_path)

    # Email notification on every run (success or failure), with today's
    # send log attached as a CSV when there's anything to report.
    attachments = []
    csv_result = fetch_today_send_log_csv()
    if csv_result:
        attachments.append(csv_result)
    send_email_report(results, orchestrator_name="Kudi Email Campaigns", attachments=attachments)

    # Exit code reflects overall success
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()

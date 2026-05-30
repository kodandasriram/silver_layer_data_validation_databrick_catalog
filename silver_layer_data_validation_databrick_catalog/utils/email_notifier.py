import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

import pandas as pd


def is_email_enabled():
    return str(os.getenv("VALIDATION_EMAIL_ENABLED", "")).strip().lower() in {"1", "true", "yes", "y"}


def send_validation_summary_email(run_status_rows, summary_report_path=None):
    if not is_email_enabled():
        return False

    smtp_host = _required_env("VALIDATION_EMAIL_SMTP_HOST")
    smtp_port = int(os.getenv("VALIDATION_EMAIL_SMTP_PORT", "587"))
    sender = _required_env("VALIDATION_EMAIL_FROM")
    recipients = _split_addresses(_required_env("VALIDATION_EMAIL_TO"))
    cc_recipients = _split_addresses(os.getenv("VALIDATION_EMAIL_CC"))
    username = os.getenv("VALIDATION_EMAIL_SMTP_USER")
    password = os.getenv("VALIDATION_EMAIL_SMTP_PASSWORD")
    use_tls = str(os.getenv("VALIDATION_EMAIL_USE_TLS", "true")).strip().lower() in {"1", "true", "yes", "y"}

    message = EmailMessage()
    status = _overall_status(run_status_rows)
    subject_prefix = os.getenv("VALIDATION_EMAIL_SUBJECT_PREFIX", "Silver validation")
    message["Subject"] = f"{subject_prefix}: {status}"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    if cc_recipients:
        message["Cc"] = ", ".join(cc_recipients)

    message.set_content(_build_body(run_status_rows, summary_report_path))
    _attach_report_if_local(message, summary_report_path)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
        if use_tls:
            smtp.starttls()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(message, to_addrs=recipients + cc_recipients)

    return True


def _required_env(name):
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    raise ValueError(f"{name} is required when VALIDATION_EMAIL_ENABLED=true")


def _split_addresses(value):
    if not value:
        return []
    return [address.strip() for address in value.replace(";", ",").split(",") if address.strip()]


def _overall_status(run_status_rows):
    statuses = [str(row.get("overall_status") or "").upper() for row in run_status_rows]
    if not statuses:
        return "NO_VALIDATIONS"
    if all(status == "PASS" for status in statuses):
        return "PASS"
    if any(status == "ERROR" for status in statuses):
        return "ERROR"
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    return "NO_VALIDATIONS"


def _build_body(run_status_rows, summary_report_path):
    status_counts = pd.Series(
        [str(row.get("overall_status") or "UNKNOWN").upper() for row in run_status_rows],
        dtype="object",
    ).value_counts()
    lines = [
        "Silver layer validation completed.",
        "",
        f"Overall status: {_overall_status(run_status_rows)}",
        f"Total tables: {len(run_status_rows)}",
    ]

    for status, count in status_counts.items():
        lines.append(f"{status}: {count}")

    issue_rows = [
        row
        for row in run_status_rows
        if str(row.get("overall_status") or "").upper() in {"FAIL", "ERROR"}
    ]
    if issue_rows:
        lines.extend(["", "Tables needing attention:"])
        for row in issue_rows[:25]:
            message = str(row.get("message") or "").strip()
            suffix = f" - {message}" if message else ""
            lines.append(f"- {row.get('table_name')}: {row.get('overall_status')}{suffix}")
        if len(issue_rows) > 25:
            lines.append(f"- ... {len(issue_rows) - 25} more")

    if summary_report_path:
        lines.extend(["", f"Summary report: {summary_report_path}"])

    return "\n".join(lines)


def _attach_report_if_local(message, summary_report_path):
    if not summary_report_path:
        return

    path_text = str(summary_report_path)
    if path_text.startswith("dbfs:/"):
        return

    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return

    message.add_attachment(
        path.read_bytes(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name,
    )

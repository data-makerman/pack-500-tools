"""Identify adults missing the Family Talent survey and optionally send reminders.

This script compares the Family Talent form export with the Pack adult roster,
produces an optional cleanup CSV, and can either print or send reminder emails.
"""

from __future__ import annotations

import argparse
import base64
import html
import logging
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import List, Optional, Sequence, Set

from numpy import record
import pandas as pd

try:  # Optional Gmail dependencies, only required when --send-mails is provided
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ModuleNotFoundError:  # pragma: no cover - allow running without Google libs
    Request = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]
    InstalledAppFlow = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
DEFAULT_FROM_NAME = "Pack 500 Cubmaster"
DEFAULT_FROM_EMAIL = "cubmaster@pack500.org"
DEFAULT_PREVIEW_RECIPIENT = DEFAULT_FROM_EMAIL
EMAIL_SIGNATURE = f"~{DEFAULT_FROM_NAME}\nCub Scout Pack 500, Scouting America"
EMAIL_POLICY = policy.default.clone(max_line_length=1000)
VOLUNTEER_NEEDS = [
    "Pinewood Derby Trackmaster - with help from your Cubmaster, lead volunteers in building the track on January 23rd during the day",
    "Pinewood Derby Decormaster - with help from your Cubmaster, lead volunteers in decorating the venue on January 23rd during the day",
    "Pinewood Derby Check-in Assistants - with your Cubmaster and other volunteers, help check in Scouts and their cars on January 23rd evening",
    "Blue and Gold Patrol - work with Event Coordinator Cassie and team to plan and execute the Blue and Gold Banquet on March 21st",
    "Graduation Picnic Grubbers - help plan and execute the end-of-year picnic at Joyner Park on May 9th",
]
EXPORT_COLUMNS = ["first_name", "last_name", "email", "positions", "has_role", "is_den_leader"]


@dataclass
class AdultRecord:
    first_name: str
    last_name: str
    email: Optional[str]
    positions: str
    is_den_leader: bool
    has_role: bool = False


@dataclass
class EmailJob:
    record: AdultRecord
    subject: str
    body: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read the Family Talent survey, compare it to the Pack 500 adult roster, "
            "and remind adults who still need to submit their talents."
        )
    )
    parser.add_argument(
        "--talent-survey",
        type=Path,
        default=Path("family_talent/Family Talent Survey (Responses) - Form Responses 1.csv"),
        help="Path to the Family Talent survey CSV export.",
    )
    parser.add_argument(
        "--adult-roster",
        type=Path,
        default=Path("roster_exports/2026/adults/RosterReport_MultiUnit_Adults_20260111.csv"),
        help="Path to the Pack 500 adult roster CSV.",
    )
    parser.add_argument(
        "--survey-encoding",
        default="utf-8-sig",
        help="Encoding for the talent survey CSV (default: utf-8-sig).",
    )
    parser.add_argument(
        "--roster-encoding",
        default="utf-8-sig",
        help="Encoding for the adult roster CSV (default: utf-8-sig).",
    )
    parser.add_argument(
        "--send-mails",
        action="store_true",
        help="Send reminders via the Gmail API instead of printing previews.",
    )
    parser.add_argument(
        "--gmail-client-secret",
        type=Path,
        default=Path("gmail_client_secret.json"),
        help="Path to the Gmail OAuth client secret JSON file.",
    )
    parser.add_argument(
        "--gmail-token",
        type=Path,
        default=Path("gmail_token.json"),
        help="Path to store the Gmail OAuth token.",
    )
    parser.add_argument(
        "--from-name",
        default=DEFAULT_FROM_NAME,
        help="Display name to use in the From header.",
    )
    parser.add_argument(
        "--from-email",
        default=DEFAULT_FROM_EMAIL,
        help="Email address that owns the Gmail credential.",
    )
    parser.add_argument(
        "--reply-to",
        default=DEFAULT_FROM_EMAIL,
        help="Optional Reply-To header value.",
    )
    parser.add_argument(
        "--preview-recipient",
        default=DEFAULT_PREVIEW_RECIPIENT,
        help="Address that receives proofs when --send-to-adults is not set.",
    )
    parser.add_argument(
        "--send-to-adults",
        action="store_true",
        help="Deliver messages to the adult email addresses instead of the preview recipient.",
    )
    parser.add_argument(
        "--max-emails",
        type=int,
        help="Optional cap on the number of messages to send in one run.",
    )
    parser.add_argument(
        "--export-missing",
        type=Path,
        help="Write the missing adult roster to this CSV for manual cleanup.",
    )
    parser.add_argument(
        "--use-missing",
        type=Path,
        help="Skip roster diffing and use a previously exported/edited CSV of missing adults.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(format="%(levelname)s %(message)s", level=logging.INFO)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_email(value: object) -> Optional[str]:
    text = normalize_text(value).lower()
    if not text or "@" not in text:
        return None
    return text


def build_name_key(first: object, last: object) -> Optional[str]:
    first_text = normalize_text(first).lower()
    last_text = normalize_text(last).lower()
    if not (first_text or last_text):
        return None
    return f"{first_text}|{last_text}"


def load_talent_survey(path: Path, encoding: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Talent survey not found: {path}")
    df = pd.read_csv(path, encoding=encoding)
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def load_adult_roster(path: Path, encoding: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Adult roster not found: {path}")
    df = pd.read_csv(path, encoding=encoding, skiprows=1)
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.rename(columns={"positions (tenure)": "positions"})
    df = df.rename(columns={"first name": "first_name", "last name": "last_name"})
    df["first_name"] = df.get("first_name", "").apply(normalize_text)
    df["last_name"] = df.get("last_name", "").apply(normalize_text)
    df["email"] = df.get("email", "").apply(normalize_email)
    df["positions"] = df.get("positions", "").fillna("").astype(str).str.strip()
    df = df[(df["first_name"].astype(bool)) | (df["last_name"].astype(bool))]
    return df


def build_response_keys(df: pd.DataFrame) -> Set[str]:
    keys: Set[str] = set()
    for row in df.to_dict("records"):
        email = normalize_email(row.get("email address"))
        if email:
            keys.add(f"email:{email}")
        name_key = build_name_key(row.get("first name"), row.get("last name"))
        if name_key:
            keys.add(f"name:{name_key}")
    return keys


def is_den_leader(positions: str) -> bool:
    text = positions.lower()
    return "den leader" in text or "den admin" in text


def missing_talent_records(roster_df: pd.DataFrame, response_keys: Set[str]) -> List[AdultRecord]:
    missing: List[AdultRecord] = []
    seen: Set[str] = set()
    for row in roster_df.to_dict("records"):
        first = row.get("first_name", "")
        last = row.get("last_name", "")
        email = row.get("email")
        positions = row.get("positions", "") or ""
        keys = []
        if email:
            keys.append(f"email:{email}")
        name_key = build_name_key(first, last)
        if name_key:
            keys.append(f"name:{name_key}")
        if any(key in response_keys for key in keys):
            continue
        dedupe_key = next((key for key in keys if key.startswith("email:")), keys[0] if keys else None)
        if dedupe_key and dedupe_key in seen:
            continue
        if dedupe_key:
            seen.add(dedupe_key)
        missing.append(
            AdultRecord(
                first_name=str(first),
                last_name=str(last),
                email=email,
                positions=positions,
                is_den_leader=is_den_leader(positions),
            )
        )
    return missing


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_text(value).lower()
    return text in {"1", "true", "yes", "y", "t"}


def record_export_row(record: AdultRecord) -> dict:
    return {
        "first_name": record.first_name,
        "last_name": record.last_name,
        "email": record.email or "",
        "positions": record.positions,
        "has_role": bool(record.positions),
        "is_den_leader": record.is_den_leader,
    }


def export_missing_records(records: Sequence[AdultRecord], path: Path) -> None:
    rows = [record_export_row(record) for record in records]
    df = pd.DataFrame(rows, columns=EXPORT_COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("Wrote %s missing adults to %s", len(rows), path)


def load_missing_export(path: Path) -> List[AdultRecord]:
    if not path.exists():
        raise FileNotFoundError(f"Missing roster export not found: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [col.strip().lower() for col in df.columns]
    required = {"first_name", "last_name", "email"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "Missing required columns in cleaned roster: " + ", ".join(sorted(missing))
        )
    records: List[AdultRecord] = []
    for row in df.to_dict("records"):
        first = normalize_text(row.get("first_name"))
        last = normalize_text(row.get("last_name"))
        email = normalize_email(row.get("email"))
        positions = normalize_text(row.get("positions"))
        has_role = parse_bool(row.get("has_role"))
        if not (first or last or email):
            continue
        leader_value = row.get("is_den_leader")
        is_leader = parse_bool(leader_value) if leader_value is not None else is_den_leader(positions)
        records.append(
            AdultRecord(
                first_name=first,
                last_name=last,
                email=email,
                positions=positions,
                is_den_leader=is_leader,
                has_role=has_role,
            )
        )
    return records


def volunteer_needs_text() -> str:
    return "\n".join(f"- {need}" for need in VOLUNTEER_NEEDS)


def render_email(record: AdultRecord) -> EmailJob:
    subject = "Your Pack 500 Family Talent Survey is Missing!"
    greeting_name = record.first_name or record.last_name or "there"
    greeting = f"Hello {greeting_name},"
    lines = [greeting]
    lines.append(
        "We are refreshing the Pack 500 family talent roster so we can match every adult with the right volunteer opportunities."
    )
    lines.append(
        "We are still missing your entry, so I'm reaching out personally to learn more about your interests and skills. "
        "Would you take three minutes today to complete the survey at https://pack500.org/talent? "
        "If you already submitted your survey this program year, thank you! Please reply so we can make sure "
        "we correctly matched the response to your registered email."
    )
    if record.has_role:
        lines.append(
            "I know you are already serving in a Pack leadership role. Thank you for the time and energy you are giving our Scouts. "
            "I'm not looking to load more onto your plate; the talent survey simply helps me understand what kinds of projects feel like a natural fit "
            "when you do have bandwidth or want to explore something new."
        )
    else:
        lines.append(
            "My roster also indicates you have not yet found a volunteer role to suit you in our Pack this year. "
            "Every job that keeps Pack 500 running is handled by a Scout's parent, guardian, or family member, and we understand that not every role suits every person. "
            "I'd love to match you with something that fits your interests and your schedule."
        )
        lines.append("Here are a few roles where we especially need help right now:")
        lines.append(volunteer_needs_text())
        lines.append(
            "If any of those resonate or if you have another idea for something you'd love to help with, please hit reply so we can talk."
        )
    lines.append(
        "Thank you for being part of Pack 500!"
    )
    lines.append(EMAIL_SIGNATURE)
    body = "\n\n".join(lines).strip() + "\n"
    return EmailJob(record=record, subject=subject, body=body)


def to_html_paragraphs(text: str) -> str:
    paragraphs = [chunk.strip() for chunk in text.strip().split("\n\n") if chunk.strip()]
    html_parts = []
    for paragraph in paragraphs:
        escaped = html.escape(paragraph).replace("\n", "<br>")
        html_parts.append(f"<p>{escaped}</p>")
    return "\n".join(html_parts)


def format_preview_subject(subject: str, intended: Optional[str], *, preview: bool) -> str:
    if not preview:
        return subject
    label = intended or "the intended recipient"
    return f"{subject} (intended for {label})"


def build_gmail_service(client_secret_path: Path, token_path: Path):  # type: ignore[override]
    if not all([Credentials, InstalledAppFlow, build, Request]):
        raise RuntimeError(
            "Google API dependencies are not installed. Please pip install google-auth-oauthlib google-api-python-client."
        )
    client_secret_path = Path(client_secret_path)
    token_path = Path(token_path)
    if not client_secret_path.exists():
        raise FileNotFoundError(f"Missing Gmail client secret: {client_secret_path}")
    creds: Optional[Credentials] = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_path), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    return build("gmail", "v1", credentials=creds)


class ReminderEmailSender:
    def __init__(
        self,
        *,
        service,
        from_name: str,
        from_email: str,
        reply_to: Optional[str],
        preview_recipient: Optional[str],
        send_to_adults: bool,
        max_emails: Optional[int],
    ) -> None:
        self.service = service
        self.from_name = from_name
        self.from_email = from_email
        self.reply_to = reply_to
        self.preview_recipient = preview_recipient
        self.send_to_adults = send_to_adults
        self.max_emails = max_emails
        self.sent_count = 0

    def _target(self, record: AdultRecord) -> Optional[str]:
        if not self.send_to_adults:
            return self.preview_recipient
        return record.email

    def send(self, job: EmailJob) -> None:
        if self.max_emails is not None and self.sent_count >= self.max_emails:
            logging.info("Max email limit of %s reached; skipping remaining reminders.", self.max_emails)
            return
        target = self._target(job.record)
        if not target:
            logging.warning(
                "Skipping reminder for %s %s due to missing target email.",
                job.record.first_name,
                job.record.last_name,
            )
            return
        subject = format_preview_subject(job.subject, job.record.email, preview=not self.send_to_adults)
        message = EmailMessage(policy=EMAIL_POLICY)
        message["To"] = target
        message["From"] = formataddr((self.from_name, self.from_email))
        if self.reply_to:
            message["Reply-To"] = self.reply_to
        message["Subject"] = subject
        message.set_content(job.body)
        message.add_alternative(to_html_paragraphs(job.body), subtype="html")
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        self.service.users().messages().send(userId="me", body={"raw": raw}).execute()
        self.sent_count += 1
        logging.info(
            "Sent reminder for %s %s to %s",
            job.record.first_name,
            job.record.last_name,
            target,
        )


def print_preview(job: EmailJob, target: Optional[str], *, preview: bool) -> None:
    subject = format_preview_subject(job.subject, job.record.email, preview=preview)
    logging.info(
        "Preview reminder for %s %s (to %s)",
        job.record.first_name,
        job.record.last_name,
        target or "<missing>",
    )
    divider = "-" * 60
    print(divider)
    print(f"To: {target or 'N/A'}")
    print(f"Subject: {subject}")
    print()
    print(job.body)
    print(divider)


def build_email_jobs(records: Sequence[AdultRecord]) -> List[EmailJob]:
    jobs: List[EmailJob] = []
    for record in records:
        if not record.email:
            logging.warning(
                "No email on file for %s %s; skipping reminder.",
                record.first_name,
                record.last_name,
            )
            continue
        jobs.append(render_email(record))
    return jobs


def main() -> None:
    args = parse_args()
    configure_logging()

    if args.use_missing:
        missing_records = load_missing_export(args.use_missing)
        logging.info(
            "Loaded %s adults from cleaned missing roster %s.",
            len(missing_records),
            args.use_missing,
        )
    else:
        talent_df = load_talent_survey(args.talent_survey, args.survey_encoding)
        roster_df = load_adult_roster(args.adult_roster, args.roster_encoding)

        response_keys = build_response_keys(talent_df)
        missing_records = missing_talent_records(roster_df, response_keys)
        logging.info(
            "Loaded %s survey responses and %s roster adults; %s adults still need the survey.",
            len(talent_df),
            len(roster_df),
            len(missing_records),
        )

    if args.export_missing:
        export_missing_records(missing_records, args.export_missing)

    jobs = build_email_jobs(missing_records)
    if not jobs:
        logging.info("No reminders to generate.")
        return

    sender: Optional[ReminderEmailSender] = None
    if args.send_mails:
        service = build_gmail_service(args.gmail_client_secret, args.gmail_token)
        sender = ReminderEmailSender(
            service=service,
            from_name=args.from_name,
            from_email=args.from_email,
            reply_to=args.reply_to,
            preview_recipient=args.preview_recipient,
            send_to_adults=args.send_to_adults,
            max_emails=args.max_emails,
        )

    preview_mode = not args.send_to_adults
    for job in jobs:
        if sender:
            sender.send(job)
        else:
            preview_target = job.record.email if args.send_to_adults else args.preview_recipient
            print_preview(job, preview_target, preview=preview_mode)

    logging.info("Prepared %s reminders for missing talent surveys.", len(jobs))


if __name__ == "__main__":
    main()

"""Build Pack 500 membership renewal and lapse reminder emails from BSA exports."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import io
import logging
import os
import sys
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gmail_oauth import build_gmail_service

try:  # Optional Gmail dependency, only needed when --send-email is provided
    from googleapiclient.errors import HttpError
except ModuleNotFoundError:  # pragma: no cover - allow running without Google libs
    HttpError = Exception  # type: ignore[assignment]


DEFAULT_FROM_NAME = "Pack 500 Cubmaster"
DEFAULT_FROM_EMAIL = "cubmaster@pack500.org"
DEFAULT_PREVIEW_RECIPIENT = DEFAULT_FROM_EMAIL
DEFAULT_INPUT_ENCODING = "latin-1"
DEFAULT_GMAIL_CLIENT_SECRET = Path("gmail_client_secret.json")
DEFAULT_GMAIL_TOKEN = Path("gmail_token.json")
DEFAULT_GMAIL_AUTH_MODE = "auto"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
EMAIL_SIGNATURE = (
    f"~{DEFAULT_FROM_NAME}\n"
    "Cub Scout Pack 500, Scouting America"
)
EMAIL_POLICY = policy.default.clone(max_line_length=1000)


@dataclass
class RenewalNotice:
    notice_type: str  # "lapsed" or "expiring"
    member_id: str
    first_name: str
    last_name: str
    suffix: str
    email: Optional[str]
    expiration: Optional[dt.date]


@dataclass
class EmailJob:
    notice: RenewalNotice
    subject: str
    body: str


def previous_report_arg(value: str) -> Optional[Path]:
    text = (value or "").strip()
    if not text or text.lower() == "none":
        return None
    return Path(text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Email courtesy reminders for members who lapsed this Scouting year "
            "or who are within the current renewal window."
        )
    )
    parser.add_argument(
        "--non-renewed",
        default=Path("member_notices/2026-01/NonRenewedMembership.csv"),
        type=Path,
        help="Path to the Non Renewed Membership report CSV.",
    )
    parser.add_argument(
        "--previous-non-renewed",
        type=previous_report_arg,
        help=(
            "Optional earlier Non Renewed Membership report. When omitted, the script uses the latest earlier "
            "dated report under member_notices/ if one exists. Use 'None' to force no previous report."
        ),
    )
    parser.add_argument(
        "--roster",
        default=Path("member_notices/2026-01/Roster_Report.csv"),
        type=Path,
        help="Path to the current roster report CSV.",
    )
    parser.add_argument(
        "--as-of",
        help="Override the date used for renewal math (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--renewal-window-days",
        type=int,
        default=62,
        help="Number of days before expiration to consider in the renewal window (default: 62).",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Send messages via the Gmail API instead of printing previews to stdout.",
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
        "--preview-recipient",
        default=DEFAULT_PREVIEW_RECIPIENT,
        help="Address that receives proofs when --send-to-members is not set.",
    )
    parser.add_argument(
        "--send-to-members",
        action="store_true",
        help="Deliver messages to the actual member email addresses instead of the preview recipient.",
    )
    parser.add_argument(
        "--max-emails",
        type=int,
        help="Optional cap on the number of messages to send in one run.",
    )
    parser.add_argument(
        "--gmail-client-secret",
        type=Path,
        default=Path(os.getenv("PACK500_GMAIL_CLIENT_SECRET", DEFAULT_GMAIL_CLIENT_SECRET)),
        help="Path to the Google OAuth desktop-app client secret JSON.",
    )
    parser.add_argument(
        "--gmail-token",
        type=Path,
        default=Path(os.getenv("PACK500_GMAIL_TOKEN", DEFAULT_GMAIL_TOKEN)),
        help="Path to the cached Gmail OAuth token JSON.",
    )
    parser.add_argument(
        "--gmail-auth-mode",
        choices=["auto", "local-server", "console"],
        default=os.getenv("PACK500_GMAIL_AUTH_MODE", DEFAULT_GMAIL_AUTH_MODE),
        help="OAuth flow to use when a new Gmail token is needed. Use console for Colab or remote terminals.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        format="%(levelname)s %(message)s",
        level=logging.INFO,
    )


def read_report_csv(path: Path, encoding: str, header_prefix: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {path}")
    text = path.read_text(encoding=encoding)
    lines = text.splitlines()
    start_idx = None
    for idx, raw in enumerate(lines):
        normalized = raw.lstrip(".").strip().lower()
        if normalized.startswith(header_prefix.lower()):
            start_idx = idx
            break
    if start_idx is None:
        raise ValueError(f"Unable to locate header '{header_prefix}' in {path}")
    trimmed = lines[start_idx:]
    trimmed[0] = trimmed[0].lstrip(".")
    csv_text = "\n".join(trimmed)
    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def load_non_renewed(path: Path, encoding: str) -> pd.DataFrame:
    df = read_report_csv(path, encoding, "district")
    if "memberid" not in df.columns:
        raise ValueError("Expected 'memberid' column in NonRenewedMembership report")
    df["memberid"] = df["memberid"].astype(str).str.strip()
    df = df[df["memberid"].astype(bool)]
    df = df.drop_duplicates(subset=["memberid"], keep="last")
    return df


def load_roster(path: Path, encoding: str) -> pd.DataFrame:
    df = read_report_csv(path, encoding, "memberid")
    if "memberid" not in df.columns:
        raise ValueError("Expected 'memberid' column in roster report")
    df["memberid"] = df["memberid"].astype(str).str.strip()
    df = df[df["memberid"].astype(bool)]
    return df


def parse_date(value: object) -> Optional[dt.date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    logging.warning("Unable to parse date value '%s'", text)
    return None


def best_email(values: Sequence[object]) -> Optional[str]:
    for value in values:
        email = str(value or "").strip()
        if not email:
            continue
        if email.lower() == "nan":
            continue
        if email:
            return email
    return None


def clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() == "nan":
        return ""
    return text


def resolve_previous_non_renewed(
    non_renewed_path: Path,
    explicit_previous: Optional[Path],
) -> Optional[Path]:
    if explicit_previous is not None:
        return explicit_previous
    history_dir = non_renewed_path.parent.parent
    current_period = non_renewed_path.parent.name
    candidates = [
        path
        for path in sorted(history_dir.glob("*/NonRenewedMembership.csv"))
        if path != non_renewed_path and path.parent.name < current_period
    ]
    if not candidates:
        return None
    chosen = candidates[-1]
    logging.info("Using prior Non Renewed Membership report at %s", chosen)
    return chosen


def build_lapsed_notices(
    nonrenewed_df: pd.DataFrame,
    *,
    skip_member_ids: Iterable[str] = (),
) -> List[RenewalNotice]:
    skip = {str(member_id).strip() for member_id in skip_member_ids if str(member_id).strip()}
    notices: List[RenewalNotice] = []
    skipped = 0
    for row in nonrenewed_df.to_dict("records"):
        member_id = str(row.get("memberid", "")).strip()
        if member_id in skip:
            skipped += 1
            continue
        email = best_email([
            row.get("email"),
            row.get("primaryemail"),
            row.get("pgprimaryemail"),
        ])
        notice = RenewalNotice(
            notice_type="lapsed",
            member_id=member_id,
            first_name=clean_text(row.get("firstname")),
            last_name=clean_text(row.get("lastname")),
            suffix="",  # Suffix is not provided in the NonRenewedMembership report
            email=email,
            expiration=parse_date(row.get("strexpirydt") or row.get("expirydtstr")),
        )
        notices.append(notice)
    if skipped:
        logging.info("Skipped %s lapsed members that already received a prior notice.", skipped)
    return notices


def latest_expirations(roster_df: pd.DataFrame) -> pd.DataFrame:
    roster_df = roster_df.copy()
    roster_df["expiration_date"] = roster_df.get("expirydtstr").apply(parse_date)
    roster_df = roster_df.dropna(subset=["expiration_date"])
    roster_df = roster_df.sort_values(["memberid", "expiration_date"])
    latest = roster_df.groupby("memberid", as_index=False).tail(1)
    return latest


def build_renewal_notices(
    roster_df: pd.DataFrame,
    *,
    as_of: dt.date,
    window_days: int,
    skip_member_ids: Iterable[str],
) -> List[RenewalNotice]:
    cutoff = dt.timedelta(days=window_days)
    skip = set(skip_member_ids)
    notices: List[RenewalNotice] = []
    for row in latest_expirations(roster_df).to_dict("records"):
        member_id = row.get("memberid", "")
        if member_id in skip:
            continue
        expiration = row.get("expiration_date")
        if not isinstance(expiration, dt.date):
            continue
        if expiration < as_of - cutoff:
            continue
        if expiration > as_of + cutoff:
            continue
        email = best_email([row.get("pgprimaryemail"), row.get("primaryemail")])
        notices.append(
            RenewalNotice(
                notice_type="expiring",
                member_id=member_id,
                first_name=clean_text(row.get("firstname")),
                last_name=clean_text(row.get("lastname")),
                suffix=clean_text(row.get("suffix")),
                email=email,
                expiration=expiration,
            )
        )
    return notices


def render_lapsed_email(notice: RenewalNotice) -> EmailJob:
    member_name = f"{notice.first_name} {notice.last_name}{' '+notice.suffix if notice.suffix else ''}".strip()
    date_text = notice.expiration.strftime("%B %d, %Y") if notice.expiration else "earlier this year"
    subject = f"Pack 500 membership courtesy notice for {member_name or 'your family'}"
    body = (
        f"Hello,\n\n"
        f"This is a courtesy note regarding {member_name or 'your family'}'s membership in Pack 500. "
        f"Our records show it lapsed during this Scouting year (expiration on {date_text}). "
        "We would be happy to welcome you back into the Pack roster whenever you are ready.\n\n"
        "If you would like to renew, please visit https://pack500.org/apply or reply to this message "
        "so we can lend a hand.\n\n"
        "If you intentionally did not renew, there is no further action needed. "
        "We are grateful for your time in Pack 500, and we hope to see you again in the future!\n\n"
        "Thank you for everything you have done for Pack 500!\n\n"
        f"{EMAIL_SIGNATURE}\n"
    )
    return EmailJob(notice=notice, subject=subject, body=body)


def render_renewal_email(notice: RenewalNotice) -> EmailJob:
    member_name = f"{notice.first_name} {notice.last_name}{' '+notice.suffix if notice.suffix else ''}".strip()
    expiration = notice.expiration.strftime("%B %d, %Y") if notice.expiration else "soon"
    subject = f"Pack 500 renewal reminder for {member_name or 'your family'}"
    body = (
        f"Hello,\n\n"
        f"This is a courtesy reminder that {member_name or 'a Pack 500 membership'}'s membership in Pack 500 "
        f"is expiring, with an expiration date of {expiration}. The renewal window is now open.\n\n"
        "Please renew through https://my.scouting.org/tools/my-applications when you have a moment, "
        "or reply to this email if you would like help with the process.\n\n"
        "Thank you for continuing to be part of Pack 500!\n\n"
        f"{EMAIL_SIGNATURE}\n"
    )
    return EmailJob(notice=notice, subject=subject, body=body)


def build_email_jobs(
    *,
    lapsed: List[RenewalNotice],
    expiring: List[RenewalNotice],
) -> List[EmailJob]:
    jobs: List[EmailJob] = []
    for notice in lapsed:
        jobs.append(render_lapsed_email(notice))
    for notice in expiring:
        jobs.append(render_renewal_email(notice))
    return jobs


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


class SummaryEmailSender:
    def __init__(
        self,
        *,
        service,
        from_name: str,
        from_email: str,
        preview_recipient: Optional[str],
        send_to_members: bool,
        max_emails: Optional[int],
    ) -> None:
        self.service = service
        self.from_name = from_name
        self.from_email = from_email
        self.preview_recipient = preview_recipient
        self.send_to_members = send_to_members
        self.max_emails = max_emails
        self.sent_count = 0

    def _target(self, notice: RenewalNotice) -> Optional[str]:
        if not self.send_to_members:
            return self.preview_recipient
        return notice.email

    def send(self, job: EmailJob) -> None:
        if self.max_emails is not None and self.sent_count >= self.max_emails:
            logging.info("Max email limit of %s reached; skipping remaining notices.", self.max_emails)
            return
        target = self._target(job.notice)
        if not target:
            logging.warning(
                "Skipping %s notice for %s %s due to missing target email.",
                job.notice.notice_type,
                job.notice.first_name,
                job.notice.last_name,
            )
            return
        subject = format_preview_subject(job.subject, job.notice.email, preview=not self.send_to_members)
        message = EmailMessage(policy=EMAIL_POLICY)
        message["To"] = target
        message["From"] = formataddr((self.from_name, self.from_email))
        message["Subject"] = subject
        message.set_content(job.body)
        message.add_alternative(to_html_paragraphs(job.body), subtype="html")
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        self.service.users().messages().send(userId="me", body={"raw": raw}).execute()
        self.sent_count += 1
        logging.info(
            "Sent %s notice for %s %s to %s",
            job.notice.notice_type,
            job.notice.first_name,
            job.notice.last_name,
            target,
        )


def print_preview(job: EmailJob, target: Optional[str], *, preview: bool) -> None:
    subject = format_preview_subject(job.subject, job.notice.email, preview=preview)
    logging.info(
        "Preview %s notice for %s %s (to %s)",
        job.notice.notice_type,
        job.notice.first_name,
        job.notice.last_name,
        target or "<missing>",
    )
    divider = "-" * 60
    print(divider)
    print(f"To: {target or 'N/A'}")
    print(f"Subject: {subject}")
    print()
    print(job.body)
    print(divider)


def main() -> None:
    args = parse_args()
    configure_logging()
    as_of = dt.date.today()
    if args.as_of:
        as_of = dt.datetime.strptime(args.as_of, "%Y-%m-%d").date()
    logging.info("Running renewal notices as of %s", as_of.isoformat())

    nonrenewed_df = load_non_renewed(args.non_renewed, DEFAULT_INPUT_ENCODING)
    previously_contacted_ids: set[str] = set()
    previous_non_renewed = resolve_previous_non_renewed(args.non_renewed, args.previous_non_renewed)
    if previous_non_renewed:
        previous_df = load_non_renewed(previous_non_renewed, DEFAULT_INPUT_ENCODING)
        prev_ids = previous_df["memberid"].astype(str).str.strip()
        previously_contacted_ids = {member_id for member_id in prev_ids if member_id}
        logging.info(
            "Loaded %s previously contacted member IDs from %s",
            len(previously_contacted_ids),
            previous_non_renewed,
        )
    roster_df = load_roster(args.roster, DEFAULT_INPUT_ENCODING)

    lapsed = build_lapsed_notices(nonrenewed_df, skip_member_ids=previously_contacted_ids)
    expiring = build_renewal_notices(
        roster_df,
        as_of=as_of,
        window_days=args.renewal_window_days,
        skip_member_ids=[notice.member_id for notice in lapsed],
    )

    jobs = build_email_jobs(lapsed=lapsed, expiring=expiring)

    if not jobs:
        logging.info("No renewal notices to generate.")
        return

    sender: Optional[SummaryEmailSender] = None
    if args.send_email:
        service = build_gmail_service(
            args.gmail_client_secret,
            args.gmail_token,
            GMAIL_SCOPES,
            auth_mode=args.gmail_auth_mode,
        )
        sender = SummaryEmailSender(
            service=service,
            from_name=args.from_name,
            from_email=args.from_email,
            preview_recipient=args.preview_recipient,
            send_to_members=args.send_to_members,
            max_emails=args.max_emails,
        )

    preview_mode = not args.send_to_members
    for job in jobs:
        if sender:
            sender.send(job)
        else:
            preview_target = job.notice.email if args.send_to_members else args.preview_recipient
            print_preview(job, preview_target, preview=preview_mode)

    logging.info(
        "Prepared %s notices (%s lapsed, %s expiring).",
        len(jobs),
        len(lapsed),
        len(expiring),
    )


if __name__ == "__main__":
    main()

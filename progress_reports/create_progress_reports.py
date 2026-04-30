"""Aggregate Scoutbook+ exports, enrich them with roster data, and build reports."""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import re
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:  # Optional Gmail dependencies (only needed when --send-email is used)
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ModuleNotFoundError:  # pragma: no cover - fallback when libs are missing
    Request = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]
    InstalledAppFlow = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]
    HttpError = Exception  # type: ignore[assignment]


PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)%")
REQUIREMENT_RE = re.compile(
    r"^\s*(\d+[a-z]?)(?:\s*\([^)]*\))*\s*(?:[\.\)])+\s*(.*)$",
    re.IGNORECASE,
)
REQUIREMENT_REQ_HASH_RE = re.compile(
    r"^\s*(?:req(?:uirement)?\s*#?)(\d+[a-z]?)\s*(?:[\.\-:]*)\s*(.*)$",
    re.IGNORECASE,
)
REQUIREMENT_PREFIX_ONLY_RE = re.compile(r"^\s*req(?:uirement)?\s*#?\s*$", re.IGNORECASE)
REQUIREMENT_CODE_TOKEN_RE = re.compile(r"(\d+[a-z]?)", re.IGNORECASE)
DEFAULT_PATTERN = "ReportBuilder_Pack0500_Adventures_*__*.csv"
DEFAULT_INPUT_ENCODING = "latin-1"
DEFAULT_OUTPUT_NAME = "progress_report_aggregate.csv"
DEFAULT_ADVENTURE_JSON = Path("adventure_requirements.json")
DEFAULT_GMAIL_CLIENT_SECRET = Path("gmail_client_secret.json")
DEFAULT_GMAIL_TOKEN = Path("gmail_token.json")
SKIP_LABELS = {
    "subunit",
    "nextrank",
    "nextrankpct",
    "nextrankpercent",
}
SUPER_ACHIEVER_ELECTIVES = {
    "lion": 7,
    "tiger": 12,
    "wolf": 12,
    "bear": 12,
    "webelos": 12,
    "aol": 8,
    "arrow of light": 8,
}
RANK_ADVENTURE_LINKS = {
    "lion": "lion",
    "tiger": "tiger",
    "wolf": "wolf",
    "bear": "bear",
    "webelos": "webelos",
    "aol": "arrow-of-light",
    "arrow of light": "arrow-of-light",
}
CANONICAL_RANK_NAMES = {
    "aol": "Arrow of Light",
    "arrowoflight": "Arrow of Light",
}
RANK_ELECTIVE_REQUIREMENT = 2
POPULAR_THRESHOLD = 0.5
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
DEFAULT_FROM_NAME = "Pack 500 Cubmaster"
DEFAULT_FROM_EMAIL = "cubmaster@pack500.org"
DEFAULT_PREVIEW_RECIPIENT = DEFAULT_FROM_EMAIL

AdventureRequirement = Dict[str, str]
AdventureRecord = Dict[str, Any]
AdventureCatalog = Dict[str, AdventureRecord]
ADVENTURE_PARENTHESES_ALIASES = {
    "aol": "arrow of light",
    "arrowoflight": "arrow of light",
}
ADVENTURE_PAREN_RE = re.compile(r"\(([^)]+)\)")
ADVENTURE_NAME_OVERRIDES = {
    "curiosityintriguemyst": "Curiosity, Intrigue, and Magical Mysteries",
}
REQUIRED_ADVENTURES_BY_RANK = {
    "arrowoflight": [
        "Bobcat (AOL)",
        "Citizenship",
        "Duty to God",
        "First Aid",
        "Outdoor Adventurer",
        "Personal Fitness",
    ],
    "bear": [
        "Bobcat (Bear)",
        "Bear Habitat",
        "Bear Strong",
        "Fellowship",
        "Paws for Action",
        "Standing Tall",
    ],
    "lion": [
        "Bobcat (Lion)",
        "Fun on the Run",
        "King of the Jungle",
        "Lion's Pride",
        "Lion's Roar",
        "Mountain Lion",
    ],
    "tiger": [
        "Bobcat (Tiger)",
        "Team Tiger",
        "Tiger Bites",
        "Tiger Circles",
        "Tigers in the Wild",
    ],
    "webelos": [
        "Bobcat (Webelos)",
        "My Community",
        "My Family",
        "My Safety",
        "Stronger, Faster, Higher",
        "Webelos Walkabout",
    ],
    "wolf": [
        "Bobcat (Wolf)",
        "Council Fire",
        "Footsteps",
        "Paws on the Path",
        "Running With the Pack",
        "Safety in Numbers",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine per-rank progress report CSVs, merge roster contacts, and emit HTML reports."
    )
    parser.add_argument(
        "--input",
        "--input-dir",
        dest="input_dir",
        default=Path("progress_reports/2026-01"),
        type=Path,
        help="Directory (or single CSV path) containing Scoutbook+ progress exports.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Destination CSV for the tall/normalized dataset. Defaults beside the input exports.",
    )
    parser.add_argument(
        "--roster",
        type=Path,
        help="Path to the raw Scouts' Parents roster export. Defaults to the matching roster file in --input.",
    )
    parser.add_argument(
        "--report-date",
        help="Override the as-of date shown in reports (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--skip-html",
        action="store_true",
        help="Only build the tall CSV and skip HTML generation.",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="If set, send each summary email via the Gmail API after generating HTML.",
    )
    parser.add_argument(
        "--from-email",
        default=DEFAULT_FROM_EMAIL,
        help="Email address to use in the From header (must match an authorized alias).",
    )
    parser.add_argument(
        "--from-name",
        default=DEFAULT_FROM_NAME,
        help="Display name to pair with the From email address.",
    )
    parser.add_argument(
        "--preview-recipient",
        default=DEFAULT_PREVIEW_RECIPIENT,
        help="Override actual parent recipients and send every email to this address for proofing. Provide an empty string to disable.",
    )
    parser.add_argument(
        "--send-to-parents",
        action="store_true",
        help="Send directly to parent/guardian email addresses instead of the preview recipient.",
    )
    parser.add_argument(
        "--max-emails",
        type=int,
        help="Optional cap on the number of emails to send in a single run (useful for testing).",
    )
    return parser.parse_args()


def normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def slugify(value: str, separator: str = "-") -> str:
    slug = re.sub(r"[^a-z0-9]+", separator, value.lower()).strip(separator)
    return slug or "scout"


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for val in values:
        if not val:
            continue
        if val in seen:
            continue
        seen.add(val)
        ordered.append(val)
    return ordered


def split_semicolon_list(value: str) -> List[str]:
    return [part.strip() for part in str(value or "").split(";") if part.strip()]


def repair_mojibake(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return text.encode("latin-1").decode("utf-8")
    except UnicodeEncodeError:
        return text
    except UnicodeDecodeError:
        return text


def build_gmail_service(client_secret_path: Path, token_path: Path):  # type: ignore[override]
    if not all([Credentials, InstalledAppFlow, build, Request]):
        raise ImportError(
            "Google API libraries are required for --send-email. Install google-auth-oauthlib and google-api-python-client."
        )
    client_secret_path = Path(client_secret_path)
    token_path = Path(token_path)
    if not client_secret_path.exists():
        raise FileNotFoundError(
            f"Gmail OAuth client secret not found at {client_secret_path}."
        )
    creds: Optional[Credentials] = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path), GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return build("gmail", "v1", credentials=creds)


class SummaryEmailSender:
    def __init__(
        self,
        *,
        service,
        from_name: str,
        from_email: str,
        preview_recipient: Optional[str],
        max_emails: Optional[int],
    ) -> None:
        self.service = service
        self.from_name = from_name
        self.from_email = from_email
        self.preview_recipient = preview_recipient.strip() if preview_recipient else None
        self.max_emails = max_emails
        self.sent = 0

    def send_summary(
        self,
        *,
        scout_name: str,
        parent_name: str,
        parent_email: str,
        summary_html: str,
    ) -> None:
        if self.max_emails is not None and self.sent >= self.max_emails:
            return
        candidate_parent = parent_email.strip()
        recipient = self.preview_recipient or candidate_parent
        if not recipient:
            print(
                f"Skipping email for {scout_name} ({parent_name}) because no recipient email was provided."
            )
            return
        subject = f"Pack 500 Progress Report for {scout_name}"
        if self.preview_recipient and parent_email:
            subject += f" (intended for {parent_email})"

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = formataddr((self.from_name, self.from_email))
        message["To"] = recipient

        text_fallback = (
            f"Hi {parent_name or 'family'},\n\n"
            "This message contains HTML content. If you cannot view HTML, please log into Scoutbook "
            "or contact your den leader for the latest advancement details."
        )
        message.set_content(text_fallback)
        message.add_alternative(summary_html, subtype="html")

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        try:
            self.service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
            self.sent += 1
            print(f"Sent email to {recipient} for {scout_name} ({parent_name})")
        except HttpError as exc:
            print(
                "Failed to send email via Gmail API for "
                f"{scout_name} ({parent_name}) to {recipient}: {exc}"
            )


def build_summary_email_sender(
    *,
    client_secret: Path,
    token_path: Path,
    from_name: str,
    from_email: str,
    preview_recipient: Optional[str],
    max_emails: Optional[int],
) -> SummaryEmailSender:
    service = build_gmail_service(client_secret, token_path)
    return SummaryEmailSender(
        service=service,
        from_name=from_name,
        from_email=from_email,
        preview_recipient=preview_recipient,
        max_emails=max_emails,
    )


def normalize_requirement_code(code: str) -> str:
    text = str(code or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def parse_requirement_label(label: str) -> Optional[Tuple[str, str]]:
    value = str(label or "").strip()
    if not value:
        return None
    match = REQUIREMENT_RE.match(value)
    if match:
        return match.group(1), match.group(2).strip()
    match = REQUIREMENT_REQ_HASH_RE.match(value)
    if match:
        return match.group(1), match.group(2).strip()
    return None


def adventure_lookup_keys(name: str) -> List[str]:
    raw = str(name or "").strip()
    if not raw:
        return []

    keys: List[str] = []

    def _add(candidate: str) -> None:
        norm = normalize_label(candidate)
        if norm and norm not in keys:
            keys.append(norm)

    _add(raw)
    override = ADVENTURE_NAME_OVERRIDES.get(normalize_label(raw))
    if override:
        _add(override)

    paren_values = ADVENTURE_PAREN_RE.findall(raw)
    if not paren_values:
        return keys

    stripped = ADVENTURE_PAREN_RE.sub("", raw).strip()
    if stripped:
        _add(stripped)

    alias_terms: List[str] = []
    for value in paren_values:
        alias = ADVENTURE_PARENTHESES_ALIASES.get(normalize_label(value))
        if alias:
            alias_terms.append(alias)

    if stripped and alias_terms:
        alias_phrase = f"{stripped} {' '.join(alias_terms)}"
        _add(alias_phrase)

    return keys


def load_adventure_requirements(path: Path) -> AdventureCatalog:
    json_path = Path(path)
    if not json_path.exists():
        raise FileNotFoundError(f"Adventure requirements file not found: {json_path}")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    catalog: AdventureCatalog = {}
    adventures = payload.get("adventures", []) if isinstance(payload, dict) else []
    for adventure in adventures:
        title = str(adventure.get("title", "")).strip()
        if not title:
            continue
        key = normalize_label(title)
        requirements: List[AdventureRequirement] = []
        for idx, req in enumerate(adventure.get("requirements", []), start=1):
            heading = str(req.get("heading", "")).strip()
            text = str(req.get("text", "")).strip()
            token_match = REQUIREMENT_CODE_TOKEN_RE.search(heading)
            token = token_match.group(1) if token_match else str(idx)
            requirements.append(
                {
                    "code": normalize_requirement_code(token),
                    "heading": heading or f"Requirement {idx}",
                    "text": text,
                }
            )
        catalog[key] = {
            "title": title,
            "link": str(adventure.get("link") or ""),
            "slug": str(adventure.get("slug") or ""),
            "requirements": requirements,
        }
    if not catalog:
        raise ValueError(f"No adventures parsed from {json_path}.")
    return catalog


def lookup_adventure(adventures: AdventureCatalog, name: str) -> Optional[AdventureRecord]:
    for key in adventure_lookup_keys(name):
        record = adventures.get(key)
        if record:
            return record
    return None


def get_requirement_detail(adventure: Optional[AdventureRecord], code: str) -> Optional[AdventureRequirement]:
    if not adventure:
        return None
    normalized = normalize_requirement_code(code)
    if not normalized:
        return None
    for req in adventure.get("requirements", []):
        if req.get("code") == normalized:
            return req
    return None


def render_adventure_label(name: str, adventures: AdventureCatalog) -> str:
    text = str(name or "").strip() or "Adventure"
    record = lookup_adventure(adventures, text)
    display = record.get("title") if record and record.get("title") else text
    escaped = html.escape(display)
    if record:
        link = str(record.get("link") or "").strip()
        if link:
            href = html.escape(link, quote=True)
            return f'<a href="{href}">{escaped}</a>'
    return escaped


def render_adventure_list_items(items: Iterable[str], adventures: AdventureCatalog) -> str:
    return "\n".join(
        f"            <li>{render_adventure_label(item, adventures)}</li>"
        for item in items
        if str(item or "").strip()
    )


def get_group_values(group: pd.DataFrame, column: str) -> List[str]:
    if column not in group:
        return []
    return group[column].astype(str).str.strip().tolist()


def parse_status(raw_value: str) -> Dict[str, object]:
    raw_text = str(raw_value or "").strip()
    lowered = raw_text.lower()
    is_awarded = "awarded" in lowered
    is_approved = "approved" in lowered or is_awarded
    pct_match = PERCENT_RE.search(raw_text)
    pct_complete = float(pct_match.group(1)) / 100.0 if pct_match else None

    if pct_complete is None and (is_awarded or is_approved):
        pct_complete = 1.0
    if pct_complete is None:
        pct_complete = 0.0
    if (is_awarded or is_approved) and pct_complete < 1.0:
        pct_complete = 1.0

    is_completed = pct_complete >= 1.0 or is_approved or is_awarded
    return {
        "raw_status": raw_text,
        "pct_complete": round(pct_complete, 4),
        "is_awarded": is_awarded,
        "is_approved": is_approved,
        "is_completed": is_completed,
    }


def gather_required_adventures(labels: Iterable[str]) -> List[str]:
    required: List[str] = []
    capturing = False
    for label in labels:
        text = (label or "").strip()
        if not text:
            continue
        parsed = parse_requirement_label(text)
        if parsed:
            code, adventure_name = parsed
            if adventure_name:
                capturing = True
                required.append(normalize_label(adventure_name))
            continue
        if capturing:
            break
    return required


def guess_rank_name(path: Path) -> str:
    match = re.search(r"Adventures_([^_]+)__", path.name)
    rank_name = match.group(1).strip() if match else path.stem
    normalized = normalize_label(rank_name)
    return CANONICAL_RANK_NAMES.get(normalized, rank_name)


def process_rank_file(path: Path, encoding: str) -> List[Dict[str, object]]:
    rank = guess_rank_name(path)
    df = pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
    label_col = df.columns[0]
    df = df.rename(columns={label_col: "label"})
    scout_columns = [
        c for c in df.columns if c != "label" and str(c).strip() and not str(c).startswith("Unnamed")
    ]
    labels = df["label"].astype(str).tolist()
    required_tokens = set(gather_required_adventures(labels))
    if not required_tokens:
        fallback = REQUIRED_ADVENTURES_BY_RANK.get(normalize_label(rank), [])
        if fallback:
            required_tokens = {normalize_label(name) for name in fallback}

    records: List[Dict[str, object]] = []
    current_adventure: Optional[str] = None
    current_required = False

    for _, row in df.iterrows():
        label = str(row["label"]).strip()
        if not label:
            continue
        normalized_label = normalize_label(label)

        if normalized_label in SKIP_LABELS or "v2024" in label.lower():
            continue
        if label.startswith("."):
            continue
        if REQUIREMENT_PREFIX_ONLY_RE.match(label):
            continue

        if normalize_label(label) == normalize_label(rank):
            for scout in scout_columns:
                status = parse_status(row[scout])
                records.append(
                    {
                        "scout": scout,
                        "scout_rank": rank,
                        "item": f"{rank} rank",
                        "subitem": "Rank",
                        "entry_type": "rank",
                        "requirement_code": "",
                        "requirement_text": "",
                        "is_required": True,
                        **status,
                        "source_file": path.name,
                    }
                )
            current_adventure = None
            current_required = False
            continue

        req_match = parse_requirement_label(label)
        if req_match:
            if not current_adventure:
                # Header sections such as "1a. Bobcat (AOL)" list required adventures
                # before any specific adventure rows; skip recording them as items.
                continue
            req_code, req_text = req_match
            for scout in scout_columns:
                status = parse_status(row[scout])
                records.append(
                    {
                        "scout": scout,
                        "scout_rank": rank,
                        "item": current_adventure,
                        "subitem": f"Req {req_code}",
                        "entry_type": "requirement",
                        "requirement_code": req_code,
                        "requirement_text": req_text,
                        "is_required": current_required,
                        **status,
                        "source_file": path.name,
                    }
                )
            continue

        current_adventure = label
        current_required = normalize_label(label) in required_tokens
        for scout in scout_columns:
            status = parse_status(row[scout])
            records.append(
                {
                    "scout": scout,
                    "scout_rank": rank,
                    "item": current_adventure,
                    "subitem": "Adventure",
                    "entry_type": "adventure",
                    "requirement_code": "",
                    "requirement_text": "",
                    "is_required": current_required,
                    **status,
                    "source_file": path.name,
                }
            )

    return records


def load_files(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]
    return sorted(input_path.glob(DEFAULT_PATTERN))


def has_aol_export(files: Iterable[Path]) -> bool:
    return any(normalize_label(guess_rank_name(path)) == "arrowoflight" for path in files)


def resolve_reports_dir(input_path: Path) -> Path:
    base_dir = input_path.parent if input_path.is_file() else input_path
    return base_dir / "reports"


def resolve_output_path(input_path: Path, output_path: Optional[Path]) -> Path:
    if output_path is not None:
        return Path(output_path)
    base_dir = input_path.parent if input_path.is_file() else input_path
    return base_dir / DEFAULT_OUTPUT_NAME


def resolve_roster_path(input_path: Path, roster_path: Optional[Path]) -> Path:
    if roster_path is not None:
        return Path(roster_path)
    search_dir = input_path.parent if input_path.is_file() else input_path
    matches = sorted(search_dir.glob("RosterReport_*Scouts_parents*.csv"))
    if not matches:
        raise FileNotFoundError(
            "No Scouts' Parents roster export was found beside the progress report exports. "
            "Provide --roster to point at the correct file."
        )
    if len(matches) > 1:
        print(f"Using latest roster export found in {search_dir}: {matches[-1].name}")
    return matches[-1]


def clean_roster_report(path: Path, encoding: str) -> pd.DataFrame:
    df = pd.read_csv(path, header=0, skiprows=1, encoding=encoding)
    rename_map = {
        " ": "ID",
        "Unnamed: 7": "Address 2",
        "Unnamed: 8": "Address 2",
        "Unnamed: 12": "DropMe",
        "Unnamed: 13": "DropMe",
    }
    df = df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns})

    required = [
        "ID",
        "Parent/Guardian Name ",
        "Relationship",
        "Address",
        "Den",
        "Home Phone",
        "Work Phone",
        "Mobile Phone",
        "Email",
        "Address 2",
        "First Name",
        "Last Name",
    ]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Roster report missing columns: {', '.join(missing)}")

    for column in [
        "Parent/Guardian Name ",
        "Relationship",
        "Address",
        "Den",
        "Home Phone",
        "Work Phone",
        "Mobile Phone",
        "Email",
        "Address 2",
    ]:
        if column in df.columns:
            df[column] = df[column].astype(object)

    mask_missing_id = df["ID"].isna()
    if mask_missing_id.any():
        shift_source = df.copy(deep=True)
        suffix_names = shift_source.loc[mask_missing_id, "Suffix"].astype(str).str.strip()
        has_suffix_name = suffix_names.astype(bool)
        df.loc[mask_missing_id & has_suffix_name, "Parent/Guardian Name "] = suffix_names[has_suffix_name]
        df.loc[mask_missing_id & ~has_suffix_name, "Parent/Guardian Name "] = shift_source.loc[mask_missing_id & ~has_suffix_name, "Den"]
        df.loc[mask_missing_id, "Relationship"] = shift_source.loc[mask_missing_id, "Den"]
        df.loc[mask_missing_id, "Address"] = shift_source.loc[mask_missing_id, "Relationship"]
        df.loc[mask_missing_id, "Den"] = None

        df.loc[mask_missing_id, "Home Phone"] = shift_source.loc[mask_missing_id, "Address"]
        df.loc[mask_missing_id, "Work Phone"] = shift_source.loc[mask_missing_id, "Home Phone"]
        df.loc[mask_missing_id, "Mobile Phone"] = shift_source.loc[mask_missing_id, "Work Phone"]

        email_columns = [
            "Email",
            "Mobile Phone",
            "Work Phone",
            "Home Phone",
            "Address 2",
            "Address",
        ]
        for idx in df.index[mask_missing_id]:
            email_value: Optional[str] = None
            for column in email_columns:
                if column not in shift_source.columns:
                    continue
                raw_value = shift_source.at[idx, column]
                if not isinstance(raw_value, str):
                    continue
                candidate = raw_value.strip()
                if "@" in candidate:
                    email_value = candidate
                    break
            if email_value:
                df.at[idx, "Email"] = email_value
        df.loc[mask_missing_id, "Address 2"] = None

    if "DropMe" in df.columns:
        df = df.drop(columns=["DropMe"])

    for column in ["First Name", "Last Name", "Den", "ID"]:
        if column in df.columns:
            df[column] = df[column].ffill()

    return df


def load_roster(path: Path, encoding: str) -> pd.DataFrame:
    try:
        roster_df = clean_roster_report(path, encoding)
    except (KeyError, ValueError, pd.errors.ParserError):
        roster_df = pd.read_csv(path, dtype=str, encoding=encoding)
    roster_df = roster_df.fillna("")
    roster_df = roster_df.rename(
        columns=lambda c: c.strip().lower().replace(" ", "_").replace("/", "_")
    )
    if "first_name" not in roster_df or "last_name" not in roster_df:
        raise ValueError("Roster file missing first/last name columns.")
    for column in ["first_name", "last_name", "parent_guardian_name"]:
        if column in roster_df:
            roster_df[column] = roster_df[column].map(repair_mojibake)
    roster_df["scout"] = (
        roster_df["first_name"].str.strip() + " " + roster_df["last_name"].str.strip()
    ).str.replace(r"\s+", " ", regex=True)
    return roster_df


def build_parent_directory(roster_df: pd.DataFrame) -> pd.DataFrame:
    parent_rows: List[Dict[str, object]] = []
    for scout, group in roster_df.groupby("scout"):
        if not scout:
            continue
        names = unique_preserve_order(get_group_values(group, "parent_guardian_name"))
        emails = unique_preserve_order(get_group_values(group, "email"))
        contact_entries: List[str] = []
        for _, row in group.iterrows():
            name = str(row.get("parent_guardian_name", "")).strip()
            email = str(row.get("email", "")).strip()
            relationship = str(row.get("relationship", "")).strip()
            if not name and not email:
                continue
            pieces = [name] if name else []
            if relationship:
                pieces.append(f"({relationship})")
            if email:
                pieces.append(f"<{email}>")
            contact_entries.append(" ".join(pieces).strip())

        parent_rows.append(
            {
                "scout": scout,
                "den": group["den"].iloc[0] if "den" in group else "",
                "parent_names": "; ".join(names),
                "parent_emails": "; ".join(emails),
                "parent_contacts": "; ".join(contact_entries),
                "primary_parent_name": names[0] if names else "",
                "primary_parent_email": emails[0] if emails else "",
                "guardian_count": len(contact_entries),
            }
        )

    return pd.DataFrame(parent_rows)


def merge_parent_contacts(progress_df: pd.DataFrame, roster_path: Path, encoding: str) -> pd.DataFrame:
    roster_df = load_roster(roster_path, encoding)
    parent_df = build_parent_directory(roster_df)
    if parent_df.empty:
        for col in [
            "den",
            "parent_names",
            "parent_emails",
            "parent_contacts",
            "primary_parent_name",
            "primary_parent_email",
            "guardian_count",
        ]:
            progress_df[col] = ""
        progress_df["guardian_count"] = 0
        return progress_df

    merged = progress_df.merge(parent_df, on="scout", how="left")
    for col in [
        "den",
        "parent_names",
        "parent_emails",
        "parent_contacts",
        "primary_parent_name",
        "primary_parent_email",
        "guardian_count",
    ]:
        if col not in merged:
            merged[col] = ""
        if col == "guardian_count":
            merged[col] = merged[col].fillna(0).astype(int)
        else:
            merged[col] = merged[col].fillna("")
    return merged


def format_percent(value: float) -> str:
    value = float(value or 0.0)
    return f"{round(value * 100):d}%"


def rank_slug(rank: str) -> str:
    key = rank.lower()
    return RANK_ADVENTURE_LINKS.get(key, key.replace(" ", "-"))


def generate_reports(
    df: pd.DataFrame,
    reports_dir: Path,
    report_date: dt.date,
    adventures: AdventureCatalog,
    *,
    from_name: str = DEFAULT_FROM_NAME,
    email_sender: Optional[SummaryEmailSender] = None,
) -> None:
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    as_of_display = report_date.strftime("%B %d, %Y")
    adventure_df = df[df["entry_type"] == "adventure"]
    peer_completion = (
        adventure_df.groupby(["scout_rank", "item"])["is_completed"].mean().to_dict()
    )

    for scout, scout_rows in df.groupby("scout"):
        if scout_rows.empty:
            continue
        scout_rank = scout_rows["scout_rank"].iloc[0]

        def _clean(value: object) -> str:
            text = str(value or "").strip()
            return "" if text.lower() == "nan" else text

        parent_field = _clean(scout_rows.get("parent_names", pd.Series([""])).iloc[0])
        parent_list = [name.strip() for name in parent_field.split(";") if name.strip()]
        primary_parent = _clean(scout_rows.get("primary_parent_name", pd.Series([""])).iloc[0])
        if not parent_list and primary_parent:
            parent_list = [primary_parent]
        if not parent_list:
            parent_list = ["Parent/Guardian"]
        parent_email_field = _clean(scout_rows.get("parent_emails", pd.Series([""])).iloc[0])
        parent_emails = split_semicolon_list(parent_email_field)

        rank_row = scout_rows[scout_rows["entry_type"] == "rank"]
        rank_pct = rank_row["pct_complete"].iloc[0] if not rank_row.empty else 0.0
        rank_complete = rank_pct >= 0.999

        required_remaining = sorted(
            set(
                scout_rows[
                    (scout_rows["entry_type"] == "adventure")
                    & (scout_rows["is_required"])
                    & (~scout_rows["is_completed"])
                ]["item"].tolist()
            )
        )

        adventure_rows = scout_rows[scout_rows["entry_type"] == "adventure"]
        elective_rows = adventure_rows[~adventure_rows["is_required"]]
        electives_completed = int(elective_rows["is_completed"].sum())
        electives_remaining_rank = max(0, RANK_ELECTIVE_REQUIREMENT - electives_completed)
        super_total = SUPER_ACHIEVER_ELECTIVES.get(scout_rank.lower(), 12)
        electives_remaining_super = max(0, super_total - electives_completed)

        popular_missed: List[str] = []
        for item, item_rows in adventure_rows.groupby("item"):
            peer_key = (scout_rank, item)
            peer_rate = peer_completion.get(peer_key)
            if peer_rate is None or peer_rate < POPULAR_THRESHOLD:
                continue
            if not item_rows["is_completed"].any():
                popular_missed.append(item)
        popular_missed.sort()

        belt_loops = unique_preserve_order(
            adventure_rows[adventure_rows["is_awarded"]]["item"].astype(str).tolist()
        )

        req_rows = scout_rows[scout_rows["entry_type"] == "requirement"]
        adventure_requirement_summary: Dict[str, Dict[str, int]] = {}
        for item, group in req_rows.groupby("item"):
            total = len(group)
            remaining = int((~group["is_completed"]).sum())
            adventure_requirement_summary[item] = {
                "remaining": remaining,
                "total": total,
            }

        detail_sections_html = render_detail_sections(
            scout_name=scout,
            scout_rank=scout_rank,
            rows=scout_rows,
            adventure_lookup=adventures,
            peer_completion=peer_completion,
        )

        parent_records: List[Tuple[str, str]] = []
        if parent_list:
            for idx, parent_name in enumerate(parent_list):
                candidate_email = parent_emails[idx] if idx < len(parent_emails) else (parent_emails[0] if parent_emails else "")
                parent_records.append((parent_name, candidate_email))
        else:
            parent_records = [("Parent/Guardian", parent_emails[0] if parent_emails else "")]

        summary_slug = slugify(scout, "_")
        for parent_name, parent_email in parent_records:
            summary_html = render_summary_html(
                scout_name=scout,
                scout_rank=scout_rank,
                parent_name=parent_name,
                report_date=as_of_display,
                from_name=from_name,
                rank_pct=rank_pct,
                rank_complete=rank_complete,
                required_remaining=required_remaining,
                electives_completed=electives_completed,
                electives_remaining_rank=electives_remaining_rank,
                super_total=super_total,
                electives_remaining_super=electives_remaining_super,
                popular_missed=popular_missed,
                belt_loops=belt_loops,
                adventure_requirement_summary=adventure_requirement_summary,
                adventure_link_slug=rank_slug(scout_rank),
                adventure_lookup=adventures,
                peer_completion=peer_completion,
                detail_sections_html=detail_sections_html,
            )
            parent_slug = slugify(parent_name or "guardian", "_")
            (reports_dir / f"{summary_slug}_for_{parent_slug}_summary.html").write_text(
                summary_html,
                encoding="utf-8",
            )
            if email_sender:
                email_sender.send_summary(
                    scout_name=scout,
                    parent_name=parent_name,
                    parent_email=parent_email,
                    summary_html=summary_html,
                )


def render_summary_html(
    *,
    scout_name: str,
    scout_rank: str,
    parent_name: str,
    report_date: str,
    from_name: str,
    rank_pct: float,
    rank_complete: bool,
    required_remaining: List[str],
    electives_completed: int,
    electives_remaining_rank: int,
    super_total: int,
    electives_remaining_super: int,
    popular_missed: List[str],
    belt_loops: List[str],
    adventure_requirement_summary: Dict[str, Dict[str, int]],
    adventure_link_slug: str,
    adventure_lookup: AdventureCatalog,
    peer_completion: Dict[Tuple[str, str], float],
    detail_sections_html: str,
) -> str:
    scout_display_name = repair_mojibake(scout_name)
    parent_display_name = repair_mojibake(parent_name)
    sender_display_name = repair_mojibake(from_name)
    parent_line = html.escape(parent_display_name or "Parent/Guardian")
    scout_name_html = html.escape(scout_display_name)
    rank_progress = format_percent(rank_pct)
    required_rows: List[str] = []
    for item in required_remaining:
        label_html = render_adventure_label(item, adventure_lookup)
        peer_rate = peer_completion.get((scout_rank, item))
        note = ""
        if peer_rate is not None and peer_rate < POPULAR_THRESHOLD:
            note = " <em>(Less than half of the Den has completed this adventure, so it probably hasn't been held yet.)</em>"
        required_rows.append(f"            <li>{label_html}{note}</li>")
    required_section = "\n".join(required_rows)
    if not required_section:
        required_section = "            <li>None \u2013 all required adventures are complete.</li>"
    rank_icon = "✅" if rank_complete else "⏳"
    rank_card_class = "done" if rank_complete else "pending"
    rank_message = (
        f"Congratulations on earning your {html.escape(scout_rank)}!"
        if rank_complete
        else f"Let us know if we're missing any completed items for your {html.escape(scout_rank)}!"
    )
    rank_card = f"""
        <article class="summary-card {rank_card_class}">
            <div class="card-title">{rank_icon} Rank Progress</div>
            <p class="summary-value">{rank_progress}</p>
            <p>{rank_message}</p>
        </article>
    """

    adventure_counts_card = f"""
        <article class="summary-card">
            <div class="card-title">Adventures Remaining</div>
            <p><em>Required Adventures:</em> {len(required_remaining)}</p>
            <p><em>Electives for Rank:</em> {electives_remaining_rank}</p>
            <p><em>Electives for Super Achiever:</em> {electives_remaining_super}</p>
        </article>
    """

    if belt_loops:
        belt_items = render_adventure_list_items(belt_loops, adventure_lookup)
        belt_body = f"""
            <p>You may wish to verify that you have all of these belt loops or pins.</p>
            <ul>
{belt_items}
            </ul>
        """
    else:
        belt_body = "<p>None recorded yet—please let us know if any are missing.</p>"
    belt_card = f"""
        <article class="summary-card">
            <div class="card-title">Belt Loops / Pins Awarded</div>
            {belt_body}
        </article>
    """

    missed_card = ""
    if popular_missed:
        missed_items: List[str] = []
        for item in popular_missed:
            label_html = render_adventure_label(item, adventure_lookup)
            stats = adventure_requirement_summary.get(item)
            detail_note = ""
            if stats and stats.get("total"):
                remaining = stats.get("remaining", 0)
                total = stats.get("total", 0)
                detail_note = (
                    f" <span class=\"requirement-note\">({remaining} of {total} requirements still pending—see details below.)</span>"
                )
            missed_items.append(f"                <li>{label_html}{detail_note}</li>")
        missed_list = "\n".join(missed_items)
        missed_card = f"""
        <article class="summary-card alert">
            <div class="card-title">⚠ Missed Adventures</div>
            <p>At least half of your Den has completed these missing adventures. Your Scout may have missed a meeting or have a do-at-home requirement remaining.</p>
            <ul>
{missed_list}
            </ul>
        </article>
        """

    summary_cards_html = f"""
    <section class="summary-grid">
{rank_card}
{adventure_counts_card}
{belt_card}
{missed_card}
    </section>
    """
    adventure_url = f"https://www.scouting.org/programs/cub-scouts/adventures/{adventure_link_slug}/"
    is_aol = normalize_label(scout_rank) in {"aol", "arrowoflight"}
    if is_aol:
        closing_note_html = (
            f"<p>Congratulations on everything {scout_name_html} has accomplished in Cub Scouts. "
            "We can't wait to see everything they'll achieve after crossover next weekend!</p>"
        )
    else:
        closing_note_html = (
            f"<p>Thanks for supporting {scout_name_html}! Cub Scouting is a family program, so if you ever work toward Adventures at home, please let your den leaders know!</p>"
        )

    detail_section = ""
    if detail_sections_html.strip():
        detail_section = f"""
    <hr />
    <section class=\"detail-sections\">
        <h2>Detailed Requirements Snapshot</h2>
        <p>Here is the requirement-level view in case you prefer everything in one place.</p>
{detail_sections_html}
    </section>
    """

    return f"""
<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <title>{html.escape(scout_name)} – Progress Summary</title>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.5; margin: 1.5rem; max-width: 720px; }}
        ul {{ margin-top: 0.3rem; }}
        hr {{ margin: 2rem 0; border: none; border-top: 1px solid #ccc; }}
        .detail-sections {{ margin-top: 1rem; }}
        .detail-sections h2 {{ margin-bottom: 0.3rem; }}
        .detail-sections p {{ margin-top: 0.2rem; }}
        .detail-block {{ margin-bottom: 1.2rem; }}
        .required-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1rem;
        }}
        .required-card {{
            border: 1px solid #d0d7de;
            border-radius: 0.5rem;
            padding: 0.9rem;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
        }}
        .required-card.done {{ border-color: #2da44e; }}
        .required-card.pending {{ border-color: #f7a600; }}
        .required-title {{ font-weight: 600; margin-bottom: 0.4rem; }}
        .den-note {{ font-size: 0.9rem; color: #555; font-style: italic; margin-bottom: 0.4rem; }}
        .summary-grid {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 1rem;
            margin: 1rem 0 1.5rem;
        }}
        .summary-card {{
            border: 1px solid #d0d7de;
            border-radius: 0.5rem;
            padding: 0.9rem;
            background: #fff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
        }}
        .summary-card .card-title {{ font-weight: 600; margin-bottom: 0.4rem; }}
        .summary-card.done {{ border-color: #2da44e; }}
        .summary-card.pending {{ border-color: #f7a600; }}
        .summary-card.alert {{ border-color: #d93025; }}
        .summary-value {{ font-size: 2rem; font-weight: 600; margin: 0.2rem 0 0.4rem; }}
        .requirement-note {{ font-size: 0.9rem; color: #555; font-style: italic; }}
    </style>
</head>
<body>
    <p>Hi {parent_line},</p>
    <p>This is your end-of-year personalized Pack 500 progress report on <strong>{scout_name_html}</strong> as of {html.escape(report_date)}. This report reflects our records in Scoutbook, so please alert your Den Leaders if something seems off.</p>
    {summary_cards_html}
    <p>You can see your Scout's advancement records at <a href=\"https://advancements.scouting.org/\">advancements.scouting.org</a> and read the requirements at <a href=\"{adventure_url}\">scouting.org/programs/cub-scouts/adventures/{html.escape(adventure_link_slug)}</a>.</p>
    {closing_note_html}
    <p>As always, feel free to reach out with any questions, comments, or concerns about your Scout, your den, or our pack.</p>
    <p>~{html.escape(sender_display_name or DEFAULT_FROM_NAME)}</p>
{detail_section}
</body>
</html>
"""


def render_detail_sections(
    *,
    scout_name: str,
    scout_rank: str,
    rows: pd.DataFrame,
    adventure_lookup: AdventureCatalog,
    peer_completion: Dict[Tuple[str, str], float],
) -> str:
    adventure_rows = rows[rows["entry_type"] == "adventure"]
    required_items = unique_preserve_order(
        adventure_rows[adventure_rows["is_required"]]["item"].astype(str).tolist()
    )
    elective_items = unique_preserve_order(
        adventure_rows[~adventure_rows["is_required"]]["item"].astype(str).tolist()
    )

    required_cards: List[str] = []
    for item in required_items:
        item_rows = adventure_rows[adventure_rows["item"] == item]
        is_done = bool(item_rows["is_completed"].any()) if not item_rows.empty else False
        req_rows = rows[(rows["entry_type"] == "requirement") & (rows["item"] == item)]
        adventure_record = lookup_adventure(adventure_lookup, item)
        title_html = render_adventure_label(item, adventure_lookup)
        missing_labels: List[str] = []
        for _, req_row in req_rows.iterrows():
            if bool(req_row["is_completed"]):
                continue
            code = str(req_row.get("requirement_code", "")).strip()
            detail = get_requirement_detail(adventure_record, code)
            if detail:
                heading = str(detail.get("heading", code or "Requirement"))
                text = str(detail.get("text", "")).strip()
                label = f"{heading}: {text}" if text else heading
            else:
                text = str(req_row.get("requirement_text", "")).strip()
                if code and text:
                    label = f"{code}. {text}"
                elif code:
                    label = code
                else:
                    label = text or "Requirement pending"
            missing_labels.append(label)

        if not missing_labels and not is_done:
            raw_status_values = (
                unique_preserve_order(item_rows["raw_status"].astype(str).tolist())
                if not item_rows.empty
                else []
            )
            status_text = ", ".join(filter(None, raw_status_values)) or "unknown status"
            source_files = (
                unique_preserve_order(item_rows["source_file"].astype(str).tolist())
                if "source_file" in item_rows
                else []
            )
            source_text = ", ".join(filter(None, source_files)) or "unknown source"
            raise ValueError(
                "Inconsistent adventure detail for"
                f" {scout_name} ({scout_rank}) on '{item}': status {status_text}"
                " but no incomplete requirement rows were found."
                f" Source CSV: {source_text}. Please fix the source data."
            )

        if is_done:
            body_html = "<p>All requirements completed!</p>"
        elif missing_labels:
            missing_items = "\n".join(
                f"                <li>{html.escape(label)}</li>" for label in missing_labels
            )
            body_html = f"""
        <p>Missing requirements:</p>
        <ul>
{missing_items}
        </ul>
        """
        else:
            body_html = "<p>Adventure is in progress.</p>"
        icon = "✅" if is_done else "⏳"
        card_class = "required-card done" if is_done else "required-card pending"
        den_note_html = ""
        if not is_done:
            peer_rate = peer_completion.get((scout_rank, item))
            if peer_rate is not None and peer_rate < POPULAR_THRESHOLD:
                den_note_html = """
        <p class=\"den-note\">Pack note: Less than half of the den has finished this yet, so it probably has not been covered in a den meeting.</p>
        """
        required_cards.append(
            f"""
        <article class=\"{card_class}\">
            <div class=\"required-title\">{icon} {title_html}</div>
            {den_note_html}
            {body_html}
        </article>
        """
        )
    required_section = "\n".join(required_cards) or "<p>No required adventures recorded yet.</p>"

    elective_cards: List[str] = []
    for item in elective_items:
        item_rows = adventure_rows[adventure_rows["item"] == item]
        is_done = bool(item_rows["is_completed"].any()) if not item_rows.empty else False
        icon = "✅" if is_done else "⏳"
        card_class = "required-card done" if is_done else "required-card pending"
        title_html = render_adventure_label(item, adventure_lookup)
        elective_cards.append(
            f"""
        <article class=\"{card_class}\">
            <div class=\"required-title\">{icon} {title_html}</div>
        </article>
        """
        )
    elective_section = "\n".join(elective_cards) or "<p>No elective adventures recorded yet.</p>"

    return f"""
    <section class=\"detail-block\">
        <h3>Required Adventures</h3>
        <div class=\"required-grid\">
{required_section}
        </div>
    </section>
    <section class=\"detail-block\">
        <h3>Elective Adventures</h3>
        <div class=\"required-grid\">
{elective_section}
        </div>
    </section>
    """

def main() -> None:
    args = parse_args()
    input_path = Path(args.input_dir)
    roster_path = resolve_roster_path(input_path, args.roster)
    reports_dir = resolve_reports_dir(input_path)
    files = load_files(input_path)
    if not files:
        raise FileNotFoundError(
            f"No files matching pattern '{DEFAULT_PATTERN}' found under {args.input_dir}."
        )
    if not has_aol_export(files):
        print(f"No AOL progress export found under {args.input_dir}; continuing without AOL reports.")

    all_records: List[Dict[str, object]] = []
    for file_path in files:
        all_records.extend(process_rank_file(file_path, DEFAULT_INPUT_ENCODING))

    if not all_records:
        raise RuntimeError("No progress records were extracted. Check the source CSV structure.")

    df = pd.DataFrame(all_records)
    df = merge_parent_contacts(df, roster_path, DEFAULT_INPUT_ENCODING)

    output_path = resolve_output_path(input_path, args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sort_cols = [
        "scout_rank",
        "scout",
        "item",
        "entry_type",
        "requirement_code",
        "subitem",
    ]
    df = df.sort_values(sort_cols, ignore_index=True)
    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df)} records to {output_path}")

    if not args.skip_html:
        report_date = (
            dt.datetime.strptime(args.report_date, "%Y-%m-%d").date()
            if args.report_date
            else dt.date.today()
        )
        adventure_catalog = load_adventure_requirements(DEFAULT_ADVENTURE_JSON)
        email_sender = None
        if args.send_email:
            preview_recipient = None
            if not args.send_to_parents:
                preview_recipient = args.preview_recipient.strip() if args.preview_recipient else None
            email_sender = build_summary_email_sender(
                client_secret=DEFAULT_GMAIL_CLIENT_SECRET,
                token_path=DEFAULT_GMAIL_TOKEN,
                from_name=args.from_name,
                from_email=args.from_email,
                preview_recipient=preview_recipient,
                max_emails=args.max_emails,
            )
        generate_reports(
            df,
            reports_dir,
            report_date,
            adventure_catalog,
            from_name=args.from_name,
            email_sender=email_sender,
        )
        print(f"Summary HTML reports written to {reports_dir}")


if __name__ == "__main__":
    main()

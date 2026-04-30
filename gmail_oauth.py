"""Shared Gmail OAuth helpers for Pack 500 scripts."""
from __future__ import annotations

from pathlib import Path
from typing import Sequence

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ModuleNotFoundError:
    Request = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]
    InstalledAppFlow = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]


def _authorize_with_console(flow) -> object:
    auth_url, _ = flow.authorization_url(prompt="consent")
    print("Open this URL to authorize Gmail access:")
    print(auth_url)
    auth_code = input("Paste the authorization code here: ").strip()
    if not auth_code:
        raise RuntimeError("No authorization code was provided.")
    flow.fetch_token(code=auth_code)
    return flow.credentials


def run_installed_app_flow(flow, auth_mode: str) -> object:
    selected_mode = (auth_mode or "auto").strip().lower()
    if selected_mode not in {"auto", "local-server", "console"}:
        raise ValueError(
            f"Unsupported Gmail auth mode '{auth_mode}'. Choose auto, local-server, or console."
        )

    if selected_mode == "console":
        return _authorize_with_console(flow)
    if selected_mode == "local-server":
        return flow.run_local_server(port=0)

    try:
        return flow.run_local_server(port=0)
    except Exception as exc:
        print(
            "Local browser-based Gmail auth was not available; falling back to copy/paste auth. "
            f"Reason: {exc}"
        )
        return _authorize_with_console(flow)


def build_gmail_service(
    client_secret_path: Path,
    token_path: Path,
    scopes: Sequence[str],
    *,
    auth_mode: str = "auto",
):
    if not all([Credentials, InstalledAppFlow, build, Request]):
        raise ImportError(
            "Google API libraries are required for Gmail sending. Install google-auth-oauthlib and google-api-python-client."
        )

    client_secret_path = Path(client_secret_path)
    token_path = Path(token_path)
    if not client_secret_path.exists():
        raise FileNotFoundError(f"Missing Gmail client secret: {client_secret_path}")

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), list(scopes))

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_path), list(scopes))
            creds = run_installed_app_flow(flow, auth_mode)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)

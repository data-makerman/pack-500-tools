# Pack 500 Handoff Setup

This repo is now set up so the next Cubmaster can run the main workflows without depending on your local machine layout.

## Recommended platform

Use GitHub Codespaces if possible. It is the cleanest handoff path because the repo now includes a dev container and a `requirements.txt` file.

Google Colab can work for one-off runs, but it is less convenient because the user has to upload CSVs and credential files into the notebook session each time. If someone only wants one environment, choose Codespaces.

## First-time setup in Codespaces

1. Open the repo in GitHub Codespaces.
2. Wait for the dev container to finish installing dependencies.
3. Create a folder for local secrets:

```bash
mkdir -p .secrets
```

4. Put the Google OAuth desktop-app client JSON at `.secrets/gmail_client_secret.json`.
5. The first time a script sends email, let it create `.secrets/gmail_token.json`.

Recommended environment variables:

```bash
export PACK500_GMAIL_CLIENT_SECRET=.secrets/gmail_client_secret.json
export PACK500_GMAIL_TOKEN=.secrets/gmail_token.json
export PACK500_GMAIL_AUTH_MODE=console
```

Use `console` auth mode in Codespaces because browser callback handling is less predictable than a copy/paste OAuth code.

## First-time setup on a local machine

1. Install Python 3.11 or newer.
2. From the repo root, create and activate a virtual environment.
3. Install runtime packages:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

4. If you want to run `scan_pii.py`, also install the heavier optional stack:

```bash
python -m pip install -r requirements-pii.txt
python -m spacy download en_core_web_lg
```

5. Put the Gmail client secret somewhere outside tracked files or under `.secrets/`.

## Create the Gmail OAuth client

These scripts use the Gmail API for real sending.

### What account to use

Use the Pack's Google Workspace account, not a personal Gmail account. For example, if Pack 500 has a role mailbox such as `cubmaster@pack500.org`, create the OAuth client under a Google Cloud project owned by that Workspace organization and authorize the scripts with that Pack account.

### Step-by-step

1. Sign into [Google Cloud Console](https://console.cloud.google.com/) with the Pack Google Workspace account or another Workspace admin account that manages Pack 500.
2. Create a new Google Cloud project for this repo, or reuse an existing Pack-owned project. A name like `Pack 500 Tools` is fine.
3. In that project, open `APIs & Services` -> `Library`.
4. Search for `Gmail API` and click `Enable`.
5. Open `APIs & Services` -> `OAuth consent screen`.
6. If Google asks for a user type and `Internal` is available, choose `Internal`. That is usually the right choice for a nonprofit Google Workspace when only Pack-owned Workspace users need to authorize the app.
7. Fill in the app name, support email, and developer contact email. Using Pack-owned addresses is better than a personal address.
8. On scopes, add the Gmail send scope if prompted: `https://www.googleapis.com/auth/gmail.send`.
9. Save the consent screen configuration.
10. Open `APIs & Services` -> `Credentials`.
11. Click `Create Credentials` -> `OAuth client ID`.
12. Choose application type `Desktop app`.
13. Give it a clear name such as `Pack 500 Scripts Desktop Client`.
14. Create the credential, then click `Download JSON`.
15. Save the downloaded file as `.secrets/gmail_client_secret.json`, or store it elsewhere and pass the path with `--gmail-client-secret`.

### Notes for Google Workspace for Nonprofits

- Google Workspace for Nonprofits still uses normal Google Cloud projects and OAuth credentials. The nonprofit status does not change the Gmail API setup steps.
- You do not need a web app OAuth client for these scripts. Use `Desktop app` only.
- The first person who runs the script will authorize Gmail sending for the Pack account and produce `.secrets/gmail_token.json`.
- If `Internal` is not available on the consent screen, use `External` and add the Pack Gmail account as a test user.

### If Workspace admin approval is needed

If Google blocks the OAuth flow or says the app is restricted by admin policy, a Google Workspace admin may need to review API access in the Admin console.

Common places to check:

1. Google Admin console -> `Security` -> `Access and data control` -> `API controls`.
2. Confirm that users are allowed to grant access to Google services, including Gmail.
3. If the org uses app restrictions, mark the new OAuth app or Cloud project as trusted for internal use.

If the Pack account can log into Cloud Console, enable Gmail API, and complete the OAuth consent screen as an internal app, that is usually enough.

## First Gmail token run

All email-capable scripts now accept:

- `--gmail-client-secret`
- `--gmail-token`
- `--gmail-auth-mode auto|local-server|console`

For remote environments, prefer `--gmail-auth-mode console`.

Example:

```bash
python progress_reports/create_progress_reports.py \
  --input progress_reports/2026-04 \
  --roster progress_reports/2026-04/RosterReport_Pack0500_Scouts_parents_20260430.csv \
  --send-email \
  --gmail-client-secret .secrets/gmail_client_secret.json \
  --gmail-token .secrets/gmail_token.json \
  --gmail-auth-mode console
```

The script will print an authorization URL. Open it, approve access, paste the returned code into the terminal, and the token cache will be written automatically.

## Main workflows

Progress reports:

```bash
python progress_reports/create_progress_reports.py \
  --input progress_reports/2026-04 \
  --roster progress_reports/2026-04/RosterReport_Pack0500_Scouts_parents_20260430.csv \
  --reports-dir progress_reports/2026-04/reports \
  --report-date 2026-04-30
```

Membership renewal reminders:

```bash
python member_notices/notify_renewals_and_lapses.py \
  --non-renewed member_notices/2026-03/NonRenewedMembership.csv \
  --roster member_notices/2026-03/Roster_Report.csv
```

Family talent reminders:

```bash
python family_talent/remind_missing_talents.py \
  --talent-survey "family_talent/Family Talent Survey (Responses) - Form Responses 1.csv" \
  --adult-roster roster_exports/2026/adults/RosterReport_MultiUnit_Adults_20260111.csv
```

Adventure requirements refresh:

```bash
python scrape_adventure_requirements.py --output adventure_requirements.json
```

## Colab notes

Colab is viable for manual runs if needed:

1. Open the repo in Colab from GitHub or upload the scripts.
2. Install dependencies in a cell: `!python -m pip install -r requirements.txt`
3. Upload the CSV inputs and Gmail client secret into the runtime session.
4. Run the scripts with `--gmail-auth-mode console`.

This works, but it is not ideal for repeated monthly operations because uploaded files disappear when the runtime resets.

## Operational guardrails

- Keep live roster exports, membership reports, and generated HTML files out of git.
- Keep Gmail secrets under `.secrets/` or another ignored location.
- Use preview mode before any live send.
- For live sends, test with `--max-emails 3` first.

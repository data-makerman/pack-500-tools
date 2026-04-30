# Pack 500 Tools

Pack 500 Tools is a small collection of scripts for turning Scoutbook Plus and other Scouting exports into parent-facing reports, printable checklists, and operational follow-up lists.

## Start Here

If you need to do something in this repo, start with one of these entry points:

- Progress reports for families: `progress_reports/create_progress_reports.py`
- Printable rank roster sheets: `generate_roster_checklist.py`
- Membership renewal and lapse emails: `member_notices/notify_renewals_and_lapses.py`
- Family talent survey follow-up: `family_talent/remind_missing_talents.py`
- Refresh the adventure requirements dataset: `scrape_adventure_requirements.py`
- Scan tracked files for likely PII before publishing: `scan_pii.py`

Everything else is either supporting data or archived reference material.

The repository is set up for a public GitHub workflow:

- Raw roster exports, progress reports, membership files, PDFs, and other files containing family PII are gitignored.
- Gmail OAuth credentials and token caches are gitignored.
- Secret and PII scanning are wired into pre-commit with `detect-secrets`, `gitleaks`, `nbstripout`, and the local `scan_pii.py` Presidio wrapper.

## Active scripts

- `progress_reports/create_progress_reports.py`: combine rank-specific Adventures exports with a Scouts' Parents roster export, then build per-Scout HTML progress reports and optional Gmail drafts/sends.
- `generate_roster_checklist.py`: turn a roster export into a printable rank-by-rank checklist packet.
- `member_notices/notify_renewals_and_lapses.py`: build renewal and lapsed-membership reminder emails from BSA membership reports.
- `family_talent/remind_missing_talents.py`: compare the adult roster against Family Talent survey responses and prepare reminder emails.
- `scrape_adventure_requirements.py`: refresh `adventure_requirements.json` from scouting.org.
- `scan_pii.py`: scan text files for likely names, emails, and phone numbers before publishing changes.

Historical and one-off helpers now live under `archive/`. See `archive/README.md` for what was moved there and why.

## Recommended setup

1. Create a Python environment and install the libraries required by the scripts you plan to run.
2. Install pre-commit and enable hooks with `pre-commit install`.
3. Keep all live exports and credentials outside git-tracked paths unless they have been explicitly sanitized.
4. Before publishing, run `pre-commit run --all-files` and review the git diff for any newly introduced names, emails, or report output.

## Public repo conventions

- Use role-based defaults such as `Pack 500 Cubmaster` and `cubmaster@pack500.org` rather than personal addresses in committed code.
- Prefer sanitized fixtures under `sample_data/` if you need example inputs for future contributors.
- Treat dated folders such as `graduation_2025/` and `wolves_2025/` as historical examples, not stable APIs.

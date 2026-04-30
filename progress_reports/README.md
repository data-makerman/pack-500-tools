# Progress Reports

`create_progress_reports.py` builds parent-facing HTML progress reports from Scoutbook Plus advancement exports plus a Scouts' Parents roster export.

## Export the source files from Scoutbook Plus

For each rank you want to include:

1. Open the `Adventures` report in Scoutbook Plus.
2. Filter the report so it includes only Scouts in the rank you are exporting.
3. Confirm the report is also scoped to that same rank's Adventures. Check this every time before running the report because saved filters can drift.
4. Run the report and export the CSV.
5. Repeat for Lions, Tigers, Wolves, Bears, Webelos, and Arrow of Light as needed.

Also export the `Scouts' Parents` report for the same time period. That roster export is how this script attaches parent names and email addresses to each Scout.

Keep all of those CSVs together in a dated folder such as `progress_reports/2026-03/`.

## What the script expects

- One or more `ReportBuilder_Pack0500_Adventures_*__*.csv` files in the input directory.
- One `RosterReport_Pack0500_Scouts_parents_*.csv` roster export.
- `adventure_requirements.json` in the repo root.

The script can now normalize the raw Scouts' Parents roster export directly, so `archive/helpers/fix_roster.py` is no longer part of the normal workflow.

## Example run

```powershell
F:/Scouts/.venv/Scripts/python.exe progress_reports/create_progress_reports.py \
  --input progress_reports/2026-03 \
  --report-date 2026-03-15
```

If the dated folder contains exactly one matching `RosterReport_*Scouts_parents*.csv`, the script will pick it automatically. It also now defaults the outputs to:

- `progress_reports/2026-03/progress_report_aggregate.csv`
- `progress_reports/2026-03/reports/`

Use `--roster` only when the folder contains multiple roster exports or the roster lives elsewhere.

Remote-safe Gmail auth is also available for Codespaces or Colab:

```powershell
F:/Scouts/.venv/Scripts/python.exe progress_reports/create_progress_reports.py \
  --input progress_reports/2026-03 \
  --send-email \
  --gmail-client-secret .secrets/gmail_client_secret.json \
  --gmail-token .secrets/gmail_token.json \
  --gmail-auth-mode console
```

## Safe sending workflow

- By default, generated HTML files are written to the dated folder's `reports/` subdirectory and no email is sent.
- Add `--send-email` only after you have local Gmail OAuth credentials available.
- In Codespaces or Colab, use `--gmail-auth-mode console` so the script prints an auth URL and accepts a pasted code.
- Without `--send-to-parents`, messages go to the preview recipient so you can proof them first.
- Add `--send-to-parents` only when you are ready for live delivery.
- Use `--max-emails` for a small proof batch before a full send.
- The default credential filenames are `gmail_client_secret.json` and `gmail_token.json`, but you can now override both paths explicitly.

## Outputs

- `--output`: optional override for the normalized aggregate CSV. By default it lands beside the source exports.
- `reports/`: one HTML report per Scout and parent pairing, written under the input folder.

If the script raises a mismatch about missing requirement rows, treat that as a data-quality warning from the underlying Scoutbook export and rerun the rank report after rechecking the filters.

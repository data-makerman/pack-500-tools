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
  --roster progress_reports/2026-03/RosterReport_Pack0500_Scouts_parents_20260315.csv \
  --output progress_reports/2026-03/progress_report_aggregate.csv \
  --reports-dir progress_reports/2026-03/reports \
  --report-date 2026-03-15
```

## Safe sending workflow

- By default, generated HTML files are written to `--reports-dir` and no email is sent.
- Add `--send-email` only after you have local Gmail OAuth credentials available.
- Without `--send-to-parents`, messages go to the preview recipient so you can proof them first.
- Add `--send-to-parents` only when you are ready for live delivery.
- Use `--max-emails` for a small proof batch before a full send.

## Outputs

- `--output`: normalized aggregate CSV with one record per advancement item.
- `--reports-dir`: one HTML report per Scout and parent pairing.

If the script raises a mismatch about missing requirement rows, treat that as a data-quality warning from the underlying Scoutbook export and rerun the rank report after rechecking the filters.

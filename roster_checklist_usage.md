# Roster checklist generator

`generate_roster_checklist.py` converts the `RosterReport_Pack0500_Checklist_20251201.csv` export into a printable HTML packet with one page per rank (Lions, Tigers, Wolves, Bears, Webelos, AOLs).

## Run it

```powershell
F:/Scouts/.venv/Scripts/python.exe generate_roster_checklist.py --input RosterReport_Pack0500_Checklist_20251201.csv --output roster_checklist.html
```

- `--input` defaults to the Scoutbook roster export in the repo root.
- `--output` defaults to `roster_checklist.html` alongside the script.

## Print it

1. Open the generated HTML file in a modern browser.
2. Use the browser print dialog, set paper size to Letter, orientation Portrait, and enable "Print backgrounds" for best results.
3. The CSS inserts automatic page breaks so each rank prints on its own sheet.

## Customizing

- Update `GRADE_TO_RANK` in the script if additional grade labels appear in future exports.
- Tweak the `SECTION_TEMPLATE` table headings or CSS to add/remove checklist columns.

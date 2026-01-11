"""Generate a printable roster checklist grouped by Cub Scout rank.

Usage:
    python generate_roster_checklist.py \
        --input RosterReport_Pack0500_Checklist_20251201.csv \
        --output roster_checklist.html

The resulting HTML file contains one printable page per rank with
check boxes and room for notes that den leaders can mark off.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List

GRADE_TO_RANK: Dict[str, str] = {
    "kindergarten": "Lions",
    "": "Lions",  # no grade provided
    "first grade": "Tigers",
    "second grade": "Wolves",
    "third grade": "Bears",
    "fourth grade": "Webelos",
    "fifth grade": "AOLs",
}

RANK_ORDER: List[str] = ["Lions", "Tigers", "Wolves", "Bears", "Webelos", "AOLs"]

COMPACT_ROW_THRESHOLD = 18
SUPER_COMPACT_ROW_THRESHOLD = 22

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\" />
<title>Pack 500 Roster Checklist</title>
<style>
:root {{
    --accent: #004c97;
    --light-accent: #e6f0fa;
    --border: #1f3b60;
    --text: #1b1b1b;
}}
* {{ box-sizing: border-box; }}
body {{
    font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    margin: 0;
    background: #f7f7f7;
    color: var(--text);
}}
.cover {{
    text-align: center;
    padding: 1.5rem 1rem 0.5rem;
}}
.cover h1 {{
    margin-bottom: 0.35rem;
    color: var(--accent);
}}
.cover p {{
    margin: 0.15rem 0;
    color: #4a4a4a;
}}
section.page {{
    background: white;
    margin: 0 auto 1rem;
    max-width: 8.5in;
    min-height: 10.5in;
    padding: 0.42in 0.65in 0.6in;
    box-shadow: 0 4px 18px rgba(0,0,0,0.08);
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    gap: 0.45rem;
    break-inside: avoid;
}}
section.page.compact {{
    padding: 0.3in 0.5in 0.35in;
    gap: 0.33rem;
}}
section.page.super-compact {{
    padding: 0.22in 0.4in 0.26in;
    gap: 0.25rem;
}}
section.page h1 {{
    margin: 0;
    font-size: 2rem;
    color: var(--accent);
}}
section.page.super-compact h1 {{
    font-size: 1.45rem;
}}
section.page .meta {{
    margin: 0.2rem 0 0;
    font-size: 0.9rem;
    color: #3a3a3a;
}}
section.page.compact .meta {{
    font-size: 0.88rem;
}}
section.page.super-compact .meta {{
    font-size: 0.78rem;
}}
.page-header {{
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    padding-bottom: 0.35rem;
    border-bottom: 2px solid var(--light-accent);
}}
table.roster {{
    border-collapse: collapse;
    width: 100%;
    font-size: 0.88rem;
    line-height: 1.08;
}}
table.roster thead {{
    background: var(--light-accent);
}}
table.roster th, table.roster td {{
    border: 1px solid var(--border);
    padding: 0.35rem 0.45rem;
    text-align: left;
}}
table.roster th.num, table.roster td.num {{
    width: 0.8in;
    text-align: center;
}}
table.roster th.checkbox, table.roster td.checkbox {{
    width: 1.2in;
    text-align: center;
}}
table.roster th.notes, table.roster td.notes {{
    width: 3.4in;
}}
section.page.compact table.roster {{
    font-size: 0.75rem;
}}
section.page.compact table.roster th, section.page.compact table.roster td {{
    padding: 0.16rem 0.24rem;
}}
section.page.compact table.roster th.notes, section.page.compact table.roster td.notes {{
    width: 2.8in;
}}
section.page.super-compact table.roster {{
    font-size: 0.66rem;
}}
section.page.super-compact table.roster th, section.page.super-compact table.roster td {{
    padding: 0.12rem 0.18rem;
}}
section.page.super-compact table.roster th.notes, section.page.super-compact table.roster td.notes {{
    width: 2.4in;
}}
span.box {{
    display: inline-block;
    width: 18px;
    height: 18px;
    border: 2px solid var(--border);
    border-radius: 4px;
}}
section.page.compact span.box {{
    width: 13px;
    height: 13px;
}}
section.page.super-compact span.box {{
    width: 10px;
    height: 10px;
}}
tr:nth-child(even) td {{
    background: rgba(0, 76, 151, 0.03);
}}
.footer-note {{
    font-size: 0.85rem;
    color: #666;
    margin-top: auto;
}}
@media print {{
    @page {{ size: Letter portrait; margin: 0.25in; }}
    body {{ background: white; }}
    section.page {{
        box-shadow: none;
        page-break-after: always;
        margin: 0;
        min-height: auto;
        height: auto;
        break-after: page;
    }}
    section.page:last-of-type {{
        page-break-after: auto;
    }}
}}
</style>
</head>
<body>
{intro}
{sections}
</body>
</html>
"""

SECTION_TEMPLATE = """
<section class=\"page{size_class}\">
    <header class=\"page-header\">
    <h1>{rank}</h1>
        <p class=\"meta\">Scouts: {count} &mdash; Dens: {dens}</p>
    </header>
  <table class=\"roster\">
    <thead>
      <tr>
    <th class=\"num\">#</th>
    <th>Scout Name</th>
    <th class=\"checkbox\">Pinewood Kit</th>
    <th class=\"notes\">Notes</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</section>
"""

ROW_TEMPLATE = """
<tr>
  <td class=\"num\">{index}</td>
  <td>{name}</td>
    <td class=\"checkbox\"><span class=\"box\"></span></td>
    <td class=\"notes\"></td>
</tr>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a printable roster checklist grouped by rank.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("RosterReport_Pack0500_Checklist_20251201.csv"),
        help="Path to the roster CSV exported from Scoutbook.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("roster_checklist.html"),
        help="Destination for the generated HTML checklist.",
    )
    return parser.parse_args()


def load_roster(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Cannot find roster file: {path}")

    raw_text = path.read_text(encoding="utf-8-sig")
    lines = raw_text.splitlines()
    if len(lines) < 2:
        raise ValueError("Roster file does not contain the expected header rows.")

    stream = StringIO("\n".join(lines[1:]))  # drop the first summary row
    reader = csv.DictReader(stream)
    cleaned_rows: List[Dict[str, str]] = []

    for row in reader:
        first = (row.get("First Name") or "").strip()
        last = (row.get("Last Name") or "").strip()
        grade = (row.get("Grade") or "").strip()
        den_raw = (row.get("Den") or "")
        den = " ".join(den_raw.split())

        if den.upper() == "99 LOST CUBS":
            continue

        if not first and not last:
            continue

        cleaned_rows.append(
            {
                "First Name": first,
                "Last Name": last,
                "Grade": grade,
                "Den": den,
                "Rank": grade_to_rank(grade),
            }
        )

    if not cleaned_rows:
        raise ValueError("No scout rows were parsed from the roster file.")

    return cleaned_rows


def grade_to_rank(raw_grade: str) -> str:
    grade_key = (raw_grade or "").strip().lower()
    rank = GRADE_TO_RANK.get(grade_key)
    if rank:
        return rank

    if grade_key in ("k", "kg", "kinder", "kindergarten/no grade"):
        return "Lions"

    raise ValueError(f"Unrecognized grade value: '{raw_grade}'")


def build_sections(rows: List[Dict[str, str]]) -> Iterable[str]:
    sections: List[str] = []
    for rank in RANK_ORDER:
        rank_rows = [r for r in rows if r["Rank"] == rank]
        if not rank_rows:
            continue

        rank_rows.sort(key=lambda item: (item["Last Name"], item["First Name"]))
        rendered_rows = []
        for idx, row in enumerate(rank_rows, start=1):
            name = f"{row['First Name']} {row['Last Name']}".strip()
            rendered_rows.append(
                ROW_TEMPLATE.format(
                    index=idx,
                    name=name,
                )
            )

        dens = sorted({r["Den"] for r in rank_rows if r.get("Den")})
        den_display = ", ".join(dens) if dens else "n/a"
        classes: List[str] = []
        if len(rank_rows) >= COMPACT_ROW_THRESHOLD:
            classes.append("compact")
        if len(rank_rows) >= SUPER_COMPACT_ROW_THRESHOLD:
            classes.append("super-compact")
        size_class = f" {' '.join(classes)}" if classes else ""

        sections.append(
            SECTION_TEMPLATE.format(
                rank=rank,
                count=len(rank_rows),
                dens=den_display,
                size_class=size_class,
                rows="\n".join(rendered_rows),
            )
        )
    return sections


def write_html(sections: Iterable[str], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generation_ts = datetime.now().strftime("Generated %B %d, %Y at %I:%M %p")
    intro = ("")
    filled = HTML_TEMPLATE.format(intro=intro, sections="\n".join(sections))
    output_path.write_text(filled, encoding="utf-8")


def main() -> None:
    args = parse_args()
    roster_rows = load_roster(args.input)
    sections = build_sections(roster_rows)
    if not sections:
        raise SystemExit("No rank data found to render. Check the roster file contents.")
    write_html(sections, args.output)
    print(f"Checklist saved to {args.output.resolve()}")


if __name__ == "__main__":
    main()

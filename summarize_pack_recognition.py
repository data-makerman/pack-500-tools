"""Summarize Pack recognition exports by rank for quick award planning."""

from __future__ import annotations

import argparse
import csv
from collections import OrderedDict
from pathlib import Path

ADVENTURE_TARGETS = OrderedDict(
    (
        ("Lion", 13),
        ("Tiger", 18),
        ("Wolf", 18),
        ("Bear", 18),
        ("Webelos", 18),
        ("AOL", 14),
    )
)


def parse_rank(den_label: str) -> str:
    label = (den_label or "").strip()
    for rank in ADVENTURE_TARGETS:
        if label.startswith(rank):
            return rank
    return label.split()[0] if label else "Unknown"


def summarize(csv_path: Path) -> list[tuple[str, int, int, int]]:
    totals: dict[str, dict[str, object]] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            rank = parse_rank(row.get("Den", ""))
            stats = totals.setdefault(
                rank,
                {
                    "adventures": 0,
                    "ranks": 0,
                    "scouts": {},
                },
            )
            scout = row.get("Name", "").strip() or "Unknown"
            rec = stats["scouts"].setdefault(
                scout,
                {
                    "adventures": 0,
                    "rank_earned": False,
                },
            )

            rec_type = (row.get("Type", "").strip().lower())
            if rec_type == "adventure":
                stats["adventures"] += 1
                rec["adventures"] += 1
            elif rec_type == "rank":
                stats["ranks"] += 1
                rec["rank_earned"] = True

    rows: list[tuple[str, int, int, int]] = []
    for rank in ADVENTURE_TARGETS:
        data = totals.get(rank)
        if not data:
            continue
        scouts = data["scouts"]
        threshold = ADVENTURE_TARGETS[rank]
        super_count = sum(
            1
            for scout_data in scouts.values()
            if scout_data["rank_earned"] and scout_data["adventures"] >= threshold
        )
        rows.append((rank, data["adventures"], data["ranks"], super_count))

    # Append any unexpected ranks so nothing is hidden.
    for rank, data in totals.items():
        if rank in ADVENTURE_TARGETS:
            continue
        scouts = data["scouts"]
        threshold = ADVENTURE_TARGETS.get(rank)
        super_count = 0
        if threshold:
            super_count = sum(
                1
                for scout_data in scouts.values()
                if scout_data["rank_earned"] and scout_data["adventures"] >= threshold
            )
        rows.append((rank, data["adventures"], data["ranks"], super_count))

    return rows


def format_table(rows: list[tuple[str, int, int, int]]) -> str:
    header = ("Rank", "Adventures", "Rank Awards", "Super Achievers")
    all_rows = [header] + rows
    widths = [max(len(str(row[idx])) for row in all_rows) for idx in range(len(header))]

    def render(row: tuple[str, int, int, int] | tuple[str, str, str, str]) -> str:
        return " | ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(row))

    divider = "-+-".join("-" * width for width in widths)
    output_lines = [render(header), divider]
    output_lines.extend(render(row) for row in rows)
    return "\n".join(output_lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize recognition data by rank.")
    parser.add_argument("csv_path", type=Path, help="Path to Pack Recognition CSV export")
    args = parser.parse_args()

    rows = summarize(args.csv_path)
    if not rows:
        print("No recognition data found in", args.csv_path)
        return

    print(format_table(rows))


if __name__ == "__main__":
    main()

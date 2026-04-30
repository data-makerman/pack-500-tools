#!/usr/bin/env python3
"""Scrape Cub Scout adventure requirement text from scouting.org."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup, Tag

BASE_API = "https://www.scouting.org/wp-json/wp/v2"
ADVENTURE_ENDPOINT = f"{BASE_API}/cs-adventure"
RANK_ENDPOINT = f"{BASE_API}/cs-adv-rank"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 30
PER_PAGE = 100


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_paginated(
    session: requests.Session,
    endpoint: str,
    *,
    params: Optional[Dict[str, object]] = None,
) -> Iterable[Dict[str, object]]:
    """Yield every entry from a paginated WordPress endpoint."""

    page = 1
    base_params = dict(params or {})
    while True:
        query = {"per_page": PER_PAGE, "page": page, **base_params}
        response = session.get(endpoint, params=query, timeout=TIMEOUT)
        if response.status_code == 400 and "rest_post_invalid_page_number" in response.text:
            break
        response.raise_for_status()
        payload = response.json()
        if not payload:
            break
        for record in payload:
            yield record
        if len(payload) < PER_PAGE:
            break
        page += 1


def fetch_rank_metadata(session: requests.Session) -> Dict[int, Dict[str, str]]:
    mapping: Dict[int, Dict[str, str]] = {}
    for record in fetch_paginated(session, RANK_ENDPOINT):
        mapping[int(record["id"])] = {
            "name": record.get("name", ""),
            "slug": record.get("slug", ""),
        }
    return mapping


def fetch_adventures(
    session: requests.Session,
    *,
    slugs: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, object]]:
    allowed = set(slugs or [])
    adventures: List[Dict[str, object]] = []
    for record in fetch_paginated(session, ADVENTURE_ENDPOINT):
        if allowed and record.get("slug") not in allowed:
            continue
        adventures.append(record)
        if limit and len(adventures) >= limit:
            break
    adventures.sort(key=lambda item: item.get("slug", ""))
    return adventures


def normalize_html_text(node: Tag) -> str:
    text = node.get_text("\n", strip=True)
    text = text.replace("\xa0", " ")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    cleaned = [" ".join(line.split()) for line in lines]
    if node.find_all("li"):
        return "\n".join(cleaned)
    return " ".join(cleaned)


def parse_requirements(html_text: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    sections = soup.select("section.adv-requirements")
    requirements: List[Dict[str, str]] = []
    for section in sections:
        classes = section.get("class", [])
        if any(cls.endswith("snapshot") for cls in classes):
            continue
        header = section.select_one("h2.elementor-heading-title")
        description = section.select_one(".adv-requirements-description")
        if not description:
            continue
        heading_text = header.get_text(" ", strip=True) if header else ""
        requirements.append(
            {
                "heading": heading_text,
                "text": normalize_html_text(description),
            }
        )
    return requirements


def scrape_adventure(
    session: requests.Session,
    adventure: Dict[str, object],
    rank_map: Dict[int, Dict[str, str]],
) -> Dict[str, object]:
    title_html = adventure.get("title", {}).get("rendered", "")
    title = BeautifulSoup(title_html, "html.parser").get_text(" ", strip=True)
    rank_ids = [int(rid) for rid in adventure.get("cs-adv-rank", [])]
    ranks = [
        {"id": rid, **rank_map.get(rid, {"name": "", "slug": ""})}
        for rid in rank_ids
    ]
    link = adventure.get("link")
    if not link:
        raise ValueError(f"Adventure {adventure.get('slug')} is missing a public link")
    response = session.get(link, timeout=TIMEOUT)
    response.raise_for_status()
    requirements = parse_requirements(response.text)
    return {
        "id": adventure.get("id"),
        "slug": adventure.get("slug"),
        "title": title,
        "link": link,
        "ranks": ranks,
        "requirement_count": len(requirements),
        "requirements": requirements,
    }


def build_payload(records: List[Dict[str, object]]) -> Dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "https://www.scouting.org/programs/cub-scouts/adventures/",
        "adventure_count": len(records),
        "adventures": records,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("adventure_requirements.json"),
        help="Destination JSON file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only scrape this many adventures (useful for testing)",
    )
    parser.add_argument(
        "--slug",
        action="append",
        dest="slugs",
        help="Restrict scraping to specific adventure slugs (repeatable)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    session = create_session()

    def log(message: str) -> None:
        print(message, file=sys.stderr)

    log("Loading rank metadata...")
    rank_map = fetch_rank_metadata(session)
    log(f"Found {len(rank_map)} ranks")

    log("Loading adventures...")
    adventures = fetch_adventures(session, slugs=args.slugs, limit=args.limit)
    if not adventures:
        log("No adventures matched the requested filters")
        return 1
    log(f"Scraping {len(adventures)} adventures...")

    scraped: List[Dict[str, object]] = []
    for idx, adventure in enumerate(adventures, start=1):
        slug = adventure.get("slug")
        log(f"[{idx}/{len(adventures)}] {slug}")
        scraped.append(scrape_adventure(session, adventure, rank_map))

    payload = build_payload(scraped)
    serialized = json.dumps(
        payload,
        indent=2,
        ensure_ascii=False,
    )
    args.output.write_text(serialized + "\n", encoding="utf-8")
    log(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

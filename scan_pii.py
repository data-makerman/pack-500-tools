#!/usr/bin/env python
"""Lightweight wrapper around Presidio analyzer/anonymizer for repo safety checks."""
from __future__ import annotations

import argparse
import json
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import List, Sequence

from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine

REPO_ROOT = Path(__file__).resolve().parent
MAX_BYTES = 5 * 1024 * 1024  # skip unusually large files
MIN_SCORE = 0.6
INTERESTING_TYPES = {"PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER"}
NLP_CONFIGURATION = {
    "nlp_engine_name": "spacy",
    "models": [
        {"lang_code": "en", "model_name": "en_core_web_lg"},
    ],
}
DEFAULT_IGNORE_FILE = REPO_ROOT / ".presidioignore"


def _build_analyzer() -> AnalyzerEngine:
    provider = NlpEngineProvider(nlp_configuration=NLP_CONFIGURATION)
    nlp_engine = provider.create_engine()
    return AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])


def _build_anonymizer() -> AnonymizerEngine:
    return AnonymizerEngine()


def _read_text(path: Path) -> str | None:
    try:
        sample = path.read_bytes()
    except OSError:  # File disappeared or unreadable
        return None
    if len(sample) > MAX_BYTES or b"\x00" in sample:
        return None
    return sample.decode("utf-8", errors="ignore")


def analyze_files(
    files: Sequence[Path],
    anonymized_root: Path | None = None,
    ignore_patterns: Sequence[str] | None = None,
) -> List[dict]:
    analyzer = _build_analyzer()
    anonymizer = _build_anonymizer()
    findings: List[dict] = []
    skip_patterns = list(ignore_patterns or [])

    for file_path in files:
        text = _read_text(file_path)
        if not text:
            continue
        results = analyzer.analyze(text=text, language="en")
        if not results:
            continue
        try:
            relative_path = str(file_path.relative_to(REPO_ROOT))
        except ValueError:
            relative_path = str(file_path)
        if _should_ignore(file_path, relative_path, skip_patterns):
            continue
        file_record = {
            "path": relative_path,
            "entities": [],
        }
        filtered = [m for m in results if _is_relevant_entity(m)]
        if not filtered:
            continue
        for match in filtered:
            file_record["entities"].append(
                {
                    "entity_type": match.entity_type,
                    "start": match.start,
                    "end": match.end,
                    "length": match.end - match.start,
                    "score": round(match.score or 0.0, 3),
                }
            )
        if anonymized_root:
            try:
                relative = file_path.relative_to(REPO_ROOT)
            except ValueError:
                relative = Path(file_path.name)
            destination = anonymized_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            anonymized_text = anonymizer.anonymize(
                text=text,
                analyzer_results=filtered,
            ).text
            destination.write_text(anonymized_text, encoding="utf-8")
            file_record["anonymized_copy"] = str(destination)
        findings.append(file_record)
    return findings


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan files for PII with Presidio")
    parser.add_argument("files", nargs="+", help="Files to scan")
    parser.add_argument(
        "--anonymized-dir",
        type=Path,
        help="Optional directory to mirror anonymized copies",
    )
    parser.add_argument(
        "--output-json",
        action="store_true",
        help="Print machine-readable findings JSON",
    )
    parser.add_argument(
        "--no-fail-on-findings",
        action="store_true",
        help="Return success even if PII is detected",
    )
    parser.add_argument(
        "--ignore-file",
        type=Path,
        default=DEFAULT_IGNORE_FILE,
        help="Optional newline-delimited glob list of files to skip",
    )
    return parser.parse_args(argv)


def _load_ignore_patterns(ignore_file: Path | None) -> list[str]:
    if not ignore_file:
        return []
    try:
        text = ignore_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    patterns = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def _should_ignore(file_path: Path, relative_path: str, patterns: Sequence[str]) -> bool:
    absolute = str(file_path)
    filename = file_path.name
    for pattern in patterns:
        if fnmatch(relative_path, pattern):
            return True
        if fnmatch(filename, pattern):
            return True
        if fnmatch(absolute, pattern):
            return True
    return False


def _is_relevant_entity(match: RecognizerResult) -> bool:
    score = match.score or 0.0
    if score < MIN_SCORE:
        return False
    return match.entity_type in INTERESTING_TYPES


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    files = [Path(name).resolve() for name in args.files]
    ignore_file = args.ignore_file
    if ignore_file and not ignore_file.is_absolute():
        ignore_file = (REPO_ROOT / ignore_file).resolve()
    ignore_patterns = _load_ignore_patterns(ignore_file)
    anonymized_root = args.anonymized_dir.resolve() if args.anonymized_dir else None
    if anonymized_root:
        anonymized_root.mkdir(parents=True, exist_ok=True)

    findings = analyze_files(files, anonymized_root, ignore_patterns)

    if args.output_json:
        json.dump(findings, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        for record in findings:
            path = record["path"]
            print(f"[PII] {path} ({len(record['entities'])} hits)")
            for entity in record["entities"]:
                print(
                    f"    - {entity['entity_type']} score={entity['score']} "
                    f"chars={entity['length']}"
                )
            if record.get("anonymized_copy"):
                print(f"      anonymized -> {record['anonymized_copy']}")
    if findings and not args.no_fail_on_findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Workflow to collect all German Volleyball Bundesliga statistics."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

from scrapy.crawler import CrawlerProcess

from ..spiders.competition import CompetitionMatchesSpider
from ..spiders.match import GuestStatsSpider, HomeStatsSpider


_VBL_COMPETITION_PHASES: Sequence[Dict[str, str]] = (
    {
        "label": "1. Bundesliga Männer - Hauptrunde",
        "fed_acronym": "vbl",
        "competition_id": "160",
        "competition_pid": "169",
    },
    {
        "label": "1. Bundesliga Männer - Zwischenrunde 1-4",
        "fed_acronym": "vbl",
        "competition_id": "160",
        "competition_pid": "170",
    },
    {
        "label": "1. Bundesliga Männer - Zwischenrunde 5-8",
        "fed_acronym": "vbl",
        "competition_id": "160",
        "competition_pid": "171",
    },
    {
        "label": "1. Bundesliga Männer - Playoffs",
        "fed_acronym": "vbl",
        "competition_id": "160",
        "competition_pid": "168",
    },
    {
        "label": "1. Bundesliga Frauen - Hauptrunde",
        "fed_acronym": "vbl",
        "competition_id": "162",
        "competition_pid": "173",
    },
    {
        "label": "1. Bundesliga Frauen - Playoffs",
        "fed_acronym": "vbl",
        "competition_id": "162",
        "competition_pid": "174",
    },
)

_DATA_DIRECTORY = Path("data")


def run_vbl_full_season_workflow(*, log: bool = False) -> None:
    """Collect competitions and match statistics for the Bundesliga."""

    _DATA_DIRECTORY.mkdir(exist_ok=True)

    unique_match_ids: List[str] = []
    seen_match_ids: Set[str] = set()

    for phase in _VBL_COMPETITION_PHASES:
        print(f"volleystats: collecting {phase['label']}")
        competition_file = _collect_competition_phase(phase, enable_log=log)
        phase_match_ids = _read_match_ids(competition_file)
        fresh_ids = [match_id for match_id in phase_match_ids if match_id not in seen_match_ids]
        if not fresh_ids:
            print("volleystats: no new matches found for this phase")
            continue

        unique_match_ids.extend(fresh_ids)
        seen_match_ids.update(fresh_ids)
        print(
            f"volleystats: added {len(fresh_ids)} matches (total unique: {len(unique_match_ids)})"
        )

    if not unique_match_ids:
        print("volleystats: no matches queued for scraping")
        return

    print("volleystats: scraping match statistics for queued matches")
    _collect_match_statistics(_VBL_COMPETITION_PHASES[0]["fed_acronym"], unique_match_ids, enable_log=log)
    print("volleystats: Bundesliga workflow finished")


def _collect_competition_phase(phase: Dict[str, str], *, enable_log: bool) -> Path:
    feeds_settings = {
        "FEEDS": {
            "data/%(fed_acronym)s-%(competition_id)s-%(competition_pid)s-%(name)s.csv": {
                "format": "csv",
                "overwrite": True,
            }
        },
        "LOG_ENABLED": enable_log,
    }

    competition_process = CrawlerProcess(settings=feeds_settings)

    competition_process.crawl(
        CompetitionMatchesSpider,
        fed_acronym=phase["fed_acronym"],
        competition_id=phase["competition_id"],
        competition_pid=phase["competition_pid"],
    )
    competition_process.start()

    return _resolve_competition_file(
        phase["fed_acronym"],
        phase["competition_id"],
        phase["competition_pid"],
    )


def _resolve_competition_file(fed_acronym: str, competition_id: str, competition_pid: str) -> Path:
    pid_prefix = f"{competition_pid}-" if competition_pid else ""
    renamed_pattern = f"{fed_acronym}-{competition_id}-{pid_prefix}*-competition-matches.csv"
    candidates = sorted(
        _DATA_DIRECTORY.glob(renamed_pattern),
        key=lambda candidate: candidate.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]

    fallback = _DATA_DIRECTORY / (
        f"{fed_acronym}-{competition_id}-{competition_pid}-competition_matches.csv"
    )
    if fallback.exists():
        return fallback

    raise FileNotFoundError(
        "volleystats: unable to locate competition matches file for "
        f"{fed_acronym} ID {competition_id} PID {competition_pid}"
    )


def _read_match_ids(csv_path: Path) -> List[str]:
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        return [row["Match ID"] for row in reader if row.get("Match ID")]


def _collect_match_statistics(fed_acronym: str, match_ids: Iterable[str], *, enable_log: bool) -> None:
    feeds_settings = {
        "FEEDS": {
            "data/%(fed_acronym)s-%(match_id)s-%(name)s.csv": {
                "format": "csv",
                "overwrite": True,
            }
        },
        "LOG_ENABLED": enable_log,
    }

    match_process = CrawlerProcess(settings=feeds_settings)

    for match_id in match_ids:
        print(f"volleystats: starting match {match_id}")
        match_process.crawl(HomeStatsSpider, fed_acronym=fed_acronym, match_id=match_id)
        match_process.crawl(GuestStatsSpider, fed_acronym=fed_acronym, match_id=match_id)

    match_process.start()


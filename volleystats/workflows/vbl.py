"""Workflow to collect all German Volleyball Bundesliga statistics."""

from __future__ import annotations

import csv
import importlib.resources as pkg_resources
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

import yaml
from scrapy.crawler import CrawlerProcess

from ..spiders.competition import CompetitionMatchesSpider
from ..spiders.match import GuestStatsSpider, HomeStatsSpider


_DATA_DIRECTORY = Path("data")
_PHASES_YAML = "vbl_phases.yml"
_REQUIRED_PHASE_KEYS = {"label", "fed_acronym", "competition_id", "competition_pid"}

_PROJECT_SETTINGS = Settings()
_PROJECT_SETTINGS.setmodule("volleystats.settings", priority="project")

install_reactor(_PROJECT_SETTINGS.get("TWISTED_REACTOR"))

from twisted.internet import defer, reactor  # noqa: E402  pylint: disable=wrong-import-position


def _create_runner(extra_settings: Dict[str, object]) -> CrawlerRunner:
    settings = _PROJECT_SETTINGS.copy()
    for key, value in extra_settings.items():
        settings.set(key, value)
    return CrawlerRunner(settings=settings)


def run_vbl_full_season_workflow(*, log: bool = False) -> None:
    """Collect competitions and match statistics for the Bundesliga."""

    phases = _load_competition_phases()

    _DATA_DIRECTORY.mkdir(exist_ok=True)

    unique_match_ids: List[str] = []
    seen_match_ids: Set[str] = set()

    for phase in phases:
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
        print("volleystats: Bundesliga workflow finished")

    workflow_deferred = _run_workflow()
    failure_holder: Dict[str, object] = {}

    def _remember_failure(failure):
        failure_holder["failure"] = failure
        return failure

    workflow_deferred.addErrback(_remember_failure)
    workflow_deferred.addBoth(lambda _: reactor.stop())
    reactor.run()

    if "failure" in failure_holder:
        failure = failure_holder["failure"]
        if hasattr(failure, "raiseException"):
            failure.raiseException()
        elif isinstance(failure, Exception):
            raise failure


@defer.inlineCallbacks
def _collect_competition_phase(phase: Dict[str, str], *, enable_log: bool):
    runner = _create_runner(
        {
            "FEEDS": {
                "data/%(fed_acronym)s-%(competition_id)s-%(competition_pid)s-%(name)s.csv": {
                    "format": "csv",
                    "overwrite": True,
                }
            },
            "LOG_ENABLED": enable_log,
        }
    )

    if not unique_match_ids:
        print("volleystats: no matches queued for scraping")
        return

    print("volleystats: scraping match statistics for queued matches")
    _collect_match_statistics(phases[0]["fed_acronym"], unique_match_ids, enable_log=log)
    print("volleystats: Bundesliga workflow finished")


@lru_cache()
def _load_competition_phases() -> Sequence[Dict[str, str]]:
    """Load the Bundesliga competition phases from the YAML descriptor."""

    with pkg_resources.files(__package__).joinpath(_PHASES_YAML).open(
        "r", encoding="utf-8"
    ) as yaml_file:
        payload = yaml.safe_load(yaml_file) or {}

    phases = payload.get("phases", [])
    if not isinstance(phases, list) or not phases:
        raise ValueError(
            "volleystats: Bundesliga workflow configuration is missing phases"
        )

    normalized_phases: List[Dict[str, str]] = []
    for index, phase in enumerate(phases, start=1):
        if not isinstance(phase, dict):
            raise TypeError(
                "volleystats: each phase in the Bundesliga workflow must be a mapping"
            )

        missing_keys = _REQUIRED_PHASE_KEYS - phase.keys()
        if missing_keys:
            raise KeyError(
                "volleystats: phase #{index} is missing required keys: {missing}".format(
                    index=index, missing=", ".join(sorted(missing_keys))
                )
            )

        normalized_phases.append(
            {
                key: str(phase[key]).strip()
                for key in _REQUIRED_PHASE_KEYS
            }
        )

    return tuple(normalized_phases)


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


@defer.inlineCallbacks
def _collect_match_statistics(
    fed_acronym: str, match_ids: Iterable[str], *, enable_log: bool
) -> defer.Deferred:
    runner = _create_runner(
        {
            "FEEDS": {
                "data/%(fed_acronym)s-%(match_id)s-%(name)s.csv": {
                    "format": "csv",
                    "overwrite": True,
                }
            },
            "LOG_ENABLED": enable_log,
        }
    )

    deferreds = []
    for match_id in match_ids:
        print(f"volleystats: starting match {match_id}")
        deferreds.append(
            runner.crawl(HomeStatsSpider, fed_acronym=fed_acronym, match_id=match_id)
        )
        deferreds.append(
            runner.crawl(GuestStatsSpider, fed_acronym=fed_acronym, match_id=match_id)
        )

    yield defer.DeferredList(deferreds, fireOnOneErrback=True)


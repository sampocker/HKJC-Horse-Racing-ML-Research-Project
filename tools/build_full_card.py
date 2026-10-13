import os
import re
import sys
import time
import subprocess
from datetime import date
from pathlib import Path

import httpx
import polars as pl

from hkjc.data.live.graphql import LiveClient
from hkjc.data.pipeline import scrape_horses

RACE_DATE = date(2026, 10, 1)
VENUE = "ST"

def season_for(ds):
    y, m, _ = map(int, ds.split("-"))
    return f"{y}-{str(y+1)[-2:]}" if m >= 9 else f"{y-1}-{str(y)[-2:]}"

def run_cli(*args):
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    subprocess.run(
        [sys.executable, "-m", "hkjc.cli", *args],
        env=env,
        check=True,
    )

print("\n=== 1. LOADING OCTOBER 1 SHA TIN CARD ===", flush=True)

with LiveClient() as c:
    card = c.card(RACE_DATE, VENUE)

races = card.races

all_runners = []
seen = set()

for race_no, race in enumerate(races, 1):
    for r in race.runners:
        if r.saddle is None or not r.horse_id:
            continue
        if r.horse_id not in seen:
            seen.add(r.horse_id)
            all_runners.append(r)

print("Races:", len(races))
print("Unique declared horses:", len(all_runners))

print("\n=== 2. SCRAPING HORSE FORM HISTORIES ===", flush=True)

result = scrape_horses(horse_ids=[r.horse_id for r in all_runners])
print(result)

print("\n=== 3. COLLECTING UNIQUE HISTORICAL MEETINGS ===", flush=True)

dates = set()

for r in all_runners:
    p = Path(f"data/raw/horse_form/{r.horse_id}.parquet")
    if not p.exists():
        print("WARNING: no horse form:", r.name_en, r.horse_id)
        continue

    df = pl.read_parquet(p)
    dates.update(str(d) for d in df["run_date"])

print("Unique historical dates:", len(dates))

if not dates:
    raise SystemExit("No historical dates found.")

earliest = min(dates)
latest = max(dates)

print("Earliest:", earliest)
print("Latest:", latest)

print("\n=== 4. SCRAPING MISSING HISTORICAL RESULTS ===", flush=True)

new_meetings = 0
stored_meetings = 0

for ds in sorted(dates):
    existing = list(
        Path("data/raw/results").rglob(f"date={ds}/results.parquet")
    )

    if existing:
        stored_meetings += 1
        continue

    print("SCRAPING", ds, flush=True)
    run_cli("scrape", "--date", ds)
    new_meetings += 1

print("New meetings:", new_meetings)
print("Already stored:", stored_meetings)

print("\n=== 5. SCRAPING MISSING SECTIONALS ===", flush=True)
run_cli("scrape-sectionals", "--since", earliest)

print("\n=== 6. SCRAPING MISSING COMMENTS ON RUNNING ===", flush=True)
run_cli("scrape-text", "--since", earliest)

print("\n=== 7. SCRAPING MISSING RACING INCIDENT REPORTS ===", flush=True)

incident_dir = Path("data/raw/race_incidents")
incident_dir.mkdir(parents=True, exist_ok=True)

saved_incidents = 0
existing_incidents = 0
failed_incidents = []

for ds in sorted(dates):
    path = incident_dir / f"{ds}.html"

    if path.exists():
        existing_incidents += 1
        continue

    url = (
        "https://racing.hkjc.com/en-us/local/information/"
        f"racereportfull?Date={ds.replace('-', '%2F')}"
    )

    success = False

    for attempt in range(3):
        try:
            response = httpx.get(
                url,
                follow_redirects=True,
                timeout=30,
            )
            response.raise_for_status()
            path.write_text(response.text, encoding="utf-8")
            print("SAVED INCIDENT REPORT", ds)
            saved_incidents += 1
            success = True
            break
        except Exception as e:
            print("Retrying incident report", ds, repr(e))
            time.sleep(2)

    if not success:
        failed_incidents.append(ds)

print("New incident reports:", saved_incidents)
print("Already stored:", existing_incidents)
print("Failed incident reports:", len(failed_incidents))

print("\n=== 8. BUILDING EVERY HORSE DOSSIER ===", flush=True)

for r in all_runners:
    print("BUILDING", r.name_en, flush=True)

    subprocess.run(
        [
            sys.executable,
            "tools/build_dossier.py",
            r.horse_id,
            r.name_en,
        ],
        check=True,
    )

print("\n=== 9. DATA QUALITY CHECK ===", flush=True)

missing_results = []
missing_sectionals = []
missing_comments = []

for r in all_runners:
    form_path = Path(f"data/raw/horse_form/{r.horse_id}.parquet")

    if not form_path.exists():
        continue

    form = pl.read_parquet(form_path)

    for f in form.iter_rows(named=True):
        if f["finish_pos_raw"] == "WV":
            continue

        ds = str(f["run_date"])
        venue = f["venue"]
        season = season_for(ds)

        base = f"season={season}/venue={venue}/date={ds}"

        result_path = Path(f"data/raw/results/{base}/results.parquet")
        sectional_path = Path(
            f"data/raw/sectionals/{base}/sectionals.parquet"
        )
        comment_path = Path(
            f"data/raw/comments_on_running/{base}/comments.parquet"
        )

        if not result_path.exists():
            missing_results.append((r.name_en, ds))
            continue

        rdf = pl.read_parquet(result_path).filter(
            pl.col("horse_id") == r.horse_id
        )

        if rdf.height == 0:
            missing_results.append((r.name_en, ds))

        if not sectional_path.exists():
            missing_sectionals.append((r.name_en, ds))
        else:
            sdf = pl.read_parquet(sectional_path).filter(
                pl.col("horse_id") == r.horse_id
            )
            if sdf.height == 0:
                missing_sectionals.append((r.name_en, ds))

        if not comment_path.exists():
            missing_comments.append((r.name_en, ds))
        else:
            cdf = pl.read_parquet(comment_path).filter(
                pl.col("horse_id") == r.horse_id
            )
            if cdf.height == 0:
                missing_comments.append((r.name_en, ds))

print("Missing actual-result rows:", len(missing_results))
print("Missing sectional records:", len(missing_sectionals))
print("Missing comment records:", len(missing_comments))
print("Failed incident pages:", len(failed_incidents))

print("\n=== 10. BUILDING RACE MASTER FILES ===", flush=True)

full_card = [
    "# SHA TIN — 2026-10-01 — FULL CARD",
    "",
    f"Races: {len(races)}",
    f"Unique declared horses: {len(all_runners)}",
    f"Historical dates represented: {len(dates)}",
    "",
    "## Data quality",
    "",
    f"- Missing actual-result rows: {len(missing_results)}",
    f"- Missing sectional records: {len(missing_sectionals)}",
    f"- Missing comment records: {len(missing_comments)}",
    f"- Failed Racing Incident pages: {len(failed_incidents)}",
    "",
    "---",
    "",
]

for race_no, race in enumerate(races, 1):

    runners = [r for r in race.runners if r.saddle is not None]

    race_out = [
        f"# SHA TIN — 2026-10-01 — RACE {race_no}",
        "",
        "## Current HKJC race-card data",
        "",
        "```json",
        race.model_dump_json(indent=2),
        "```",
        "",
        "## Declared runners",
        "",
    ]

    for r in runners:
        race_out.append(
            f"{r.saddle}. {r.name_en} ({r.horse_code})"
        )

    race_out += ["", "---", ""]

    for r in runners:
        safe = re.sub(
            r"[^A-Za-z0-9]+", "_", r.name_en
        ).strip("_")

        dossier = Path(
            f"data/{safe}_{r.horse_code}_dossier.md"
        )

        race_out += [
            f"# HORSE {r.saddle}",
            "",
        ]

        if dossier.exists():
            race_out.append(
                dossier.read_text(encoding="utf-8")
            )
        else:
            race_out.append(
                f"NO DOSSIER FOUND FOR {r.name_en}"
            )

        race_out += ["", "---", ""]

    race_file = Path(
        f"data/RACE{race_no}_2026-10-01_MASTER.md"
    )

    race_file.write_text(
        "\n".join(race_out),
        encoding="utf-8",
    )

    print(
        "Created:",
        race_file,
        race_file.stat().st_size,
        "bytes",
    )

    full_card += race_out
    full_card += ["", "================================", ""]

full_file = Path("data/FULL_CARD_2026-10-01_MASTER.md")

full_file.write_text(
    "\n".join(full_card),
    encoding="utf-8",
)

print("\n================================")
print("FULL CARD COMPLETE")
print("Created:", full_file)
print("File size:", full_file.stat().st_size, "bytes")
print("Races:", len(races))
print("Unique horses:", len(all_runners))
print("Historical dates:", len(dates))
print("Missing actual results:", len(missing_results))
print("Missing sectionals:", len(missing_sectionals))
print("Missing comments:", len(missing_comments))
print("Failed incident pages:", len(failed_incidents))
print("================================")

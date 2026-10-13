import sys
import re
from pathlib import Path

import polars as pl
from selectolax.parser import HTMLParser

if len(sys.argv) < 3:
    raise SystemExit('Usage: python tools/build_dossier.py HORSE_ID "HORSE NAME"')

HORSE_ID = sys.argv[1]
HORSE_NAME = sys.argv[2]
HORSE_CODE = HORSE_ID.split("_")[-1]

def season_for(d):
    y = d.year
    return f"{y}-{str(y+1)[-2:]}" if d.month >= 9 else f"{y-1}-{str(y)[-2:]}"

form_path = Path(f"data/raw/horse_form/{HORSE_ID}.parquet")

if not form_path.exists():
    raise SystemExit(f"Missing horse-form file: {form_path}")

form = pl.read_parquet(form_path).sort("run_date", descending=True)

actual_starts = form.filter(pl.col("finish_pos_raw") != "WV").height
withdrawals = form.filter(pl.col("finish_pos_raw") == "WV").height

out = [
    f"# {HORSE_NAME} ({HORSE_CODE})",
    "",
    f"Actual starts: {actual_starts}",
    f"Withdrawals: {withdrawals}",
    "",
]

written = 0

for f in form.iter_rows(named=True):
    ds = str(f["run_date"])
    venue = f["venue"]
    season = season_for(f["run_date"])
    base = f"season={season}/venue={venue}/date={ds}"

    if f["finish_pos_raw"] == "WV":
        print("Skipping withdrawal:", ds)
        continue

    result_path = Path(f"data/raw/results/{base}/results.parquet")
    race_path = Path(f"data/raw/races/{base}/races.parquet")

    if not result_path.exists() or not race_path.exists():
        print("Skipping missing meeting:", ds, venue)
        continue

    results = pl.read_parquet(result_path).filter(pl.col("horse_id") == HORSE_ID)
    if results.height == 0:
        print("Skipping missing horse result:", ds)
        continue

    result = results.row(0, named=True)
    race_no = result["race_no"]

    races = pl.read_parquet(race_path).filter(pl.col("race_no") == race_no)
    if races.height == 0:
        print("Skipping missing race:", ds, race_no)
        continue

    race = races.row(0, named=True)

    sec_path = Path(f"data/raw/sectionals/{base}/sectionals.parquet")
    sections = None
    if sec_path.exists():
        sections = (
            pl.read_parquet(sec_path)
            .filter(pl.col("horse_id") == HORSE_ID)
            .sort("section_index")
        )

    comment = ""
    comment_path = Path(f"data/raw/comments_on_running/{base}/comments.parquet")
    if comment_path.exists():
        comments = pl.read_parquet(comment_path).filter(
            pl.col("horse_id") == HORSE_ID
        )
        if comments.height:
            comment = comments.row(0, named=True)["comment"]

    incident = ""
    incident_path = Path(f"data/raw/race_incidents/{ds}.html")
    if incident_path.exists():
        tree = HTMLParser(incident_path.read_text(encoding="utf-8"))
        for tr in tree.css("tr"):
            text = " ".join(tr.text(separator=" ").split())
            if HORSE_NAME in text and f"({HORSE_CODE})" in text:
                incident = text
                break

    written += 1

    out += [
        f"## {written}. {ds} — {venue} Race {race_no}",
        "",
        f"- Race index: {race['race_index']}",
        f"- Race: {race['race_name'] or 'Unknown'}",
        f"- Class: {race['race_class']}",
        f"- Rating band: {race['rating_band']}",
        f"- Distance: {race['distance_m']}m",
        f"- Surface: {race['surface']}",
        f"- Course: {race['course']}",
        f"- Going: {race['going']}",
        f"- Draw: {result['draw']}",
        f"- Rating: {f['rating']}",
        f"- Jockey: {result['jockey_name']}",
        f"- Trainer: {result['trainer_name']}",
        f"- Carried weight: {result['actual_weight']} lb",
        f"- Declared body weight: {result['declared_weight']} lb",
        f"- Gear: {f['gear']}",
        f"- Finish: {result['finish_pos']}",
        f"- Beaten margin: {result['lbw_raw']}",
        f"- Running positions: {result['running_position_raw']}",
        f"- Horse time: {result['finish_time_s']} sec",
        f"- Winner time: {race['final_time_s']} sec",
        f"- Win odds: {result['win_odds']}",
        "",
        "### Sectionals",
    ]

    if sections is not None and sections.height:
        for s in sections.iter_rows(named=True):
            out.append(
                f"- Section {s['section_index']}: "
                f"position {s['running_position']}, "
                f"margin {s['margin_raw']}, "
                f"section time {s['section_time_s']} sec, "
                f"200m split {s['split_200m_s']}"
            )
    else:
        out.append("- No sectional data found.")

    out += [
        "",
        "### Official comment on running",
        comment or "No comment found.",
        "",
        "### Racing Incidents",
        incident or "No individual incident entry found.",
        "",
        "---",
        "",
    ]

safe_name = re.sub(r"[^A-Za-z0-9]+", "_", HORSE_NAME).strip("_")
dest = Path(f"data/{safe_name}_{HORSE_CODE}_dossier.md")
dest.write_text("\n".join(out), encoding="utf-8")

print("Created:", dest)
print("Past performances written:", written)
print("File size:", dest.stat().st_size, "bytes")

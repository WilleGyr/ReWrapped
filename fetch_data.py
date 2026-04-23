#!/usr/bin/env python3
"""
ReWrapped — pulls listening history from Google Sheets, enriches it with
Spotify API data (durations, album art, artist images), and writes data.json
for the dashboard.

Usage:
    cp .env.example .env   # fill in your credentials
    pip install requests python-dotenv
    python fetch_data.py
"""

import csv
import io
import json
import os
import sys
import time
import base64
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

def _require(key):
    val = os.getenv(key)
    if not val:
        print(f"Error: {key} is not set. Copy .env.example to .env and fill it in.")
        sys.exit(1)
    return val

# ── Config ─────────────────────────────────────────────────────────────────────
SHEET_IDS   = [s.strip() for s in _require("SHEET_IDS").split(",")]
CLIENT_ID     = _require("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = _require("SPOTIFY_CLIENT_SECRET")
SHEET_TAB   = "Blad1"
TARGET_YEAR = 2026
OUT_FILE    = "data.json"


# ── Spotify helpers ─────────────────────────────────────────────────────────────
def get_token():
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {creds}"},
        data={"grant_type": "client_credentials"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def sp(token, path, params=None):
    """GET from Spotify API with automatic rate-limit retry."""
    while True:
        r = requests.get(
            f"https://api.spotify.com/v1/{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=20,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2))
            print(f"  Rate-limited — sleeping {wait}s…", file=sys.stderr)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()


def batches(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ── Google Sheets helpers ───────────────────────────────────────────────────────
def fetch_sheet_csv(sheet_id):
    # export?format=csv returns the full sheet without the gviz row cap (~500 rows)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return list(csv.reader(io.StringIO(r.text)))


def parse_ts(s):
    """Parse timestamps in various formats Google Sheets may export."""
    s = s.strip()
    formats = [
        "%B %d, %Y at %I:%M%p",    # March 1, 2026 at 02:04PM
        "%B %d, %Y at %I:%M %p",   # March 1, 2026 at 02:04 PM
        "%B %-d, %Y at %I:%M%p",   # March 1, 2026 at 02:04PM (no zero-pad, Unix)
        "%Y-%m-%d %H:%M:%S",        # 2026-03-01 14:04:00
        "%Y-%m-%dT%H:%M:%S",        # ISO
        "%m/%d/%Y %H:%M:%S",        # 03/01/2026 14:04:00
        "%d/%m/%Y %H:%M:%S",        # 01/03/2026 14:04:00
        "%B %d, %Y",                # March 1, 2026 (no time)
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ── Main ────────────────────────────────────────────────────────────────────────
def main():
    # Load all plays from every sheet
    plays = []
    for sid in SHEET_IDS:
        print(f"Fetching sheet {sid[:28]}…")
        rows = fetch_sheet_csv(sid)
        print(f"  {len(rows)} rows in CSV")

        # Debug: show first 3 raw rows so we can spot format issues
        for i, row in enumerate(rows[:3]):
            print(f"  row[{i}] col[0] = {repr(row[0]) if row else '(empty)'}")

        parsed = skipped_year = skipped_parse = 0
        for row in rows:
            if len(row) < 4:
                continue
            ts = parse_ts(row[0])
            if ts is None:
                skipped_parse += 1
                continue
            if ts.year != TARGET_YEAR:
                skipped_year += 1
                continue
            parsed += 1
            plays.append({
                "ts":     ts,
                "track":  row[1].strip(),
                "artist": row[2].strip(),
                "tid":    row[3].strip(),
                "url":    row[4].strip() if len(row) > 4 else "",
            })
        print(f"  → {parsed} kept, {skipped_parse} unparseable, {skipped_year} wrong year")

    plays.sort(key=lambda p: p["ts"])
    print(f"  {len(plays):,} plays found in {TARGET_YEAR}")

    if not plays:
        print("No data — aborting.")
        sys.exit(1)

    # Spotify enrichment
    print("Getting Spotify token…")
    token = get_token()

    track_ids = [t for t in {p["tid"] for p in plays} if t]
    print(f"Fetching {len(track_ids):,} unique tracks…")
    tdata: dict = {}
    for chunk in batches(track_ids, 50):
        for t in sp(token, "tracks", {"ids": ",".join(chunk)}).get("tracks", []):
            if t:
                tdata[t["id"]] = {
                    "duration_ms": t["duration_ms"],
                    "album_art":   t["album"]["images"][0]["url"] if t["album"]["images"] else None,
                    "album_name":  t["album"]["name"],
                    "artist_ids":  [a["id"] for a in t["artists"]],
                }

    all_artist_ids = list({aid for td in tdata.values() for aid in td["artist_ids"]})
    print(f"Fetching {len(all_artist_ids):,} unique artists…")
    adata: dict = {}
    for chunk in batches(all_artist_ids, 50):
        for a in sp(token, "artists", {"ids": ",".join(chunk)}).get("artists", []):
            if a:
                adata[a["id"]] = {
                    "name":        a["name"],
                    "image_url":   a["images"][0]["url"] if a["images"] else None,
                    "genres":      a.get("genres", []),
                    "spotify_url": a["external_urls"].get("spotify", ""),
                }

    name_to_aid = {v["name"].lower(): k for k, v in adata.items()}

    # Compute stats
    track_plays:  Counter      = Counter()
    artist_plays: Counter      = Counter()
    hour_plays:   Counter      = Counter()
    dow_plays:    Counter      = Counter()
    month_plays:  Counter      = Counter()
    day_plays:    Counter      = Counter()
    month_ms:     defaultdict  = defaultdict(int)
    total_ms = 0

    for p in plays:
        key = (p["track"], p["artist"], p["tid"])
        track_plays[key]  += 1
        artist_plays[p["artist"]] += 1
        hour_plays[p["ts"].hour]  += 1
        dow_plays[p["ts"].weekday()] += 1
        month_plays[p["ts"].month]   += 1
        day_plays[p["ts"].date()]    += 1
        ms = tdata.get(p["tid"], {}).get("duration_ms", 0)
        total_ms  += ms
        month_ms[p["ts"].month] += ms

    total_min = total_ms // 60_000
    most_active_day = max(day_plays, key=day_plays.get)

    # Weekly buckets
    week_plays: Counter = Counter()
    for p in plays:
        iso = p["ts"].isocalendar()
        week_plays[f"{iso[0]}-W{iso[1]:02d}"] += 1

    # Top tracks (top 20)
    top_tracks = []
    for (name, artist, tid), cnt in track_plays.most_common(20):
        td = tdata.get(tid, {})
        top_tracks.append({
            "name":        name,
            "artist":      artist,
            "track_id":    tid,
            "plays":       cnt,
            "duration_ms": td.get("duration_ms", 0),
            "album_art":   td.get("album_art"),
            "album_name":  td.get("album_name", ""),
            "spotify_url": f"https://open.spotify.com/track/{tid}",
        })

    # Top artists (top 20)
    top_artists = []
    for artist_name, cnt in artist_plays.most_common(20):
        aid = name_to_aid.get(artist_name.lower())
        ad  = adata.get(aid, {}) if aid else {}
        top_artists.append({
            "name":        artist_name,
            "plays":       cnt,
            "image_url":   ad.get("image_url"),
            "genres":      ad.get("genres", []),
            "spotify_url": ad.get("spotify_url", ""),
        })

    # Genre counts (weighted by artist plays)
    genre_counts: Counter = Counter()
    for artist_name, cnt in artist_plays.most_common(100):
        aid = name_to_aid.get(artist_name.lower())
        if aid and aid in adata:
            for g in adata[aid]["genres"]:
                genre_counts[g] += cnt
    top_genres = [{"genre": g, "count": c} for g, c in genre_counts.most_common(15)]

    # Monthly breakdown
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    by_month = [
        {
            "month":     month_names[m - 1],
            "month_num": m,
            "plays":     month_plays[m],
            "minutes":   month_ms[m] // 60_000,
        }
        for m in range(1, 13)
        if month_plays[m] > 0
    ]

    day_of_week_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

    first_p, last_p = plays[0], plays[-1]

    # Day series (for potential future sparkline use)
    day_series = [
        {"date": d.isoformat(), "plays": c}
        for d, c in sorted(day_plays.items())
    ]

    output = {
        "generated_at": datetime.now().isoformat(),
        "year": TARGET_YEAR,
        "summary": {
            "total_plays":         len(plays),
            "total_minutes":       total_min,
            "total_hours":         total_min // 60,
            "unique_tracks":       len(track_plays),
            "unique_artists":      len(artist_plays),
            "most_active_day":     most_active_day.isoformat(),
            "most_active_day_plays": day_plays[most_active_day],
            "avg_plays_per_day":   round(len(plays) / max(len(day_plays), 1), 1),
            "first_play": {
                "track":     first_p["track"],
                "artist":    first_p["artist"],
                "timestamp": first_p["ts"].isoformat(),
                "album_art": tdata.get(first_p["tid"], {}).get("album_art"),
            },
            "last_play": {
                "track":     last_p["track"],
                "artist":    last_p["artist"],
                "timestamp": last_p["ts"].isoformat(),
                "album_art": tdata.get(last_p["tid"], {}).get("album_art"),
            },
        },
        "top_tracks":  top_tracks,
        "top_artists": top_artists,
        "top_genres":  top_genres,
        "by_month":    by_month,
        "by_hour":     [hour_plays[h] for h in range(24)],
        "by_dow": [
            {"day": day_of_week_names[d], "plays": dow_plays[d]}
            for d in range(7)
        ],
        "by_week":    [{"week": k, "plays": v} for k, v in sorted(week_plays.items())],
        "day_series": day_series,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nSaved → {OUT_FILE}")
    print(f"  Plays   : {len(plays):,}")
    print(f"  Time    : {total_min // 60}h {total_min % 60}m")
    print(f"  Tracks  : {len(track_plays):,} unique")
    print(f"  Artists : {len(artist_plays):,} unique")
    print(f"\nOpen index.html in a browser (via a local server) to see your stats.")


if __name__ == "__main__":
    main()

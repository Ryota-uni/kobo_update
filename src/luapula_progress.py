import os
import sys
import datetime as dt
from typing import Dict, Any, List, Optional

import pandas as pd
import requests


# ---- Settings from env ----
KOBO_BASE_URL = os.getenv("KOBO_BASE_URL", "").rstrip("/")
KOBO_TOKEN = os.getenv("KOBO_TOKEN", "")
KOBO_ASSET_UID = os.getenv("KOBO_ASSET_UID", "")

# Kobo fields
FIELD_CAMP = "sec0_camp"
FIELD_FARMER_ID = "sec0_farmerid"

# Paths
TARGET_CSV_PATH = os.getenv("TARGET_CSV_PATH", "data/luapula_camps.csv")
OUT_DIR = os.getenv("OUT_DIR", "docs")
OUT_HTML = os.path.join(OUT_DIR, "index.html")
OUT_CSV = os.path.join(OUT_DIR, "progress.csv")


def normalize_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def kobo_headers() -> Dict[str, str]:
    if not KOBO_TOKEN:
        raise RuntimeError("KOBO_TOKEN is empty. Set it as env var or GitHub Secret.")
    return {"Authorization": f"Token {KOBO_TOKEN}", "Accept": "application/json"}


def fetch_kobo_submissions() -> List[Dict[str, Any]]:
    """
    Fetch all submissions from Kobo:
      GET /api/v2/assets/{asset_uid}/data/?format=json
    Handles common pagination: {results:[...], next:"..."}
    """
    if not KOBO_BASE_URL:
        raise RuntimeError("KOBO_BASE_URL is empty.")
    if not KOBO_ASSET_UID:
        raise RuntimeError("KOBO_ASSET_UID is empty.")

    url = f"{KOBO_BASE_URL}/api/v2/assets/{KOBO_ASSET_UID}/data/"
    params = {"format": "json"}

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(url, headers=kobo_headers(), params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Kobo API error {r.status_code}: {r.text[:500]}")

        payload = r.json()

        if isinstance(payload, dict) and isinstance(payload.get("results"), list):
            out.extend(payload["results"])
            next_url = payload.get("next")
            if next_url:
                url = next_url
                params = None  # next already has query params
                continue
            break

        if isinstance(payload, list):
            out.extend(payload)
            break

        raise RuntimeError(f"Unexpected Kobo response format: keys={list(payload.keys()) if isinstance(payload, dict) else type(payload)}")

    return out


def build_progress_table(submissions: List[Dict[str, Any]], target_df: pd.DataFrame) -> pd.DataFrame:
    """
    camp別に farmerid のユニーク数を集計し、targetと突合。
    """
    # target_df: columns = sec0_camp, target_n
    target_df = target_df.copy()
    target_df["sec0_camp"] = target_df["sec0_camp"].map(normalize_str)
    target_df = target_df.dropna(subset=["sec0_camp"])
    target_df["target_n"] = target_df["target_n"].astype(int)

    if len(submissions) == 0:
        df = target_df.copy()
        df["collected_n"] = 0
        df["missing_farmerid_rows"] = 0
        df["missing_camp_rows"] = 0
    else:
        raw = pd.DataFrame(submissions)

        # Ensure columns exist
        for col in [FIELD_CAMP, FIELD_FARMER_ID]:
            if col not in raw.columns:
                raw[col] = None

        raw[FIELD_CAMP] = raw[FIELD_CAMP].map(normalize_str)
        raw[FIELD_FARMER_ID] = raw[FIELD_FARMER_ID].map(normalize_str)

        missing_camp_rows = int(raw[FIELD_CAMP].isna().sum())
        missing_farmerid_rows = int(raw[FIELD_FARMER_ID].isna().sum())

        # Count unique farmerid within camp
        valid = raw.dropna(subset=[FIELD_CAMP, FIELD_FARMER_ID]).copy()
        collected = (
            valid.groupby(FIELD_CAMP)[FIELD_FARMER_ID]
            .nunique()
            .reset_index()
            .rename(columns={FIELD_CAMP: "sec0_camp", FIELD_FARMER_ID: "collected_n"})
        )

        df = target_df.merge(collected, on="sec0_camp", how="left")
        df["collected_n"] = df["collected_n"].fillna(0).astype(int)

        df["missing_farmerid_rows"] = missing_farmerid_rows
        df["missing_camp_rows"] = missing_camp_rows

    df["remaining_n"] = (df["target_n"] - df["collected_n"]).clip(lower=0).astype(int)
    df["progress_pct"] = (100 * df["collected_n"] / df["target_n"]).fillna(0.0)

    # sort: progress low first
    df = df.sort_values(["progress_pct", "remaining_n"], ascending=[True, False]).reset_index(drop=True)
    return df


def render_html(df: pd.DataFrame, updated_at: str) -> str:
    # Simple, self-contained HTML (no JS)
    rows = []
    for _, r in df.iterrows():
        camp = r["sec0_camp"]
        target_n = int(r["target_n"])
        collected_n = int(r["collected_n"])
        remaining_n = int(r["remaining_n"])
        pct = float(r["progress_pct"])
        bar = max(0.0, min(100.0, pct))
        status_class = "behind" if remaining_n > 0 else "done"

        rows.append(f"""
        <tr class="{status_class}">
          <td class="camp">{camp}</td>
          <td class="num">{target_n}</td>
          <td class="num">{collected_n}</td>
          <td class="num">{remaining_n}</td>
          <td class="progress">
            <div class="bar-bg"><div class="bar" style="width:{bar:.1f}%"></div></div>
            <div class="pct">{pct:.1f}%</div>
          </td>
        </tr>
        """)

    miss_farmer = int(df["missing_farmerid_rows"].iloc[0]) if "missing_farmerid_rows" in df.columns else 0
    miss_camp = int(df["missing_camp_rows"].iloc[0]) if "missing_camp_rows" in df.columns else 0

    return f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Zambia Survey Progress</title>
<style>
  :root {{
    --fg:#111; --muted:#666; --bg:#fff; --line:#e6e6e6;
    --bar:#111; --barbg:#f2f2f2;
    --behind:#fff7ed; --done:#f6ffed;
    --shadow: 0 8px 24px rgba(0,0,0,.06);
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Noto Sans JP", sans-serif;
  }}
  body {{ margin:0; color:var(--fg); background:#fafafa; }}
  .wrap {{ max-width: 1100px; margin: 32px auto; padding: 0 16px; }}
  header {{ display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin-bottom: 12px; }}
  h1 {{ font-size: 20px; margin:0; }}
  .meta {{ color: var(--muted); font-size: 13px; white-space: nowrap; }}
  .card {{ background: var(--bg); border: 1px solid var(--line); border-radius: 16px; box-shadow: var(--shadow); overflow:hidden; }}
  table {{ width:100%; border-collapse: collapse; }}
  thead th {{ text-align:left; font-size: 13px; color: var(--muted); padding: 12px 14px; border-bottom:1px solid var(--line); background:#fcfcfc; position:sticky; top:0; }}
  tbody td {{ padding: 12px 14px; border-bottom: 1px solid var(--line); font-size: 14px; }}
  tbody tr.behind {{ background: var(--behind); }}
  tbody tr.done {{ background: var(--done); }}
  td.num {{ text-align:right; font-variant-numeric: tabular-nums; width: 90px; }}
  td.camp {{ font-weight: 600; }}
  td.progress {{ width: 240px; }}
  .bar-bg {{ height: 10px; background: var(--barbg); border-radius: 999px; overflow:hidden; border:1px solid #eaeaea; }}
  .bar {{ height:100%; background: var(--bar); }}
  .pct {{ font-size: 12px; color: var(--muted); margin-top: 6px; text-align:right; font-variant-numeric: tabular-nums; }}
  .footer {{ display:flex; justify-content:space-between; align-items:center; gap:12px; padding: 12px 14px; color: var(--muted); font-size: 13px; }}
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <h1>Zambia Survey Progress（camp別）</h1>
      <div class="meta">Last updated: {updated_at}</div>
    </header>

    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Camp</th>
            <th style="text-align:right;">Target</th>
            <th style="text-align:right;">Collected（unique sec0_farmerid）</th>
            <th style="text-align:right;">Remaining</th>
            <th>Progress</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
      <div class="footer">
        <div>
          missing camp rows: <b>{miss_camp}</b> /
          missing farmerid rows: <b>{miss_farmer}</b>
        </div>
        <div><a href="./progress.csv">CSV</a></div>
      </div>
    </div>
  </div>
</body>
</html>
"""


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    if not os.path.exists(TARGET_CSV_PATH):
        raise FileNotFoundError(f"Target CSV not found: {TARGET_CSV_PATH}")

    target_df = pd.read_csv(TARGET_CSV_PATH, dtype={"sec0_camp": str})

    if "sec0_camp" not in target_df.columns or "target_n" not in target_df.columns:
        raise ValueError("data/target_camps.csv must have columns: sec0_camp,target_n")

    submissions = fetch_kobo_submissions()
    df = build_progress_table(submissions, target_df)

    df.to_csv(OUT_CSV, index=False)

    updated_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html = render_html(df, updated_at)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print("Wrote:", OUT_HTML)
    print("Wrote:", OUT_CSV)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
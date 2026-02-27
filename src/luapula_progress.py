import os
import sys
import datetime as dt
from typing import Dict, Any, List, Optional

import pandas as pd
import requests


# ---- env ----
KOBO_BASE_URL = os.getenv("KOBO_BASE_URL", "").rstrip("/")
KOBO_TOKEN = os.getenv("KOBO_TOKEN", "")
KOBO_ASSET_UID = os.getenv("LUAPULA_ASSET_UID", "")

# ---- Kobo actual column names (confirmed) ----
FIELD_CAMP_CODE = "section0/sec0_camp"
FIELD_FARMER_ID = "section0/sec0_farmerid"

# ---- camp code -> label (district ignored) ----
CAMP_LABEL_MAP = {
    "1": "Mabumba",
    "2": "Monga",
    "3": "Lukwesa",
    "4": "Chibondo",
    "5": "Katuta",
    "6": "Kabalange",
    "7": "Lusambo",
    "8": "Luena",
    "9": "Kanengo",
}

# ---- paths ----
TARGET_CSV_PATH = os.getenv("TARGET_CSV_PATH", "data/luapula_camps.csv")
OUT_DIR = os.getenv("OUT_DIR", "docs")
OUT_HTML = os.path.join(OUT_DIR, "luapula.html")
OUT_CSV  = os.path.join(OUT_DIR, "luapula_progress.csv")


def normalize_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def headers() -> Dict[str, str]:
    if not KOBO_TOKEN:
        raise RuntimeError("KOBO_TOKEN is empty. Set GitHub Secret KOBO_TOKEN.")
    return {"Authorization": f"Token {KOBO_TOKEN}", "Accept": "application/json"}


def fetch_all_submissions() -> List[Dict[str, Any]]:
    if not KOBO_BASE_URL:
        raise RuntimeError("KOBO_BASE_URL is empty.")
    if not KOBO_ASSET_UID:
        raise RuntimeError("KOBO_ASSET_UID is empty.")

    url = f"{KOBO_BASE_URL}/api/v2/assets/{KOBO_ASSET_UID}/data/"
    params = {"format": "json"}

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(url, headers=headers(), params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Kobo API error {r.status_code}: {r.text[:300]}")

        payload = r.json()

        if isinstance(payload, dict) and isinstance(payload.get("results"), list):
            out.extend(payload["results"])
            nxt = payload.get("next")
            if nxt:
                url = nxt
                params = None
                continue
            break

        if isinstance(payload, list):
            out.extend(payload)
            break

        raise RuntimeError("Unexpected Kobo response format.")

    return out


def build_progress(submissions: List[Dict[str, Any]], target_df: pd.DataFrame) -> pd.DataFrame:
    # normalize targets
    target_df = target_df.copy()
    target_df["camp_label"] = target_df["camp_label"].map(normalize_str)
    target_df = target_df.dropna(subset=["camp_label"])
    target_df["target_n"] = target_df["target_n"].astype(int)

    raw = pd.json_normalize(submissions, sep="/") if submissions else pd.DataFrame()

    # ensure columns exist
    for col in [FIELD_CAMP_CODE, FIELD_FARMER_ID]:
        if col not in raw.columns:
            raw[col] = None

    raw[FIELD_CAMP_CODE] = raw[FIELD_CAMP_CODE].map(normalize_str)
    raw[FIELD_FARMER_ID] = raw[FIELD_FARMER_ID].map(normalize_str)

    missing_camp_rows = int(raw[FIELD_CAMP_CODE].isna().sum()) if len(raw) else 0
    missing_farmerid_rows = int(raw[FIELD_FARMER_ID].isna().sum()) if len(raw) else 0

    # camp code -> label
    raw["camp_label"] = raw[FIELD_CAMP_CODE].map(CAMP_LABEL_MAP)

    # unmapped codes (quality check)
    unmapped_codes = sorted(
        [c for c in raw[FIELD_CAMP_CODE].dropna().unique().tolist() if c not in CAMP_LABEL_MAP]
    ) if len(raw) else []

    # aggregate unique farmerid per camp_label
    if len(raw):
        agg = (
            raw.dropna(subset=["camp_label", FIELD_FARMER_ID])
               .groupby("camp_label")[FIELD_FARMER_ID]
               .nunique()
               .reset_index(name="collected_n")
        )
    else:
        agg = pd.DataFrame({"camp_label": [], "collected_n": []})

    df = target_df.merge(agg, on="camp_label", how="left")
    df["collected_n"] = df["collected_n"].fillna(0).astype(int)

    df["remaining_n"] = (df["target_n"] - df["collected_n"]).clip(lower=0).astype(int)
    df["progress_pct"] = (100 * df["collected_n"] / df["target_n"]).fillna(0.0)

    # attach diagnostics (same value for all rows; just for display)
    df["missing_camp_rows"] = missing_camp_rows
    df["missing_farmerid_rows"] = missing_farmerid_rows
    df["unmapped_camp_codes"] = ", ".join(unmapped_codes)

    df = df.sort_values(["progress_pct", "remaining_n"], ascending=[True, False]).reset_index(drop=True)
    return df


def render_html(df: pd.DataFrame, updated_at: str) -> str:
    miss_camp = int(df["missing_camp_rows"].iloc[0]) if len(df) else 0
    miss_fid = int(df["missing_farmerid_rows"].iloc[0]) if len(df) else 0
    unmapped = df["unmapped_camp_codes"].iloc[0] if len(df) else ""

    rows = []
    for _, r in df.iterrows():
        camp = r["camp_label"]
        target_n = int(r["target_n"])
        collected_n = int(r["collected_n"])
        remaining_n = int(r["remaining_n"])
        pct = float(r["progress_pct"])
        bar = max(0.0, min(100.0, pct))
        status = "behind" if remaining_n > 0 else "done"

        rows.append(f"""
        <tr class="{status}">
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

    unmapped_line = f"<div>unmapped camp codes: <b>{unmapped}</b></div>" if unmapped else ""

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
  .footer {{ display:flex; justify-content:space-between; align-items:flex-start; gap:12px; padding: 12px 14px; color: var(--muted); font-size: 13px; }}
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <h1>Luapula Survey Progress</h1>
      <div class="meta">Last updated: {updated_at}</div>
    </header>

    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Camp</th>
            <th style="text-align:right;">Target</th>
            <th style="text-align:right;">Collected</th>
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
          <div>missing camp rows: <b>{miss_camp}</b></div>
          <div>missing farmerid rows: <b>{miss_fid}</b></div>
          {unmapped_line}
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

    target_df = pd.read_csv(TARGET_CSV_PATH, dtype={"camp_label": str})

    if "camp_label" not in target_df.columns or "target_n" not in target_df.columns:
        raise ValueError("data/target_camps.csv must have columns: camp_label,target_n")

    submissions = fetch_all_submissions()
    df = build_progress(submissions, target_df)

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
import os
import sys
import datetime as dt
from typing import Dict, Any, List, Optional

import pandas as pd
import requests

# ===== env =====
KOBO_BASE_URL = os.getenv("KOBO_BASE_URL", "").rstrip("/")
KOBO_TOKEN = os.getenv("KOBO_TOKEN", "")
KOBO_ASSET_UID = os.getenv("WESTERN_ASSET_UID", "")  # ★ここだけWestern

# ===== Kobo column names (Western formも同じならOK) =====
FIELD_CAMP_CODE = "section0/sec0_camp"
FIELD_FARMER_ID = "section0/sec0_farmerid"

# ===== camp code -> label (districtは捨てる) =====
CAMP_LABEL_MAP = {
    "1": "Lukena",
    "2": "Mishulundu",
    "3": "Namatindi",
    "4": "Ng'Uma",
    "5": "Sihole",
    "6": "Ikabako",
    "7": "Limulunga North",
    "8": "Limulunga South",
    "9": "Nangili",
    "10": "Ndanda East",
    "11": "Ndanda West",
    "12": "Simaa",
    "13": "Sitoya",
    "14": "Ushaa",
    "15": "Kawaya",
    "16": "Kashamba",
    "17": "Luanchuma",
    "18": "Lyalala",
    "19": "Mbanga",
    "20": "Ngulwana",
    "21": "Kakwacha",
    "22": "Lubelele",
    "23": "Lutembwe",
    "24": "Muyondoti",
    "25": "Mataba",
    "26": "Mitete Central",
    "27": "Sitwala",
    "28": "Sikunduko",
    "29": "Lupuyi",
    "30": "Kama",
    "31": "Litawa",
    "32": "Nakanya",
    "33": "Nalwei",
    "34": "Namushakende",
    "35": "Namusheshe",
    "36": "Sefula",
    "37": "Tapo",
    "38": "Liliachi",
    "39": "Litoya",
    "40": "Muoyo",
    "41": "Nasilimwe",
}

# ===== paths =====
TARGET_CSV_PATH = "data/western_camps.csv"  # ★これがターゲット数表
OUT_DIR = "docs"
OUT_HTML = os.path.join(OUT_DIR, "western.html")
OUT_CSV = os.path.join(OUT_DIR, "western_progress.csv")


def normalize_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def headers() -> Dict[str, str]:
    if not KOBO_TOKEN:
        raise RuntimeError("KOBO_TOKEN is empty.")
    return {"Authorization": f"Token {KOBO_TOKEN}", "Accept": "application/json"}


def fetch_all_submissions() -> List[Dict[str, Any]]:
    if not KOBO_BASE_URL:
        raise RuntimeError("KOBO_BASE_URL is empty.")
    if not KOBO_ASSET_UID:
        raise RuntimeError("WESTERN_ASSET_UID is empty.")

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


def load_targets() -> pd.DataFrame:
    """
    western_camps.csv は「ターゲット数表」
    必須列：
      - camp_label（or sec0_camp_label）
      - target_n
    """
    df = pd.read_csv(TARGET_CSV_PATH, dtype=str)

    # 許容：camp_label / sec0_camp / sec0_camp_label など。最終的に camp_label に揃える
    if "camp_label" not in df.columns:
        for alt in ["sec0_camp_label", "camp", "Camp", "label"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "camp_label"})
                break

    if "target_n" not in df.columns:
        for alt in ["target", "Target", "n_target", "N_target"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "target_n"})
                break

    if "camp_label" not in df.columns or "target_n" not in df.columns:
        raise ValueError("data/western_camps.csv must have columns camp_label,target_n (or compatible names)")

    df["camp_label"] = df["camp_label"].map(normalize_str)
    df = df.dropna(subset=["camp_label"])
    df["target_n"] = df["target_n"].astype(int)
    return df


def build_progress(submissions: List[Dict[str, Any]], target_df: pd.DataFrame) -> pd.DataFrame:
    raw = pd.json_normalize(submissions, sep="/") if submissions else pd.DataFrame()

    for col in [FIELD_CAMP_CODE, FIELD_FARMER_ID]:
        if col not in raw.columns:
            raw[col] = None

    raw[FIELD_CAMP_CODE] = raw[FIELD_CAMP_CODE].map(normalize_str)
    raw[FIELD_FARMER_ID] = raw[FIELD_FARMER_ID].map(normalize_str)

    missing_camp_rows = int(raw[FIELD_CAMP_CODE].isna().sum()) if len(raw) else 0
    missing_farmer_rows = int(raw[FIELD_FARMER_ID].isna().sum()) if len(raw) else 0

    raw["camp_label"] = raw[FIELD_CAMP_CODE].map(CAMP_LABEL_MAP)

    unmapped_codes = sorted(
        [c for c in raw[FIELD_CAMP_CODE].dropna().unique().tolist() if c not in CAMP_LABEL_MAP]
    ) if len(raw) else []

    agg = (
        raw.dropna(subset=["camp_label", FIELD_FARMER_ID])
           .groupby("camp_label")[FIELD_FARMER_ID]
           .nunique()
           .reset_index(name="collected_n")
    ) if len(raw) else pd.DataFrame({"camp_label": [], "collected_n": []})

    df = target_df.merge(agg, on="camp_label", how="left")
    df["collected_n"] = df["collected_n"].fillna(0).astype(int)
    df["remaining_n"] = (df["target_n"] - df["collected_n"]).clip(lower=0).astype(int)
    df["progress_pct"] = (100 * df["collected_n"] / df["target_n"]).fillna(0.0)

    df["missing_camp_rows"] = missing_camp_rows
    df["missing_farmerid_rows"] = missing_farmer_rows
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
<title>Western Survey Progress</title>
<style>
  body {{ font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Noto Sans JP", sans-serif; background:#fafafa; margin:0; }}
  .wrap {{ max-width: 1100px; margin: 32px auto; padding: 0 16px; }}
  header {{ display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin-bottom: 12px; }}
  h1 {{ font-size: 20px; margin:0; }}
  .meta {{ color:#666; font-size: 13px; white-space: nowrap; }}
  .card {{ background:#fff; border:1px solid #e6e6e6; border-radius: 16px; overflow:hidden; }}
  table {{ width:100%; border-collapse: collapse; }}
  thead th {{ text-align:left; font-size: 13px; color:#666; padding: 12px 14px; border-bottom:1px solid #e6e6e6; background:#fcfcfc; }}
  tbody td {{ padding: 12px 14px; border-bottom: 1px solid #e6e6e6; font-size: 14px; }}
  tbody tr.behind {{ background:#fff7ed; }}
  tbody tr.done {{ background:#f6ffed; }}
  td.num {{ text-align:right; font-variant-numeric: tabular-nums; width: 90px; }}
  td.camp {{ font-weight: 600; }}
  td.progress {{ width: 240px; }}
  .bar-bg {{ height: 10px; background:#f2f2f2; border-radius: 999px; overflow:hidden; border:1px solid #eaeaea; }}
  .bar {{ height:100%; background:#111; }}
  .pct {{ font-size: 12px; color:#666; margin-top: 6px; text-align:right; font-variant-numeric: tabular-nums; }}
  .footer {{ display:flex; justify-content:space-between; align-items:flex-start; gap:12px; padding: 12px 14px; color:#666; font-size: 13px; }}
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <h1>Western Survey Progress</h1>
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
        <div><a href="./western_progress.csv">CSV</a></div>
      </div>
    </div>
  </div>
</body>
</html>
"""


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    target_df = load_targets()
    subs = fetch_all_submissions()
    df = build_progress(subs, target_df)

    df.to_csv(OUT_CSV, index=False)
    updated_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(render_html(df, updated_at))

    print("Wrote:", OUT_HTML)
    print("Wrote:", OUT_CSV)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
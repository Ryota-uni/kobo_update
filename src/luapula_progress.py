import os
import pandas as pd
import datetime as dt

OUT_DIR = "docs"
OUT_HTML = os.path.join(OUT_DIR, "index.html")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = pd.DataFrame({
        "sec0_camp": ["CampA", "CampB", "CampC"],
        "target_n": [10, 15, 8],
        "collected_n": [3, 15, 4],
    })
    df["remaining_n"] = df["target_n"] - df["collected_n"]
    df["progress_pct"] = 100 * df["collected_n"] / df["target_n"]

    updated_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    html = f"""
    <html>
    <head><title>Zambia Survey Progress</title></head>
    <body>
    <h1>Zambia Survey Progress</h1>
    <p>Last updated: {updated_at}</p>
    {df.to_html(index=False)}
    </body>
    </html>
    """

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print("Generated:", OUT_HTML)

if __name__ == "__main__":
    main()
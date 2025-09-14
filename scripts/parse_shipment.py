# scripts/parse_shipment.py
from pathlib import Path
import pandas as pd
import csv, re, io

RAW = Path("data/raw/SHIPMENT.TXT")
OUTDIR = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTDIR / "master2.csv"
OUT_TSV = OUTDIR / "master2.tsv"
OUT_MIN = OUTDIR / "master_minimal.csv"
OUT_SPLIT = OUTDIR / "by_year"
OUT_SPLIT.mkdir(parents=True, exist_ok=True)

# --- Fonctions utilitaires ---

def parse_line_comma(line: str):
    """Découpe une ligne CSV/TXT en colonnes"""
    reader = csv.reader(io.StringIO(line))
    return next(reader)

def parse_yyyymmdd_float(val):
    """Transforme un float style 19981103.000000 en date YYYY-MM-DD"""
    try:
        s = str(int(float(val)))
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    except:
        return None

# --- Main ---

def main():
    # Charger brut
    rows = []
    with open(RAW, "r", encoding="latin1") as f:
        for line in f:
            parts = parse_line_comma(line.strip())
            rows.append(parts)

    # DataFrame
    df = pd.DataFrame(rows)

    # Colonnes (ajuste selon ton export réel)
    df.columns = [
        "shipment_number", "order_date", "from_site", "to_site",
        "from_site_name", "to_site", "to_site_name",
        "pickup_date", "delivery_date",
        "price", "rated", "cost", "extra1", "extra2", "extra3", "active"
    ][:df.shape[1]]

    # Conversion des dates
    if "order_date" in df.columns:
        df["order_date"] = df["order_date"].apply(parse_yyyymmdd_float)
    if "pickup_date" in df.columns:
        df["pickup_date"] = df["pickup_date"].apply(parse_yyyymmdd_float)
    if "delivery_date" in df.columns:
        df["delivery_date"] = df["delivery_date"].apply(parse_yyyymmdd_float)

    # Sauvegardes principales
    df.to_csv(OUT_CSV, index=False, quoting=csv.QUOTE_MINIMAL)
    df.to_csv(OUT_TSV, index=False, sep="\t")

    # Version minimaliste
    master = pd.DataFrame({
        "date": df.get("order_date"),
        "order_no": df.get("shipment_number"),
        "customer": df.get("from_site_name"),
        "origin": df.get("from_site_name"),
        "destination": df.get("to_site_name"),
        "revenue": pd.to_numeric(df.get("price"), errors="coerce"),
        "cost": pd.to_numeric(df.get("cost"), errors="coerce"),
    })
    master["margin"] = master["revenue"] - master["cost"]
    master.to_csv(OUT_MIN, index=False)

    # --- Split annuel ---
    if "order_date" in df.columns:
        df["year"] = pd.to_datetime(df["order_date"], errors="coerce").dt.year
        for year, sub in df.groupby("year"):
            if pd.isna(year):
                continue
            out_path = OUT_SPLIT / f"master2_{int(year)}.csv"
            sub.drop(columns=["year"]).to_csv(out_path, index=False)
            print(f"  → {out_path} ({sub.shape[0]} lignes)")

    print(f"✅ Export global : {OUT_CSV} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
    print(f"✅ Export minimal : {OUT_MIN} ({master.shape[0]} lignes)")
    print(f"✅ Split annuel dans : {OUT_SPLIT}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import re
from pathlib import Path
import pandas as pd

ENRICHED_DIR = Path("data/processed/pdf_csv")
OUT_DIR = Path("data/processed/master")
OUT_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED_COLS = [
    "order_no","req_pu_date","customer","origin","destination",
    "revenue","cost","margin","distance_km",
    "rate_per_km","cost_per_km","margin_per_km",
]

YEAR_RE = re.compile(r"(\d{4})")

def load_one(file: Path) -> pd.DataFrame:
    df = pd.read_csv(file, sep="\t", dtype=str, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]

    # Colonnes manquantes -> colonnes vides
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[EXPECTED_COLS].copy()

    # Parse dates si possible
    df["req_pu_date"] = pd.to_datetime(df["req_pu_date"], errors="coerce", utc=False)

    # Types numériques
    for c in ["revenue","cost","margin","distance_km","rate_per_km","cost_per_km","margin_per_km"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ajoute colonne year: d’abord via la date…
    df["year"] = df["req_pu_date"].dt.year

    # …puis fallback: essaie de prendre l’année depuis le **nom du fichier**
    if df["year"].isna().all():
        m = YEAR_RE.search(file.name)
        if m:
            y = int(m.group(1))
            df["year"] = y
            print(f"⚠️  {file.name}: dates inutilisables → fallback année={y} via nom de fichier")
        else:
            print(f"⚠️  {file.name}: aucune date exploitable et pas d’année dans le nom → lignes sans année")

    # Dedup (ordre + date)
    df = df.drop_duplicates(subset=["order_no","req_pu_date"], keep="last")
    return df

def main():
    files = sorted(ENRICHED_DIR.glob("*_enriched.tsv"))
    if not files:
        print("❌ Aucun *_enriched.tsv dans data/processed/pdf_csv — lance d’abord l’enrichissement.")
        raise SystemExit(1)

    parts = []
    for f in files:
        try:
            df = load_one(f)
            print(f"✓ {f.name}: {len(df)} lignes, années détectées: {sorted(set(df['year'].dropna().astype(int))) or ['(aucune)']}")
            parts.append(df)
        except Exception as e:
            print(f"❌ Erreur lecture {f}: {e}")

    if not parts:
        print("❌ Aucun dataframe valide, arrêt.")
        raise SystemExit(1)

    all_df = pd.concat(parts, ignore_index=True)

    # Si vraiment aucune année, on abandonne (évite d’écrire seulement le master vide)
    if all_df["year"].isna().all():
        print("❌ Aucune année détectée dans les données fusionnées — rien à écrire par année.")
        raise SystemExit(1)

    # Tri + écriture master
    all_df = all_df.sort_values(["req_pu_date","order_no"], kind="mergesort", na_position="last")
    all_path = OUT_DIR / "orders_master_enriched.tsv"
    all_df.to_csv(all_path, sep="\t", index=False)
    print(f"💾 Master écrit: {all_path} ({len(all_df)} lignes)")
    print(f"   Années présentes: {sorted(set(all_df['year'].dropna().astype(int)))}")

    # Écriture par année
    wrote_any_year = False
    for year, g in all_df.groupby(all_df["year"].dropna().astype(int)):
        ydf = g.sort_values(["req_pu_date","order_no"], kind="mergesort", na_position="last")
        ypath = OUT_DIR / f"orders_{int(year)}_enriched.tsv"
        ydf.to_csv(ypath, sep="\t", index=False)
        wrote_any_year = True
        print(f"💾 Année {int(year)}: {ypath} ({len(ydf)} lignes)")

    if not wrote_any_year:
        print("❌ groupby(year) n’a rien produit — vérifie les dates/années")
        raise SystemExit(1)

if __name__ == "__main__":
    main()

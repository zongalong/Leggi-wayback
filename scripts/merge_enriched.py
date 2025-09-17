#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd

ENRICHED_DIR = Path("data/processed/pdf_csv")
OUT_DIR = Path("data/processed/master")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Colonnes attendues (ordre canonique)
EXPECTED_COLS = [
    "order_no",
    "req_pu_date",
    "customer",
    "origin",
    "destination",
    "revenue",
    "cost",
    "margin",
    "distance_km",
    "rate_per_km",
    "cost_per_km",
    "margin_per_km",
]

def load_one(file: Path) -> pd.DataFrame:
    df = pd.read_csv(file, sep="\t", dtype=str, engine="python")
    # Nettoyage colonnes (enlève espaces, lower)
    df.columns = [c.strip().lower() for c in df.columns]

    # Si certaines colonnes manquent, on les crée vides
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # Ne garder que les colonnes attendues, dans l’ordre
    df = df[EXPECTED_COLS].copy()

    # Types
    # date -> datetime (format déjà ISO YYYY-MM-DD côté normalize)
    df["req_pu_date"] = pd.to_datetime(df["req_pu_date"], errors="coerce")

    # numériques
    for c in ["revenue","cost","margin","distance_km","rate_per_km","cost_per_km","margin_per_km"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ajoute year pour la sortie par année
    df["year"] = df["req_pu_date"].dt.year

    # Dedup simple (même ordre + date)
    df = df.drop_duplicates(subset=["order_no","req_pu_date"], keep="last")

    return df

def main():
    files = sorted(ENRICHED_DIR.glob("*_enriched.tsv"))
    if not files:
        print("⚠️  Aucun fichier *_enriched.tsv trouvé, rien à fusionner.")
        return

    parts = []
    for f in files:
        try:
            df = load_one(f)
            print(f"✓ Lu {f.name}: {len(df)} lignes")
            parts.append(df)
        except Exception as e:
            print(f"❌ Erreur sur {f}: {e}")

    if not parts:
        print("⚠️  Rien à écrire.")
        return

    all_df = pd.concat(parts, ignore_index=True)
    # Tri global
    all_df = all_df.sort_values(["req_pu_date","order_no"], kind="mergesort")

    # Sauvegarde all-in-one
    all_path = OUT_DIR / "orders_master_enriched.tsv"
    all_df.to_csv(all_path, sep="\t", index=False)
    print(f"💾 Écrit {all_path} ({len(all_df)} lignes)")

    # Sauvegardes par année
    for year, g in all_df.groupby("year", dropna=True):
        ypath = OUT_DIR / f"orders_{int(year)}_enriched.tsv"
        g_sorted = g.sort_values(["req_pu_date","order_no"], kind="mergesort")
        g_sorted.to_csv(ypath, sep="\t", index=False)
        print(f"💾 Écrit {ypath} ({len(g_sorted)} lignes)")

if __name__ == "__main__":
    main()

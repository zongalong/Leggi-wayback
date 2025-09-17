#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robuste PDF -> TSV pour "Activity Report" (ordersYYYY.pdf)

- Essaie Tabula en mode lattice et stream
- Détecte et scinde les colonnes fusionnées "Origin Destination Revenue"
- Nettoie "CA", virgules, espaces, lignes d'en-tête
- Écrit 2 fichiers:
    data/processed/pdf_csv/ordersYYYY.tsv
    data/processed/pdf_csv/ordersYYYY_norm.tsv  (colonnes normalisées)
"""

from __future__ import annotations
import re
import sys
from pathlib import Path
import pandas as pd

try:
    import tabula  # tabula-py (nécessite Java)
except Exception as e:
    print("❗ tabula-py n'est pas installé ou Java absent:", e)
    sys.exit(1)

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/pdf_csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- utilitaires -------------------------------------------------------------

def _clean_money(val: str | float | int) -> float:
    """'2,150.00 CA' -> 2150.0 ; gère None/''."""
    if pd.isna(val):
        return 0.0
    s = str(val)
    s = s.replace("CA", "").replace("$", "").replace(",", "").strip()
    if s == "" or re.fullmatch(r"[-–—]?\s*", s):
        return 0.0
    try:
        return float(s)
    except ValueError:
        # parfois "0.04" devient ".04"
        m = re.search(r"(-?\d+(?:\.\d+)?)", s)
        return float(m.group(1)) if m else 0.0

def _coerce_date(s: str) -> str:
    """'04/01/2022' -> '2022-01-04' ; renvoie tel quel si déjà normalisée."""
    if pd.isna(s):
        return ""
    s = str(s).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)  # dd/mm/yyyy
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    # parfois Tabula laisse "01/ 01 /2022"
    s2 = re.sub(r"\s+", "", s)
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s2)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    return s

FUSED_COL_PAT = re.compile(
    r"""
    ^\s*
    (?P<origin>[A-ZÉÈÀÎÔÂÇ' \.-]+,[A-Z]{2})     # ex. BOUCHERVILLE,PQ
    \s+
    (?P<dest>[A-ZÉÈÀÎÔÂÇ' \.-]+,[A-Z]{2})       # ex. MONTREAL-NORD,PQ
    \s+
    (?P<rev>-?\d[\d,]*\.?\d*)                   # 225.00 ou 2,150.00
    (?:\s*CA)?\s*$
    """,
    re.VERBOSE
)

def split_fused_origin_dest_rev(cell: str) -> tuple[str, str, str] | None:
    if not isinstance(cell, str):
        return None
    m = FUSED_COL_PAT.match(cell.strip())
    if not m:
        return None
    return m.group("origin"), m.group("dest"), m.group("rev")

# --- extraction --------------------------------------------------------------

def read_pdf_best(pdf_path: Path) -> pd.DataFrame:
    """Essaie lattice puis stream, retourne le meilleur DF concaténé."""
    dfs = []

    for mode in ("lattice", "stream"):
        try:
            frames = tabula.read_pdf(
                str(pdf_path),
                pages="all",
                multiple_tables=True,
                lattice=(mode == "lattice"),
                stream=(mode == "stream"),
                guess=True
            )
            # gardons seulement tables "assez larges"
            for f in frames or []:
                if isinstance(f, pd.DataFrame) and f.shape[1] >= 3:
                    dfs.append((mode, f))
        except Exception as e:
            print(f"⚠️ Tabula {mode} a échoué: {e}")

    if not dfs:
        raise RuntimeError(f"Aucune table extraite depuis {pdf_path}")

    # Heuristique: privilégier la table avec le plus de colonnes
    best_mode, best_df = max(dfs, key=lambda it: it[1].shape[1])
    print(f"ℹ️  Mode retenu: {best_mode} ({best_df.shape[0]} lignes, {best_df.shape[1]} colonnes)")

    # Concatène toutes les tables "compatibles" en suivant le même mode choisi
    tables = [df for m, df in dfs if m == best_mode]
    df = pd.concat(tables, ignore_index=True)

    return df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Supprimer lignes d'en-tête / titres intermédiaires
    def is_header_row(row: pd.Series) -> bool:
        text = " ".join(str(x) for x in row.values if not pd.isna(x)).upper()
        return (
            "ACTIVITY REPORT" in text
            or "REPORT PERIOD" in text
            or "ORDER NO" in text
            or "HOME CUR" in text
        )

    df = df[~df.apply(is_header_row, axis=1)].copy()

    # Renommer colonnes par heuristique
    cols = [str(c).strip().lower() for c in df.columns]

    # Cas 1: colonnes attendues présentes
    expected = ["order", "req", "customer", "origin", "destination", "revenue", "cost", "margin"]
    # mapping heuristique
    mapping = {}
    for i, c in enumerate(cols):
        if "order" in c and "no" in c:
            mapping[df.columns[i]] = "order_no"
        elif ("req" in c and "pu" in c) or ("pickup" in c):
            mapping[df.columns[i]] = "req_pu_date"
        elif "customer" in c:
            mapping[df.columns[i]] = "customer"
        elif "origin" in c and "destination" in c and "revenue" in c:
            mapping[df.columns[i]] = "fused_odr"
        elif "origin" in c:
            mapping[df.columns[i]] = "origin"
        elif "destination" in c:
            mapping[df.columns[i]] = "destination"
        elif "revenue" in c:
            mapping[df.columns[i]] = "revenue"
        elif "cost" in c:
            mapping[df.columns[i]] = "cost"
        elif "margin" in c:
            mapping[df.columns[i]] = "margin"

    df = df.rename(columns=mapping)

    # Si colonne fusionnée détectée, scinder
    if "fused_odr" in df.columns:
        o, d, r = [], [], []
        for v in df["fused_odr"].astype(str).fillna(""):
            tri = split_fused_origin_dest_rev(v)
            if tri:
                oo, dd, rr = tri
            else:
                oo, dd, rr = "", "", ""
            o.append(oo); d.append(dd); r.append(rr)
        df["origin"] = df.get("origin", pd.Series(o)).replace("", pd.NA)
        df["destination"] = df.get("destination", pd.Series(d)).replace("", pd.NA)
        # ne pas écraser un revenue déjà séparé
        if "revenue" not in df.columns or df["revenue"].isna().all():
            df["revenue"] = r
        df = df.drop(columns=["fused_odr"], errors="ignore")

    # Garder uniquement les colonnes d'intérêt
    keep = ["order_no", "req_pu_date", "customer", "origin", "destination", "revenue", "cost", "margin"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA
    df = df[keep]

    # Nettoyage/normalisation
    df["order_no"] = df["order_no"].astype(str).str.extract(r"(\d+)")[0]
    df["req_pu_date"] = df["req_pu_date"].map(_coerce_date)
    df["customer"] = df["customer"].astype(str).str.strip()

    # Certaines lignes vides héritent visuellement du client juste au-dessus → forward fill
    df["customer"] = df["customer"].replace({"": pd.NA}).ffill()

    for col in ("origin", "destination"):
        df[col] = df[col].astype(str).str.strip()
        # Filtrer les fusions ratées (mots collés sans province) si besoin
        df[col] = df[col].where(df[col].str.contains(r",[A-Z]{2}$", na=False), df[col])

    for col in ("revenue", "cost", "margin"):
        df[col] = df[col].map(_clean_money).astype(float)

    # Supprimer lignes clairement vides
    mask_allna = df[["order_no","req_pu_date","customer","origin","destination"]].replace("", pd.NA).isna().all(axis=1)
    df = df[~mask_allna].copy()

    # Re-trier si l'ordre a sauté
    with pd.option_context("future.no_silent_downcasting", True):
        df["order_no"] = pd.to_numeric(df["order_no"], errors="coerce").astype("Int64")

    return df.reset_index(drop=True)

# --- pipeline principal ------------------------------------------------------

def convert_one(pdf_path: Path):
    year = re.search(r"(\d{4})", pdf_path.stem)
    year = year.group(1) if year else "unknown"

    print(f"\n📄 PDF: {pdf_path.name}")
    raw_df = read_pdf_best(pdf_path)

    # Sauvegarde brute "au cas où"
    raw_tsv = OUT_DIR / f"{pdf_path.stem}_raw.tsv"
    raw_df.to_csv(raw_tsv, sep="\t", index=False)
    print(f"💾 TSV brut: {raw_tsv}")

    norm_df = normalize_columns(raw_df)
    norm_tsv = OUT_DIR / f"orders{year}.tsv"
    norm_df.to_csv(norm_tsv, sep="\t", index=False)
    print(f"✅ TSV normalisé: {norm_tsv} ({len(norm_df)} lignes)")

    # Variante _norm pour la suite du pipeline existant
    norm_tsv2 = OUT_DIR / f"orders{year}_norm.tsv"
    norm_df.to_csv(norm_tsv2, sep="\t", index=False)
    print(f"↳ Alias: {norm_tsv2}")

def main():
    pdfs = sorted(RAW_DIR.glob("orders*.pdf"))
    if not pdfs:
        print("Aucun PDF trouvé dans data/raw (pattern orders*.pdf).")
        return
    for pdf in pdfs:
        convert_one(pdf)

if __name__ == "__main__":
    main()

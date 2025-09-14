#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convertit tous les PDF de data/raw/ en TSV (séparateur tab) dans data/processed/.
- Un TSV par PDF (même nom de base).
- Un TSV fusionné "invoices_master.tsv" regroupant toutes les tables.
Dépendances: pdfplumber, pandas
"""

from pathlib import Path
import pandas as pd
import pdfplumber
import re

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PER_FILE_DIR = OUT_DIR / "pdf_csv"
OUT_PER_FILE_DIR.mkdir(parents=True, exist_ok=True)

MASTER_OUT = OUT_DIR / "invoices_master.tsv"

def _clean_cell(x):
    if x is None:
        return None
    # Normalise espaces, supprime espaces non imprimables
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _drop_empty(df: pd.DataFrame) -> pd.DataFrame:
    # Supprime colonnes 100% vides puis lignes 100% vides
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    # Supprime colonnes vides après nettoyage (ex: ""," ","—")
    df = df[[c for c in df.columns if not (df[c].astype(str).str.strip() == "").all()]]
    return df

def _maybe_promote_header(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tente de promouvoir la 1re ligne en en-têtes si elle ressemble à des titres.
    Heuristique simple: si >50% des cellules de la 1re ligne sont “alphabétiques”.
    """
    if df.empty:
        return df
    first = df.iloc[0].astype(str).fillna("")
    alpha_ratio = (first.str.contains(r"[A-Za-z]", regex=True)).mean()
    if alpha_ratio >= 0.5:
        df.columns = [c if str(c).strip() not in ["", "None"] else f"col_{i+1}" for i, c in enumerate(first)]
        df = df.iloc[1:].reset_index(drop=True)
    else:
        # sinon, génère des noms de colonnes génériques s’ils sont 0..N-1
        if (df.columns == pd.RangeIndex(0, len(df.columns))).all():
            df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
    return df

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoie en-têtes (supprime espaces, caractères spéciaux légers)
    def fix_col(c):
        c = str(c).strip()
        c = re.sub(r"\s+", "_", c)
        c = re.sub(r"[^0-9A-Za-z_]", "", c)
        return c.lower() or "col"
    df.columns = [fix_col(c) for c in df.columns]
    # Dé-duplique les colonnes en cas de collisions
    seen = {}
    new_cols = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
    df.columns = new_cols
    return df

def extract_tables_from_pdf(pdf_path: Path) -> list[pd.DataFrame]:
    """Extrait toutes les tables d’un PDF (toutes pages), renvoie une liste de DataFrames nettoyés."""
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Plusieurs moteurs de détection : table + tables
            # 1) tables = liste de matrices (list[list[str]])
            tables = page.extract_tables() or []
            for t in tables:
                df = pd.DataFrame(t)
                # Nettoyage des cellules
                df = df.applymap(_clean_cell)
                df = _drop_empty(df)
                if df.empty:
                    continue
                df = _maybe_promote_header(df)
                df = _drop_empty(df)
                df = _normalize_columns(df)
                if not df.empty:
                    out.append(df)
    return out

def save_tsv(df: pd.DataFrame, path: Path):
    # TSV “propre” (utf-8 + tabulation)
    df.to_csv(path, sep="\t", index=False)

def main():
    pdfs = sorted([p for p in RAW_DIR.glob("*.pdf") if p.is_file()])
    all_rows = []

    if not pdfs:
        print("Aucun PDF trouvé dans data/raw/. Rien à faire.")
        return

    for pdf_path in pdfs:
        print(f"→ Traitement: {pdf_path.name}")
        dfs = extract_tables_from_pdf(pdf_path)
        if not dfs:
            print(f"   (aucune table détectée)"); continue

        # Stratégie: si plusieurs tables, on les concatène verticalement (mêmes colonnes ou non)
        # On essaie d’aligner sur le set de colonnes maximal.
        # Sinon on concatène en alignant par nom (les manquants seront NaN).
        # NB: ça reste générique; si tes rapports ont 1 table/page, ce sera nickel.
        base_cols = set()
        for d in dfs:
            base_cols |= set(d.columns)
        base_cols = list(base_cols)

        dfs_aligned = []
        for d in dfs:
            dfs_aligned.append(d.reindex(columns=base_cols))

        combined = pd.concat(dfs_aligned, ignore_index=True)
        combined = _drop_empty(combined)

        # Sauvegarde par PDF
        out_file = OUT_PER_FILE_DIR / f"{pdf_path.stem}.tsv"
        save_tsv(combined, out_file)
        print(f"   ✔ {out_file} ({combined.shape[0]} lignes, {combined.shape[1]} colonnes)")

        # Empile pour le master
        combined.insert(0, "source_pdf", pdf_path.name)
        all_rows.append(combined)

    # Master fusionné (toutes sources)
    if all_rows:
        master = pd.concat(all_rows, ignore_index=True)
        save_tsv(master, MASTER_OUT)
        print(f"✔ Master: {MASTER_OUT} ({master.shape[0]} lignes, {master.shape[1]} colonnes)")
    else:
        print("Aucune table exportée; master non généré.")

if __name__ == "__main__":
    main()

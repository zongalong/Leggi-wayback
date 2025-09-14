#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convertit tous les PDF de data/raw/ en TSV (tab) dans data/processed/.
Utilise Tabula (tabula-py) pour une extraction plus robuste des tableaux.
Recollage des "lignes dédoublées" (ligne 1 = Order/Client/Orig/Dest, ligne 2 = Date/Marge).
Sorties :
- data/processed/pdf_csv/<nom_pdf>.tsv
- data/processed/invoices_master.tsv (fusion de tous les PDF)
"""

from pathlib import Path
import pandas as pd
import re
import tabula  # nécessite Java sur le runner

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
PER_FILE_DIR = OUT_DIR / "pdf_csv"
PER_FILE_DIR.mkdir(parents=True, exist_ok=True)
MASTER = OUT_DIR / "invoices_master.tsv"

# ---- utilitaires ----

def _norm_txt(s):
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _to_amount(x):
    """Convertit '1,234.50 CA' -> 1234.50 ; gère vide/None."""
    if pd.isna(x):
        return None
    s = str(x)
    s = s.replace(" CA", "").replace(" C A", "")
    s = s.replace(",", "")
    s = s.replace("$", "")
    s = s.strip()
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except:
        return None

def _looks_order_no(x):
    # N° d'ordre = entier à 5 chiffres+ (ex: 21401)
    if pd.isna(x): 
        return False
    s = str(x).strip()
    return bool(re.fullmatch(r"\d{4,}", s))

def _looks_date(x):
    # pdf montre souvent 'dd/mm/yyyy' (ex: 04/01/2022)
    if pd.isna(x): 
        return False
    s = str(x).strip()
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", s))

def _stitch_rows(df):
    """
    Recolle les lignes "dédoublées".
    Hypothèse de colonnes brut : quelque chose comme
    [col1, col2, col3, col4, col5, col6, col7, col8]
    mais très variable. On détecte :
      - ligne A : col1 ~ order_no, col3/4/5 ~ customer/origin/destination
      - ligne B (juste après) : col2 ~ date, dernière col ~ margin
    On fabrique un enregistrement unique : order_no, date, customer, origin, destination, revenue, cost, margin.
    """
    records = []
    # on remplit d'abord tout en texte
    dft = df.applymap(_norm_txt)

    # essaie de repérer des colonnes candidates
    # Heuristique : dernière(s) colonnes = [Revenue, Cost, Margin]
    ncol = dft.shape[1]
    # On autorise 2 à 4 dernières colonnes comme montants :
    tail_cols = list(range(max(0, ncol-4), ncol))

    i = 0
    while i < len(dft):
        row = dft.iloc[i]
        order_no = None
        req_date = None
        customer = None
        origin = None
        destination = None
        revenue = None
        cost = None
        margin = None

        # Cherche un order_no sur la ligne courante
        for c in range(ncol):
            if _looks_order_no(row.iloc[c]):
                order_no = row.iloc[c]
                break

        # Candidate customer/origin/destination : on prend les 2-3 colonnes plus textuelles de la ligne
        # (en général ce sont des colonnes du milieu)
        # On prend les colonnes non vides hors montants et hors date.
        text_cols = [c for c in range(ncol) if c not in tail_cols]
        texts = [row.iloc[c] for c in text_cols if row.iloc[c] not in (None, "")]
        # Heuristique : souvent 3 blocs -> customer, origin, destination
        if texts:
            # on reduce taille excessive
            texts = [t for t in texts if not _looks_date(t) and not _looks_order_no(t)]
            # Si la 1re cellule non vide est 'CA', on l'ignore
            texts = [t for t in texts if t != "CA"]
            if len(texts) >= 1: customer = texts[0]
            if len(texts) >= 2: origin   = texts[1]
            if len(texts) >= 3: destination = texts[2]

        # Montants ligne A (parfois déjà présents)
        tail_vals = [row.iloc[c] for c in tail_cols]
        amounts = list(map(_to_amount, tail_vals))
        # On suppose l'ordre : Revenue, Cost, Margin si 3 colonnes
        if len(amounts) >= 1: 
            revenue = amounts[0]
        if len(amounts) >= 2: 
            cost = amounts[1]
        if len(amounts) >= 3: 
            margin = amounts[2]

        # Regarde la ligne suivante pour la date/marge déportée
        if i + 1 < len(dft):
            row2 = dft.iloc[i+1]
            # date souvent en 2e colonne
            for c in range(ncol):
                if _looks_date(row2.iloc[c]):
                    req_date = row2.iloc[c]
                    break
            # marge souvent en dernière colonne
            tail_vals2 = [row2.iloc[c] for c in tail_cols]
            amounts2 = list(map(_to_amount, tail_vals2))
            # la marge se trouve souvent en dernière position
            if amounts2:
                # prend le dernier non None
                for a in reversed(amounts2):
                    if a is not None:
                        margin = margin if margin is not None else a
                        break

            # Si on a détecté une date sur la ligne suivante,
            # on considère que la paire (ligne courante + suivante) forme un enregistrement.
            if req_date is not None:
                i += 2
            else:
                i += 1
        else:
            i += 1

        # Only keep if on a au moins un order_no + quelque chose d'utile
        if order_no is not None and any([customer, origin, destination, revenue, cost, margin, req_date]):
            records.append({
                "order_no": order_no,
                "req_pu_date": req_date,
                "customer": customer,
                "origin": origin,
                "destination": destination,
                "revenue": revenue,
                "cost": cost,
                "margin": margin,
            })

    out = pd.DataFrame.from_records(records)
    # Nettoyage final
    out["order_no"] = out["order_no"].astype(str)
    out["req_pu_date"] = pd.to_datetime(out["req_pu_date"], format="%d/%m/%Y", errors="coerce")
    return out

def extract_pdf(pdf_path: Path) -> pd.DataFrame:
    """
    Essaye d'abord en mode 'lattice', puis 'stream', puis combine et recolle.
    """
    dfs = []

    try:
        t1 = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True,
                             lattice=True, guess=False)
        dfs += t1 or []
    except Exception:
        pass

    try:
        t2 = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True,
                             stream=True, guess=True)
        dfs += t2 or []
    except Exception:
        pass

    if not dfs:
        return pd.DataFrame()

    # Nettoyage de base des tables
    cleaned = []
    for d in dfs:
        if d is None or d.empty:
            continue
        d = d.applymap(_norm_txt)
        # enlève colonnes/lignes vides
        d = d.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if d.empty:
            continue
        cleaned.append(d)

    if not cleaned:
        return pd.DataFrame()

    # Concat brute puis recollage par heuristique
    brute = pd.concat(cleaned, ignore_index=True, sort=False)
    stitched = _stitch_rows(brute)
    return stitched

def save_tsv(df: pd.DataFrame, path: Path):
    df.to_csv(path, sep="\t", index=False)

def main():
    pdfs = sorted([p for p in RAW_DIR.glob("*.pdf") if p.is_file()])
    if not pdfs:
        print("Aucun PDF trouvé dans data/raw/.")
        return

    all_df = []
    for p in pdfs:
        print(f"→ {p.name}")
        df = extract_pdf(p)
        if df.empty:
            print("   (aucune table interprétable)")
            continue
        out_file = PER_FILE_DIR / f"{p.stem}.tsv"
        save_tsv(df, out_file)
        print(f"   ✔ {out_file} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
        df2 = df.copy()
        df2.insert(0, "source_pdf", p.name)
        all_df.append(df2)

    if all_df:
        master = pd.concat(all_df, ignore_index=True)
        save_tsv(master, MASTER)
        print(f"✔ Master: {MASTER} ({master.shape[0]} lignes)")
    else:
        print("Rien à fusionner.")

if __name__ == "__main__":
    main()

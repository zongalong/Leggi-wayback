#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extraction des rapports 'Activity Report' PDF -> TSV propre
Colonnes: order_no, req_pu_date, customer, origin, destination, revenue, cost, margin
- Ignore automatiquement les 2–3 premières lignes d'entête (Report Period, headers, etc.)
- Gère les lignes "dédoublées" (ligne A: order/customer/origin/dest ; ligne B: date/margin)
- Produit 1 TSV par PDF (data/processed/pdf_csv/<nom>.tsv) + un master (data/processed/orders_master.tsv)
Dépendances: pandas, tabula-py (Java requis sur le runner)
"""

from pathlib import Path
import re
import pandas as pd
import tabula  # nécessite default-jre sur GitHub Actions

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
PER_FILE_DIR = OUT_DIR / "pdf_csv"
PER_FILE_DIR.mkdir(parents=True, exist_ok=True)
MASTER_PATH = OUT_DIR / "orders_master.tsv"

# ---------- utils ----------

def norm(s):
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def to_amount(x):
    if pd.isna(x):
        return None
    s = str(x)
    s = s.replace(" CA", "").replace("$", "").replace(",", "").strip()
    if s in ("", "-"):
        return None
    try:
        return float(s)
    except:
        # essaie 1 000,00 style FR
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except:
            return None

def looks_order_no(x):
    if pd.isna(x): 
        return False
    return bool(re.fullmatch(r"\d{4,}", str(x).strip()))

def looks_date(x):
    if pd.isna(x): 
        return False
    s = str(x).strip()
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", s))

def drop_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les 2–3 premières lignes d'en-tête (ex: 'Report Period', 'Activity Report', etc.)
    + lignes vides globales.
    """
    if df.empty:
        return df

    # normalise
    df = df.applymap(norm)
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)

    # si 1ère ou 2e ligne contient 'Report Period' ou 'Activity Report' => drop jusqu’à la ligne de titres
    head_flags = []
    for i in range(min(5, len(df))):
        rowtxt = " ".join([c for c in df.iloc[i].astype(str).fillna("") if c])
        head_flags.append(
            any(k in rowtxt.upper() for k in ("REPORT PERIOD", "ACTIVITY REPORT", "HOME CUR"))
        )

    drop_n = 0
    # heuristique: si on voit des flags en haut, on drop 2 ou 3 lignes
    if any(head_flags[:2]):
        drop_n = 3
    elif head_flags and head_flags[0]:
        drop_n = 2

    if drop_n:
        df = df.iloc[drop_n:].reset_index(drop=True)

    # re-nettoyage
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    return df

def stitch_lines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recolle les paires de lignes 'A/B' en un seul enregistrement.
    Sortie: DataFrame(order_no, req_pu_date, customer, origin, destination, revenue, cost, margin)
    """
    if df.empty:
        return pd.DataFrame(columns=["order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"])

    # normalise tout
    df = df.applymap(norm).dropna(how="all", axis=1).dropna(how="all", axis=0)
    ncol = df.shape[1]

    # hypothèse: dernières colonnes = montants (Revenue, Cost, Margin)
    tail_cols = list(range(max(0, ncol-4), ncol))
    out = []

    i = 0
    while i < len(df):
        row = df.iloc[i]

        order_no = None
        req_pu_date = None
        customer = None
        origin = None
        destination = None
        revenue = None
        cost = None
        margin = None

        # A) ligne courante: trouve order_no
        for c in range(ncol):
            if looks_order_no(row.iloc[c]):
                order_no = row.iloc[c]
                break

        # A) extraire blocs texte (customer, origin, destination)
        text_idxs = [c for c in range(ncol) if c not in tail_cols]
        texts = [row.iloc[c] for c in text_idxs if row.iloc[c]]
        # filtre CA, dates, n°
        texts = [t for t in texts if t not in ("CA","C A") and not looks_date(t) and not looks_order_no(t)]
        if len(texts) >= 1: customer = texts[0]
        if len(texts) >= 2: origin   = texts[1]
        if len(texts) >= 3: destination = texts[2]

        # A) montants déjà présents ?
        amtA = [to_amount(row.iloc[c]) for c in tail_cols]
        if len(amtA) >= 1: revenue = amtA[0]
        if len(amtA) >= 2: cost    = amtA[1]
        if len(amtA) >= 3: margin  = amtA[2]

        used_two = False

        # B) ligne suivante pour date / marge (format du rapport)
        if i + 1 < len(df):
            row2 = df.iloc[i+1]
            # date où qu’elle soit
            for c in range(ncol):
                if looks_date(row2.iloc[c]):
                    req_pu_date = row2.iloc[c]
                    break
            # marge en queue de ligne
            amtB = [to_amount(row2.iloc[c]) for c in tail_cols]
            for a in reversed(amtB):
                if a is not None:
                    if margin is None:
                        margin = a
                    break

            # si on a repéré une date en B, on consomme les deux lignes
            if req_pu_date is not None:
                used_two = True

        i += 2 if used_two else 1

        if order_no:
            out.append({
                "order_no": str(order_no),
                "req_pu_date": req_pu_date,
                "customer": customer,
                "origin": origin,
                "destination": destination,
                "revenue": revenue,
                "cost": cost,
                "margin": margin
            })

    res = pd.DataFrame(out)
    if not res.empty:
        res["req_pu_date"] = pd.to_datetime(res["req_pu_date"], format="%d/%m/%Y", errors="coerce")
    return res

def extract_one(pdf_path: Path) -> pd.DataFrame:
    # on tente lattice puis stream
    dfs = []
    try:
        dfs += tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True, guess=False) or []
    except Exception:
        pass
    try:
        dfs += tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, stream=True, guess=True) or []
    except Exception:
        pass

    cleaned = []
    for d in dfs:
        if d is None or d.empty:
            continue
        d = drop_header_rows(d)
        if not d.empty:
            cleaned.append(d)

    if not cleaned:
        return pd.DataFrame()

    brute = pd.concat(cleaned, ignore_index=True, sort=False)
    stitched = stitch_lines(brute)

    # filtrage final: enlève lignes vides complètes
    if not stitched.empty:
        stitched = stitched[
            stitched[["order_no","customer","origin","destination","revenue","cost","margin"]]
            .notna().any(axis=1)
        ].reset_index(drop=True)

    return stitched

def save_tsv(df: pd.DataFrame, path: Path):
    df.to_csv(path, sep="\t", index=False)

def main():
    pdfs = sorted(p for p in RAW_DIR.glob("*.pdf") if p.is_file())
    if not pdfs:
        print("Aucun PDF dans data/raw/")
        return

    all_rows = []
    for pdf in pdfs:
        print(f"→ {pdf.name}")
        df = extract_one(pdf)
        if df.empty:
            print("   (aucune table exploitable)")
            continue
        out_file = PER_FILE_DIR / f"{pdf.stem}.tsv"
        save_tsv(df, out_file)
        print(f"   ✔ {out_file} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
        df2 = df.copy()
        df2.insert(0, "source_pdf", pdf.name)
        all_rows.append(df2)

    if all_rows:
        master = pd.concat(all_rows, ignore_index=True)
        save_tsv(master, MASTER_PATH)
        print(f"✔ Master: {MASTER_PATH} ({master.shape[0]} lignes)")
    else:
        print("Rien à fusionner.")

if __name__ == "__main__":
    main()

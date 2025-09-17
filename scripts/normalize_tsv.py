#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, math
from pathlib import Path
import pandas as pd
from unidecode import unidecode

ROOT = Path(__file__).resolve().parents[1]
IN_DIR = ROOT / "data" / "processed" / "pdf_csv"

# Schéma cible
OUT_COLS = ["order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"]

def to_float(s):
    if s is None:
        return math.nan
    s = str(s)
    s = re.sub(r"[^\d.\-]", "", s)  # enlève CA, $, espaces, etc.
    try:
        return float(s) if s != "" else math.nan
    except:
        return math.nan

def parse_date_ddmmyyyy(s: str) -> str:
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s or "")
    if not m:
        return ""
    dd, mm, yyyy = m.groups()
    return f"{yyyy}-{mm}-{dd}"

def split_origin_dest_revenue(text: str):
    """
    Reçoit typiquement: 'BOUCHERVILLE,PQ MONTREAL-NORD,PQ 225.00 CA'
    -> ('BOUCHERVILLE,PQ', 'MONTREAL-NORD,PQ', 225.00)
    """
    if not isinstance(text, str):
        return "", "", math.nan
    t = text.strip()

    # récupère le dernier nombre = revenue (s'il existe)
    m_rev = list(re.finditer(r"(\d+(?:\.\d{1,2})?)", t))
    revenue = math.nan
    if m_rev:
        revenue = to_float(m_rev[-1].group(1))
        t = t[:m_rev[-1].start()].strip()

    # coupe en deux villes '...,XX ...,...,YY'
    m = re.match(r"^(.*?,[A-Za-z]{2})\s+(.*?,[A-Za-z]{2})$", t)
    if m:
        return m.group(1).strip(), m.group(2).strip(), revenue
    return "", "", revenue

def normalize_one_file(path: Path):
    # on ignore déjà les fichiers normalisés et enrichis
    if path.name.endswith("_norm.tsv") or path.name.endswith("_enriched.tsv"):
        return False

    # on s'attend à 4 colonnes Tabula (A,B,C,D), en-têtes variables
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    cols = list(df.columns)

    # cas “brut” classique: 4 colonnes type ['Unnamed: 0', 'Unnamed: 1', '(By Requested Pickup date)', 'Unnamed: 4']
    if len(cols) == 4:
        colA, colB, colC, colD = cols
        a = df[colA].fillna("")
        b = df[colB].fillna("")      # customer
        c = df[colC].fillna("")      # origin dest revenue
        d = df[colD].fillna("")      # cost margin

        rows = []
        for a_val, cust, c_val, d_val in zip(a, b, c, d):
            a_txt = str(a_val)

            # Cherche "OrderNo  DD/MM/YYYY"
            m = re.search(r"(\d+)\s+(\d{2}/\d{2}/\d{4})", a_txt)
            if not m:
                # saute les lignes d’entête ("Report Period", etc.)
                continue

            order_no = m.group(1)
            req_date = parse_date_ddmmyyyy(m.group(2))
            customer = unidecode(str(cust)).strip()

            origin, destination, revenue = split_origin_dest_revenue(str(c_val))

            # “130.00 95.00” -> cost, margin
            nums = re.findall(r"(\d+(?:\.\d{1,2})?)", str(d_val))
            cost   = to_float(nums[0]) if len(nums) >= 1 else math.nan
            margin = to_float(nums[1]) if len(nums) >= 2 else math.nan

            rows.append({
                "order_no": order_no,
                "req_pu_date": req_date,
                "customer": customer,
                "origin": origin,
                "destination": destination,
                "revenue": revenue,
                "cost": cost,
                "margin": margin,
            })

        if not rows:
            print(f"⚠️  {path.name}: aucun enregistrement valide détecté (en-têtes ?)")
            return False

        out = pd.DataFrame(rows, columns=OUT_COLS)

    else:
        # Si le fichier a déjà le bon schéma, on l'uniformise seulement
        if all(c in df.columns for c in OUT_COLS):
            out = df[OUT_COLS].copy()
        else:
            print(f"⚠️  {path.name}: format inattendu (colonnes = {cols})")
            return False

    out_name = path.with_name(path.stem + "_norm.tsv")
    out.to_csv(out_name, sep="\t", index=False)
    print(f"✅ Normalisé: {path.name} → {out_name.name} ({out.shape[0]} lignes)")
    return True

def main():
    # On cible uniquement les “ordersYYYY.tsv” (4 chiffres après 'orders')
    files = sorted([p for p in IN_DIR.glob("orders????.tsv")])
    if not files:
        print("Aucun TSV 'ordersYYYY.tsv' à normaliser dans data/processed/pdf_csv/")
        return
    for f in files:
        normalize_one_file(f)

if __name__ == "__main__":
    main()

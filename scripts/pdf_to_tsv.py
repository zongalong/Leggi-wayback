#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PDF -> TSV (robuste) pour les rapports "Activity Report (By Requested Pickup date)".

On évite Tabula: on lit le texte avec pdfminer.six, on regroupe les lignes
par ordre, puis on extrait les champs avec des regex.
Sortie: data/processed/pdf_csv/ordersYYYY.tsv
Colonnes: order_no, req_pu_date, customer, origin, destination, revenue, cost, margin
"""

from __future__ import annotations
from pathlib import Path
import re
import sys
import pandas as pd

try:
    from pdfminer.high_level import extract_text
except Exception as e:
    print("❗ Il faut pdfminer.six (pip install pdfminer.six):", e)
    sys.exit(1)

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/pdf_csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Utilitaires ----------

MONEY = r"(-?\d{1,3}(?:,\d{3})*\.\d{2}|\d+\.\d{2}|\d+)"
CITYPR = r"[A-ZÉÈÀÎÔÂÇ' .-]+,[A-Z]{2}"

# Motif principal: tout sur une seule “ligne logique”
ROW_RE = re.compile(
    rf"""
    ^\s*
    (?P<order>\d{{5}})\s+                              # Order No
    (?P<date>\d{{2}}/\d{{2}}/\d{{4}})\s+               # Req PU (dd/mm/yyyy)
    (?P<customer>[A-Z0-9&' .\-]+?)\s+                  # Customer (non-gourmand)
    (?P<origin>{CITYPR})\s+                            # Origin
    (?P<dest>{CITYPR})\s+                              # Destination
    (?P<rev>{MONEY})\s*(?:CA)?\s+                      # Revenue (optionnel 'CA')
    (?P<cost>{MONEY})\s+                               # Cost
    (?P<margin>{MONEY})\s*                             # Margin
    $""",
    re.VERBOSE,
)

# Certaines lignes sont “cassées” en 2 (client ou origin/destination sur la 2e)
# On regroupe les lignes en blocs: chaque bloc commence par “5 chiffres + espace + date”
START_RE = re.compile(r"^\s*\d{5}\s+\d{2}/\d{2}/\d{4}\b")

HEADER_MARKERS = (
    "ACTIVITY REPORT",
    "REPORT PERIOD",
    "BY REQUESTED PICKUP DATE",
    "ORDER NO",
    "HOME CUR",
)

def is_header_or_blank(line: str) -> bool:
    up = line.upper()
    if not line.strip():
        return True
    return any(k in up for k in HEADER_MARKERS)

def normalize_spaces(s: str) -> str:
    # “CA ” peut coller aux nombres; on garde juste des simples espaces
    s = re.sub(r"\s+", " ", s.strip())
    # nettoie les colonnes "---- Home Cur ----" résiduelles
    s = s.replace(" ---- Home Cur ---- ", " ")
    return s

def ddmmyyyy_to_iso(s: str) -> str:
    # 04/01/2022 -> 2022-01-04
    d, m, y = s.split("/")
    return f"{y}-{m}-{d}"

def money_to_float(s: str) -> float:
    s = s.replace("CA", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else 0.0

# ---------- Parsing d’un PDF ----------

def parse_pdf(path: Path) -> pd.DataFrame:
    text = extract_text(str(path))
    lines = [ln.rstrip() for ln in text.splitlines()]

    # 1) regrouper les “lignes logiques” par ordre
    blocks: list[str] = []
    cur: list[str] = []

    for raw in lines:
        if is_header_or_blank(raw):
            continue
        if START_RE.match(raw):
            # nouveau bloc
            if cur:
                blocks.append(" ".join(normalize_spaces(x) for x in cur))
            cur = [raw]
        else:
            if cur:
                cur.append(raw)
            else:
                # ligne orpheline → ignore
                pass
    if cur:
        blocks.append(" ".join(normalize_spaces(x) for x in cur))

    rows = []
    for blk in blocks:
        s = normalize_spaces(blk)
        m = ROW_RE.match(s)
        if not m:
            # Tentative de réparation fréquente: certaines lignes ont “CA” collée,
            # ou un double espace avant les montants → on assouplit un peu.
            s2 = s.replace(" CA ", " ").replace(" CA", " ")
            m = ROW_RE.match(s2)

        if m:
            order_no    = m.group("order")
            req_pu_date = ddmmyyyy_to_iso(m.group("date"))
            customer    = m.group("customer").strip()
            origin      = m.group("origin").strip()
            dest        = m.group("dest").strip()
            rev         = money_to_float(m.group("rev"))
            cost        = money_to_float(m.group("cost"))
            margin      = money_to_float(m.group("margin"))

            rows.append(
                (order_no, req_pu_date, customer, origin, dest, rev, cost, margin)
            )
        else:
            # Debug minimal en console pour identifier les cas à rajouter
            print(f"⚠️  Ligne non parsée, bloc brut:\n{blk}\n")

    df = pd.DataFrame(
        rows,
        columns=["order_no", "req_pu_date", "customer", "origin", "destination", "revenue", "cost", "margin"],
    )
    return df

def main():
    pdfs = sorted(RAW_DIR.glob("orders*.pdf"))
    if not pdfs:
        print("Aucun PDF trouvé dans data/raw/ (ex: orders2025.pdf)")
        return

    for pdf in pdfs:
        year_match = re.search(r"(\d{4})", pdf.stem)
        year = year_match.group(1) if year_match else "unknown"

        print(f"📄 {pdf.name} → extraction…")
        df = parse_pdf(pdf)
        out = OUT_DIR / f"orders{year}.tsv"
        df.to_csv(out, sep="\t", index=False)
        print(f"✅ {out} ({len(df)} lignes)")

if __name__ == "__main__":
    main()
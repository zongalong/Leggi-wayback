#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PDF → TSV pour les rapports "Activity Report" (ordersYYYY.pdf)

- Entrée : data/raw/orders*.pdf
- Sortie : data/processed/pdf_csv/ordersYYYY.tsv (tab-separated)

Colonnes: order_no, req_pu_date (YYYY-MM-DD), customer, origin, destination, revenue, cost, margin
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Optional, Iterable
import re
import sys
from datetime import datetime
import pandas as pd
import pdfplumber

# ---------------------- Config ----------------------
RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/pdf_csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Tente d'abord l'extraction par "tables" pdfplumber, sinon regex bloc-par-bloc
TABLE_SETTINGS = dict(
    vertical_strategy="lines",
    horizontal_strategy="lines",
    snap_tolerance=3,
    join_tolerance=3,
    text_tolerance=3,
    intersection_tolerance=3,
    keep_blank_chars=True,
)

# ---------------------- Utils ----------------------

MONEY_RE = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+\.[0-9]{2}|0|0\.00)")
DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")
ORDER_RE = re.compile(r"^\s*(\d{5,})\b")  # début de bloc
# version "inline" (order + date) pour détecter démarreurs sur une ligne
ORDER_DATE_INLINE_RE = re.compile(r"^\s*(?P<order>\d{5,})\s+(?:CA\s+)?(?P<date>\d{2}/\d{2}/\d{4})\b")

CITY_RE = r"[A-Z' \-\.\&/]+?,[A-Z]{2}"
# pattern complet (ligne compactée)
FULL_ROW_PATTERNS = [
    re.compile(
        rf"(?P<order>\d{{5,}})\s+(?:CA\s+)?(?P<date>\d{{2}}/\d{{2}}/\d{{4}})\s+"
        rf"(?P<customer>[A-Z0-9 \-\.'&/]+?)\s+"
        rf"(?P<origin>{CITY_RE})\s+(?P<dest>{CITY_RE})\s+"
        rf"(?P<rev>{MONEY_RE.pattern})(?:\s*CA)?\s+(?P<cost>{MONEY_RE.pattern})\s+(?P<margin>{MONEY_RE.pattern})"
    ),
    # Variante : chiffres sans cost/margin (rare)
    re.compile(
        rf"(?P<order>\d{{5,}})\s+(?:CA\s+)?(?P<date>\d{{2}}/\d{{2}}/\d{{4}})\s+"
        rf"(?P<customer>[A-Z0-9 \-\.'&/]+?)\s+"
        rf"(?P<origin>{CITY_RE})\s+(?P<dest>{CITY_RE})\s+"
        rf"(?P<rev>{MONEY_RE.pattern})(?:\s*CA)?"
    ),
]

def clean_money(x: Optional[str]) -> float:
    if x is None:
        return 0.0
    s = x.replace("CA", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def to_iso(date_str: str) -> str:
    # format PDF : DD/MM/YYYY
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return ""

def is_header_row(cells: Iterable[str]) -> bool:
    row = " ".join(cells).upper()
    return ("ORDER" in row and "CUSTOMER" in row) or ("BY REQUESTED PICKUP DATE" in row)

# ------------------ Extraction par tables ------------------

def try_tables(page: pdfplumber.page.Page) -> List[Dict[str, str]]:
    tables = page.extract_tables(TABLE_SETTINGS) or []
    rows: List[Dict[str, str]] = []

    for t in tables:
        # normalise largeur de table
        # on s'attend à quelque chose proche de 7-9 colonnes
        for raw in t:
            cells = [(c or "").strip() for c in raw]
            # ignore lignes vides et entêtes
            if not any(cells):
                continue
            if is_header_row(cells):
                continue

            # Heuristique : essaye de repérer inline "order + date"
            joined = " ".join(cells)
            m_inline = ORDER_DATE_INLINE_RE.search(joined)
            if not m_inline:
                # pas une ligne de data plausible
                continue

            # Map souple : on essaie d’aligner sur 8 colonnes cibles
            # Beaucoup de PDF donne un split en : [order+date, customer, origin, destination, revenue, cost, margin]
            # donc on rebâtit à partir de "joined" si besoin.
            parsed = parse_block(joined)
            if parsed:
                rows.append(parsed)

    return rows

# ------------------ Extraction par blocs/regex ------------------

def blockify(lines: List[str]) -> List[List[str]]:
    """Découpe des lignes textuelles en blocs commençant par un order_no."""
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if ORDER_RE.match(ln):
            # démarre un nouveau bloc
            if cur:
                blocks.append(cur)
            cur = [ln]
        else:
            if cur:
                cur.append(ln)
    if cur:
        blocks.append(cur)
    return blocks

def parse_block(block_text: str) -> Optional[Dict[str, str]]:
    # Essaie plusieurs patterns complets
    for pat in FULL_ROW_PATTERNS:
        m = pat.search(block_text)
        if m:
            d = m.groupdict()
            order_no = d.get("order", "").strip()
            date_iso = to_iso(d.get("date", "").strip())
            customer = (d.get("customer") or "").strip()
            origin = (d.get("origin") or "").strip()
            dest = (d.get("dest") or "").strip()
            rev = clean_money(d.get("rev"))
            cost = clean_money(d.get("cost"))
            margin = clean_money(d.get("margin"))

            # si cost/margin absents dans ce pattern
            if "cost" not in d:
                cost = 0.0
            if "margin" not in d:
                margin = round(rev - cost, 2)

            return {
                "order_no": order_no,
                "req_pu_date": date_iso,
                "customer": customer,
                "origin": origin,
                "destination": dest,
                "revenue": rev,
                "cost": cost,
                "margin": margin,
            }

    # Plan B minimaliste : on essaie de recoller depuis tokens
    # 1) order/date
    mhead = ORDER_DATE_INLINE_RE.search(block_text)
    if not mhead:
        return None
    order_no = mhead.group("order")
    date_iso = to_iso(mhead.group("date"))
    tail = block_text[mhead.end():].strip()

    # 2) money en fin (3 montants)
    money = MONEY_RE.findall(tail)
    rev = cost = margin = 0.0
    if money:
        # on prend les 3 derniers s’ils existent
        last3 = money[-3:]
        if len(last3) == 3:
            rev, cost, margin = [clean_money(x) for x in last3]
        elif len(last3) == 2:
            rev, cost = [clean_money(x) for x in last3]
            margin = round(rev - cost, 2)
        elif len(last3) == 1:
            rev = clean_money(last3[0])
            margin = rev

    # 3) origin/destination (2 villes)
    city_matches = re.findall(CITY_RE, tail)
    origin = city_matches[0].strip() if len(city_matches) >= 1 else ""
    dest = city_matches[1].strip() if len(city_matches) >= 2 else ""

    # 4) customer : ce qu’il y a entre la date et la 1ère ville
    customer = ""
    if origin:
        before_origin = tail.split(origin, 1)[0]
        # nettoie espaces / doubles
        customer = re.sub(r"\s{2,}", " ", before_origin).strip()
        # parfois le customer finit par des bouts de villes coupées → coupe aux majuscules+virgule pattern
        customer = re.sub(r",\s*[A-Z]{2}.*$", "", customer).strip()

    return {
        "order_no": order_no,
        "req_pu_date": date_iso,
        "customer": customer,
        "origin": origin,
        "destination": dest,
        "revenue": rev,
        "cost": cost,
        "margin": margin,
    }

def try_blocks(page: pdfplumber.page.Page) -> List[Dict[str, str]]:
    txt = page.extract_text() or ""
    # supprime l’en-tête / pied de page si présent
    lines = [ln.rstrip() for ln in txt.splitlines() if ln.strip()]
    # élimine les lignes d’entête évidentes
    lines = [ln for ln in lines if "Activity Report" not in ln and "Report Period" not in ln]
    lines = [ln for ln in lines if not re.search(r"----\s*Home Cur\s*----", ln)]

    blocks = blockify(lines)
    out: List[Dict[str, str]] = []
    for b in blocks:
        block_text = " ".join(b)
        parsed = parse_block(block_text)
        if parsed:
            out.append(parsed)
    return out

# ---------------------- Orchestrateur ----------------------

def process_pdf(pdf_path: Path) -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # 1) essai “tables”
            page_rows = try_tables(page)
            if not page_rows:
                # 2) fallback par blocs / regex
                page_rows = try_blocks(page)
            rows.extend(page_rows)

    if not rows:
        return pd.DataFrame(columns=["order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"])

    df = pd.DataFrame(rows)

    # Nettoyage final
    # garde uniquement les lignes plausibles (order_no, date, au moins revenue)
    df = df[df["order_no"].astype(str).str.fullmatch(r"\d{5,}", na=False)]
    df = df[df["req_pu_date"].astype(str).str.len() == 10]

    # types
    for col in ["revenue", "cost", "margin"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # supprime doublons évidents
    df = df.drop_duplicates(subset=["order_no", "req_pu_date", "customer", "origin", "destination"])

    # tri par date puis order
    df = df.sort_values(["req_pu_date", "order_no"]).reset_index(drop=True)
    return df

def main():
    pdfs = sorted(RAW_DIR.glob("orders*.pdf"))
    if not pdfs:
        print(f"⚠️ Aucun PDF trouvé dans {RAW_DIR}")
        sys.exit(0)

    for pdf_path in pdfs:
        print(f"📄 {pdf_path.name} → extraction…")
        df = process_pdf(pdf_path)
        out_tsv = OUT_DIR / f"{pdf_path.stem}.tsv"
        df.to_csv(out_tsv, sep="\t", index=False)
        print(f"✅ {out_tsv} ({len(df)} lignes)")

if __name__ == "__main__":
    main()
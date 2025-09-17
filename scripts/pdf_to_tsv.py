#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extraction Orders PDF -> TSV (sans Java)
- Tente tables pdfplumber
- Fallback parsing texte (regex) si besoin
Sorties : data/processed/pdf_csv/ordersYYYY.tsv

Colonnes: order_no, req_pu_date, customer, origin, destination, revenue, cost, margin
"""

from __future__ import annotations
from pathlib import Path
import re
import sys
import csv
import pandas as pd
import pdfplumber

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/pdf_csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Réglages d'extraction de tables pdfplumber (sans keep_blank_chars)
TABLE_SETTINGS = dict(
    vertical_strategy="lines",
    horizontal_strategy="lines",
    snap_tolerance=3,
    join_tolerance=3,
    text_tolerance=3,
    intersection_tolerance=3,
)

# ---- Utils ----

MONEY_RE = re.compile(r"^\$?\s*([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)(?:\.[0-9]{2})?$")
CLEAN_MONEY_RE = re.compile(r"[^\d\.]")

HDR_RE = re.compile(r"\bOrder\s+No\b", re.I)

LINE_RE = re.compile(
    r"""
    (?P<order>\d{5})\s+
    (?P<date>\d{2}/\d{2}/\d{4})\s+
    (?P<customer>.*?)\s+
    (?P<origin>[A-Z0-9\'\-\.\s,]+,[A-Z]{2})\s+
    (?P<dest>[A-Z0-9\'\-\.\s,]+,[A-Z]{2})\s+
    (?P<rev>\d{1,3}(?:,\d{3})*\.\d{2})\s*(?:CA|\$)?\s+
    (?P<cost>\d{1,3}(?:,\d{3})*\.\d{2})\s*(?:CA|\$)?\s+
    (?P<margin>\d{1,3}(?:,\d{3})*\.\d{2})
    """,
    re.X,
)

def clean_money(x: str | float | int) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("CA", "").replace("$", "")
    s = s.replace("O", "0")  # sécurité OCR
    s = s.replace(" ", "")
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def normalize_date(d: str) -> str:
    d = (d or "").strip()
    # dd/mm/yyyy -> yyyy-mm-dd
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", d)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    return d  # au pire on laisse

def is_header_line(text: str) -> bool:
    return bool(HDR_RE.search(text))

def collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

# ---- Extraction par tables ----

def tables_to_rows(page: pdfplumber.page.Page) -> list[list[str]]:
    rows: list[list[str]] = []
    try:
        tables = page.extract_tables(TABLE_SETTINGS) or []
    except Exception:
        tables = []
    for t in tables:
        for raw_row in t:
            if not raw_row:
                continue
            # Nettoyage des cellules
            row = [collapse_spaces((cell or "").replace("\n", " ")) for cell in raw_row]
            # Ignore les lignes d’en-tête de tableau
            joined = " ".join(row).lower()
            if "order no" in joined and "customer" in joined and "origin" in joined:
                continue
            rows.append(row)
    return rows

def map_row_from_table(row: list[str]) -> dict | None:
    """
    On tente de reconnaître l’ordre des colonnes dans une ligne extraite en table.
    Plusieurs PDF collent "revenue cost margin" dans une seule cellule; on gère ce cas.
    """
    if not row:
        return None
    # prio: trouver le numéro d’ordre & date
    joined = " ".join(row)
    m_order = re.search(r"\b(\d{5})\b", joined)
    m_date = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", joined)
    order = m_order.group(1) if m_order else None
    date = normalize_date(m_date.group(1)) if m_date else ""

    # Heuristique: si on a >=6 cellules, on suppose
    # [order, date, customer, origin, dest, revenue, cost, margin] en séquence,
    # avec parfois [revenue cost margin] collés.
    cells = [c for c in row if c]
    if len(cells) >= 6 and order and date:
        # tente de localiser origin / dest: pattern "...,XX"
        def first_city_idx(start=0):
            for i in range(start, len(cells)):
                if re.search(r",[A-Z]{2}$", cells[i]):
                    return i
            return -1

        cust_start = 0
        # place probable du customer: après l'order et date
        # cherche l'index de la date pour couper
        if m_date:
            # sépare avant/après la date dans la séquence jointe
            pass

        # Stratégie simple: on scanne pour trouver origin puis dest
        oi = first_city_idx(0)
        di = first_city_idx(oi + 1) if oi >= 0 else -1

        origin = cells[oi] if oi >= 0 else ""
        dest = cells[di] if di >= 0 else ""

        # customer = tout ce qui est entre la date (qu'on ignore dans cells) et origin
        # On reconstruit en prenant tout avant origin qui ne ressemble pas à date/order
        # Simplification: customer = cellules avant le premier ",XX"
        customer_parts = []
        for c in cells:
            if c == origin:
                break
            # saute order/date si identiques
            if c == order or c == re.sub("-", "/", date):
                continue
            # ignore rubriques genre "CA"
            customer_parts.append(c)
        customer = collapse_spaces(" ".join(customer_parts))

        # Récup valeurs financières : on regarde les dernières valeurs numériques
        tail = cells[-3:]
        nums = []
        for c in tail:
            # si c est "rev cost margin" collés : découpe
            found = re.findall(r"\d{1,3}(?:,\d{3})*\.\d{2}", c)
            if found:
                nums.extend(found)
        if len(nums) < 3:
            # essaie en scannant toutes les cells de droite à gauche
            for c in reversed(cells):
                found = re.findall(r"\d{1,3}(?:,\d{3})*\.\d{2}", c)
                for f in reversed(found):
                    nums.append(f)
                if len(nums) >= 3:
                    break
        nums = nums[-3:] if nums else ["0.00", "0.00", "0.00"]
        rev, cost, margin = (clean_money(n) for n in nums)

        # sanity minimal
        if order and customer and (origin or dest):
            return dict(
                order_no=order,
                req_pu_date=date,
                customer=customer,
                origin=origin,
                destination=dest,
                revenue=rev,
                cost=cost,
                margin=margin,
            )
    return None

# ---- Extraction en mode texte (fallback) ----

def text_blocks_after_header(page: pdfplumber.page.Page) -> list[str]:
    txt = page.extract_text() or ""
    if not txt.strip():
        return []
    lines = [collapse_spaces(x) for x in txt.splitlines()]
    # coupe avant/à partir de l’en-tête
    start = 0
    for i, line in enumerate(lines):
        if is_header_line(line):
            start = i + 1
            break
    return lines[start:]


def parse_lines_to_rows(lines: list[str]) -> list[dict]:
    """
    Assemble les lignes et applique la regex de détail.
    Certaines PDF coupent les colonnes en plusieurs lignes; on recolle 2-3 lignes.
    """
    out: list[dict] = []
    buf: list[str] = []
    def try_flush():
        joined = " ".join(buf)
        m = LINE_RE.search(joined)
        if m:
            out.append(dict(
                order_no=m.group("order"),
                req_pu_date=normalize_date(m.group("date")),
                customer=collapse_spaces(m.group("customer")),
                origin=collapse_spaces(m.group("origin")),
                destination=collapse_spaces(m.group("dest")),
                revenue=clean_money(m.group("rev")),
                cost=clean_money(m.group("cost")),
                margin=clean_money(m.group("margin")),
            ))
            return True
        return False

    for ln in lines:
        if not ln:
            continue
        buf.append(ln)
        # essaie avec 1, 2, 3 lignes concaténées
        if try_flush():
            buf.clear()
            continue
        if len(buf) > 3:
            # si on n’a pas matché au bout de 4 lignes, on pop la plus vieille
            buf.pop(0)
    # flush final
    try_flush()
    return out

# ---- Pipeline par page ----

def extract_page_records(page: pdfplumber.page.Page) -> list[dict]:
    # 1) tente via tables
    table_rows = tables_to_rows(page)
    mapped: list[dict] = []
    for r in table_rows:
        m = map_row_from_table(r)
        if m:
            mapped.append(m)
    if mapped:
        return mapped

    # 2) fallback : texte
    lines = text_blocks_after_header(page)
    if not lines:
        return []
    return parse_lines_to_rows(lines)

# ---- Fichier entier ----

def process_pdf(pdf_path: Path) -> pd.DataFrame:
    all_rows: list[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for pi, page in enumerate(pdf.pages, start=1):
            recs = extract_page_records(page)
            all_rows.extend(recs)

    df = pd.DataFrame(all_rows, columns=[
        "order_no", "req_pu_date", "customer", "origin",
        "destination", "revenue", "cost", "margin"
    ])

    # Nettoyage final
    df = df.dropna(how="all")
    # filtre évidences fausses : order_no doit être 5 chiffres
    df = df[df["order_no"].astype(str).str.fullmatch(r"\d{5}", na=False)]
    # dates normalisées
    df["req_pu_date"] = df["req_pu_date"].astype(str)

    # types numériques
    for c in ["revenue", "cost", "margin"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df.reset_index(drop=True)

# ---- Main ----

def main():
    pdfs = sorted(RAW_DIR.glob("orders*.pdf"))
    if not pdfs:
        print("⚠️  Aucun PDF trouvé dans data/raw (pattern orders*.pdf)")
        sys.exit(0)

    for pdf_path in pdfs:
        year = re.search(r"(\d{4})", pdf_path.stem)
        y = year.group(1) if year else "unknown"
        print(f"📄 {pdf_path.name} → extraction…")
        try:
            df = process_pdf(pdf_path)
        except Exception as e:
            print(f"❌ Échec {pdf_path.name}: {e}")
            continue

        out_path = OUT_DIR / f"orders{y}.tsv"
        df.to_csv(out_path, sep="\t", index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"✅ {out_path} ({len(df)} lignes)")

if __name__ == "__main__":
    main()
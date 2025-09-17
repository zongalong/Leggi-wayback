#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extraction PDF -> TSV robuste pour les rapports 'Activity Report'.
- Lit tous les PDF dans data/raw/
- Pour chaque page/table, repère la ligne d'en-têtes (Order No, Customer, Origin, Destination, Revenue, Cost, Margin),
  coupe tout ce qui est avant, promeut la ligne comme header, puis normalise.
- Tolère les colonnes fusionnées (Order No + Req P/U) et la colonne 'Origin Destination Revenue' ; reconstruit revenue/cost/margin.

Sorties :
- data/processed/pdf_csv/<nom>.tsv
- data/processed/orders_master.tsv
Dépendances : tabula-py, pandas (Java requis sur runner Actions)
"""

from pathlib import Path
import re
import pandas as pd
import tabula

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
PER_FILE = OUT_DIR / "pdf_csv"
PER_FILE.mkdir(parents=True, exist_ok=True)

MASTER = OUT_DIR / "orders_master.tsv"

# ------------------ utilitaires ------------------

HEADER_KEYS = {"ORDER", "CUSTOMER", "ORIGIN", "DESTINATION"}  # revenue/cost/margin parfois sur 2e ligne
AMOUNT_RE = r"([\d\.,]+)\s*(?:CA)?"

def _norm(x: object) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _row_text(row) -> str:
    return " ".join(_norm(v) for v in row if _norm(v))

def _find_header_idx(df: pd.DataFrame) -> int | None:
    """Repère l'index de la ligne contenant les mots-clés d'en-tête."""
    for i in range(len(df)):
        line = _row_text(df.iloc[i].tolist()).upper()
        if all(k in line for k in HEADER_KEYS):
            return i
    return None

def _to_float(x: str | None) -> float | None:
    if not x:
        return None
    s = x.replace(" CA", "").replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except:
        return None

def _parse_order_and_date(text: str) -> tuple[str | None, str | None]:
    """
    Extrait order_no (>=4 chiffres) + date dd/mm/yyyy depuis la première colonne ou concat.
    """
    if not text:
        return None, None
    m1 = re.search(r"\b(\d{4,})\b", text)
    m2 = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text)
    return (m1.group(1) if m1 else None, m2.group(1) if m2 else None)

def _split_origin_dest(text: str) -> tuple[str | None, str | None]:
    """
    Scinde 'ORIGIN DESTINATION' en deux blocs se terminant par ',PR' (province/état sur 2 lettres).
    """
    if not text:
        return None, None
    # on supprime un montant collé à la fin si présent (Tabula colle parfois '... 225.00 CA')
    text2 = re.sub(rf"\s*{AMOUNT_RE}\s*$", "", text)
    m = re.match(r"^(.+?,[A-Z]{2})\s+(.*,[A-Z]{2})$", text2)
    if m:
        return m.group(1), m.group(2)
    return None, None

def _extract_amounts(*candidates: str) -> tuple[float | None, float | None, float | None]:
    """
    Cherche revenue, cost, margin dans l'ordre en scrutant les colonnes candidates (fin de ligne en priorité).
    """
    joined = " ".join([_norm(c) for c in candidates if c])
    # Cherche 3 montants à la fin (revenue cost margin)
    nums = re.findall(AMOUNT_RE, joined)
    nums = [n for n in nums if n]  # tuples => prendre n
    if not nums:
        return None, None, None
    # Heuristique : on prend les 3 DERNIERS montants de la ligne
    nums = nums[-3:]
    while len(nums) < 3:
        nums.insert(0, None)
    rev, cost, mar = (_to_float(nums[0]), _to_float(nums[1]), _to_float(nums[2]))
    return rev, cost, mar

def _promote_header(df: pd.DataFrame, idx: int) -> pd.DataFrame:
    header = df.iloc[idx].astype(str).tolist()
    cols = []
    seen = {}
    for h in header:
        h1 = _norm(h).lower()
        h1 = re.sub(r"[^a-z0-9 ]", " ", h1)
        h1 = re.sub(r"\s+", "_", h1).strip("_") or "col"
        if h1 in seen:
            seen[h1] += 1
            h1 = f"{h1}_{seen[h1]}"
        else:
            seen[h1] = 1
        cols.append(h1)
    body = df.iloc[idx+1:].reset_index(drop=True)
    body.columns = cols[:body.shape[1]]
    return body

# ------------------ traitement par page ------------------

def extract_one_pdf(pdf_path: Path) -> pd.DataFrame:
    """
    Lit un PDF avec Tabula (stream puis lattice).
    Pour chaque table, coupe avant l'en-tête et normalise les colonnes de sortie :
    order_no, req_pu_date, customer, origin, destination, revenue, cost, margin
    """
    out_rows = []

    # 1) Essai stream, puis lattice
    dfs: list[pd.DataFrame] = []
    try:
        dfs += tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, stream=True, guess=True) or []
    except Exception:
        pass
    try:
        dfs += tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, lattice=True, guess=False) or []
    except Exception:
        pass

    for raw in dfs:
        if raw is None or raw.empty:
            continue
        raw = raw.astype(str).applymap(_norm)
        raw = raw.replace({"None": ""})
        raw = raw.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if raw.empty:
            continue

        # 2) Repère l'en-tête dans CETTE table
        hdr_idx = _find_header_idx(raw)
        if hdr_idx is None:
            # rien d'exploitable ici
            continue

        tbl = _promote_header(raw, hdr_idx)
        if tbl.empty:
            continue

        # 3) Canonique : on crée des champs cibles à partir des colonnes présentes
        for _, row in tbl.iterrows():
            cells = [row.get(c, "") for c in tbl.columns]
            line = " ".join(_norm(c) for c in cells if _norm(c))

            # a) order + date
            order_no, req_date = _parse_order_and_date(" ".join([
                row.get(tbl.columns[0], ""),
                row.get(tbl.columns[1], "")
            ]))

            # b) customer
            # on prend la 1re colonne 'customer' si elle existe, sinon la 2e/3e textuelle
            customer = None
            for cname in tbl.columns[:4]:
                txt = _norm(row.get(cname, ""))
                if "customer" in cname and txt:
                    customer = txt
                    break
            if not customer:
                # fallback : colonne 2 si non numérique
                for cname in tbl.columns[1:3]:
                    txt = _norm(row.get(cname, ""))
                    if txt and not re.search(r"\d{2}/\d{2}/\d{4}", txt):
                        customer = txt
                        break

            # c) origin / destination
            # cherche colonne avec 'origin' ou 'destination', sinon concat des colonnes 2-4
            od = None
            for cname in tbl.columns:
                if "origin" in cname or "destination" in cname:
                    od = _norm(row.get(cname, ""))
                    if od:
                        break
            if not od:
                od = " ".join(_norm(row.get(c, "")) for c in tbl.columns[2:5])
            origin, destination = _split_origin_dest(od)

            # d) montants
            # cherche explicitement les colonnes revenue/cost/margin si nommées, sinon prendre la fin de ligne
            rev = _to_float(row.get("revenue", "")) if "revenue" in tbl.columns else None
            cst = _to_float(row.get("cost", "")) if "cost" in tbl.columns else None
            mar = _to_float(row.get("margin", "")) if "margin" in tbl.columns else None
            if rev is None or cst is None or mar is None:
                r2, c2, m2 = _extract_amounts(*cells[-4:])
                rev = rev if rev is not None else r2
                cst = cst if cst is not None else c2
                mar = mar if mar is not None else m2

            # filtre lignes vides parasites (pieds de page etc.)
            if not any([order_no, customer, origin, destination, rev, cst, mar]):
                continue

            out_rows.append({
                "order_no": order_no,
                "req_pu_date": req_date,
                "customer": customer,
                "origin": origin,
                "destination": destination,
                "revenue": rev,
                "cost": cst,
                "margin": mar,
            })

    return pd.DataFrame(out_rows)

def save_tsv(df: pd.DataFrame, path: Path):
    df.to_csv(path, sep="\t", index=False)

def main():
    pdfs = sorted([p for p in RAW_DIR.glob("*.pdf") if p.is_file()])
    if not pdfs:
        print("Aucun PDF dans data/raw/")
        return

    all_df = []
    for p in pdfs:
        print(f"→ {p.name}")
        df = extract_one_pdf(p)
        if df.empty:
            print("   (aucune table exploitable)")
            continue

        # typage & nettoyage finaux
        df["order_no"] = df["order_no"].astype(str).str.replace(r"\.0+$", "", regex=True)
        df["req_pu_date"] = pd.to_datetime(df["req_pu_date"], format="%d/%m/%Y", errors="coerce")
        for col in ["revenue", "cost", "margin"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        out_file = PER_FILE / f"{p.stem}.tsv"
        save_tsv(df, out_file)
        print(f"   ✔ {out_file} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")

        df2 = df.copy()
        df2.insert(0, "source_pdf", p.name)
        all_df.append(df2)

    if all_df:
        master = pd.concat(all_df, ignore_index=True)
        save_tsv(master, MASTER)
        print(f"✔ Master: {MASTER} ({master.shape[0]} lignes, {master.shape[1]} colonnes)")
    else:
        print("Aucun enregistrement consolidé; master non généré.")

if __name__ == "__main__":
    main()

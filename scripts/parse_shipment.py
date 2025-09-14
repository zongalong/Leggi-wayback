# scripts/parse_shipment.py
from pathlib import Path
import pandas as pd
import csv, re, io

# --- chemins de travail ---
RAW = Path("data/raw/SHIPMENT.TXT")   # ajuste si nom différent
OUTDIR = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT_CSV  = OUTDIR / "master2.csv"
OUT_TSV  = OUTDIR / "master2.tsv"
OUT_MIN  = OUTDIR / "master_minimal.csv"
OUT_SPLIT = OUTDIR / "by_year"
OUT_SPLIT.mkdir(parents=True, exist_ok=True)

ENCODINGS = ["utf-8", "cp1252", "latin-1"]

# ---------------- utilitaires ----------------
def read_text_any(path: Path) -> str:
    """Lit le TXT en essayant plusieurs encodages courants (Windows/UTF-8)."""
    for enc in ENCODINGS:
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return path.read_text(encoding="latin-1", errors="ignore")

NUM_RE = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")
DATEF_RE = re.compile(r"^\s*\d{8}(?:\.\d+)?\s*$")  # ex: 19981103.000000

def is_num(tok: str) -> bool:
    return bool(NUM_RE.match((tok or "").strip()))

def is_date_float(tok: str) -> bool:
    t = (tok or "").strip()
    if not t or t == "0" or t.startswith("0."):
        return False
    return bool(DATEF_RE.match(t))

def take(tokens, i):
    if i < len(tokens):
        return tokens[i].strip(), i + 1
    return "", i + 1

def parse_yyyymmdd_float(s):
    s = (s or "").strip()
    if not s or s == "0" or s.startswith("0."):
        return pd.NaT
    if "." in s:
        s = s.split(".", 1)[0]
    return pd.to_datetime(s, format="%Y%m%d", errors="coerce")

def to_float(x):
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

# ------------- parseur COMMA non-quoté (structure confirmée) -------------
def parse_line_comma(line: str) -> dict:
    """
    Recompose correctement les champs texte qui contiennent des virgules.
    Structure (confirmée par tes exemples) :
      shipment_number, order_date, bill_to_code, from_site_code,
      from_site_name..., to_site_code, to_site_name..., pickup_date,
      delivery_due_date, price, delivered_flag, cost1, cost2, cost3, cost4,
      (champ vide/marge place-holder), service_flag, [éventuel champ vide final]
    """
    tks = line.rstrip("\n").split(",")
    i = 0

    shipment_no, i     = take(tks, i)
    order_date_raw, i  = take(tks, i)
    bill_to_code, i    = take(tks, i)
    from_site_code, i  = take(tks, i)

    # from_site_name : concatène jusqu'au prochain "numérique" (to_site_code)
    parts = []
    while i < len(tks) and not is_num(tks[i]):
        parts.append(tks[i]); i += 1
    from_site_name = ",".join(p.strip() for p in parts)

    to_site_code, i = take(tks, i)  # numérique attendu

    # to_site_name : concatène jusqu'à rencontrer une "date float" (pickup)
    parts = []
    while i < len(tks) and not is_date_float(tks[i]):
        parts.append(tks[i]); i += 1
    to_site_name = ",".join(p.strip() for p in parts)

    pickup_date_raw, i       = take(tks, i)
    delivery_due_date_raw, i = take(tks, i)
    price_raw, i             = take(tks, i)
    delivered_flag, i        = take(tks, i)
    cost1_raw, i             = take(tks, i)
    cost2_raw, i             = take(tks, i)
    cost3_raw, i             = take(tks, i)
    cost4_raw, i             = take(tks, i)
    _margin_placeholder, i   = take(tks, i)   # souvent vide/0
    service_flag, i          = take(tks, i)

    # s'il reste un dernier token vide dû à une virgule finale, on l'ignore

    return {
        "shipment_number": shipment_no,
        "order_date_raw": order_date_raw,
        "bill_to_code": bill_to_code,
        "from_site_code": from_site_code,
        "from_site_name": from_site_name,
        "to_site_code": to_site_code,
        "to_site_name": to_site_name,
        "pickup_date_raw": pickup_date_raw,
        "delivery_due_date_raw": delivery_due_date_raw,
        "price": price_raw,
        "delivered_flag": delivered_flag,
        "cost1": cost1_raw,
        "cost2": cost2_raw,
        "cost3": cost3_raw,
        "cost4": cost4_raw,
        "service_flag": service_flag,
    }

# -------------------------- pipeline principal --------------------------
def main():
    if not RAW.exists():
        raise FileNotFoundError(f"Introuvable: {RAW}. Vérifie le chemin exact.")

    txt = read_text_any(RAW)
    first = txt.splitlines()[0] if txt else ""

    # 1) Cas idéal : export TAB -> lecture directe
    if "\t" in first:
        df = pd.read_csv(
            io.StringIO(txt),
            sep="\t",
            dtype=str,
            engine="python",
            header=0 if any(ch.isalpha() for ch in first) else None,
            on_bad_lines="skip",
        )
        # si pas de header réel
        if not any(str(c).strip() for c in df.columns):
            df = pd.read_csv(io.StringIO(txt), sep="\t", dtype=str, header=None, engine="python")
            df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
    else:
        # 2) Ancien export COMMA non-quoté -> reconstruction
        lines = [ln for ln in txt.splitlines() if ln.strip()]
        rows = [parse_line_comma(ln) for ln in lines]
        df = pd.DataFrame(rows)

        # conversions typées utiles
        for dcol in ["order_date_raw","pickup_date_raw","delivery_due_date_raw"]:
            df[dcol.replace("_raw","")] = df[dcol].apply(parse_yyyymmdd_float)
        for c in ["price","cost1","cost2","cost3","cost4"]:
            df[c] = df[c].apply(to_float)
        # coût par défaut (ajuste si besoin)
        df["cost"] = df["cost1"]
        df["margin"] = (df["price"] - df["cost"]).where(df["price"].notna() & df["cost"].notna())

    # nettoyage
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

    # ordre conseillé des colonnes si elles existent
    preferred = [
        "order_date","shipment_number",
        "bill_to_code",
        "from_site_code","from_site_name",
        "to_site_code","to_site_name",
        "pickup_date","delivery_due_date",
        "price","cost","cost1","cost2","cost3","cost4",
        "delivered_flag","service_flag",
        "margin",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    # ------------- sorties principales -------------
    df.to_csv(OUT_CSV, index=False, quoting=csv.QUOTE_MINIMAL)
    df.to_csv(OUT_TSV, index=False, sep="\t")

    # master minimal pour analyse / GPT
    master = pd.DataFrame({
        "date": df.get("order_date"),
        "order_no": df.get("shipment_number"),
        "customer": df.get("from_site_name"),
        "origin": df.get("from_site_name"),
        "destination": df.get("to_site_name"),
        "revenue": pd.to_numeric(df.get("price"), errors="coerce"),
        "cost": pd.to_numeric(df.get("cost"), errors="coerce"),
    })
    master["margin"] = master["revenue"] - master["cost"]
    master.to_csv(OUT_MIN, index=False)

    # ------------- split annuel -------------
    if "order_date" in df.columns:
        dty = pd.to_datetime(df["order_date"], errors="coerce")
        df_year = df.assign(year=dty.dt.year)
        for year, sub in df_year.groupby("year"):
            if pd.isna(year):
                continue
            out_path = OUT_SPLIT / f"master2_{int(year)}.csv"
            sub.drop(columns=["year"]).to_csv(out_path, index=False)
            print(f"  → {out_path} ({sub.shape[0]} lignes)")

    print(f"✅ Export OK : {OUT_CSV}  ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
    print(f"✅ TSV OK    : {OUT_TSV}")
    print(f"✅ Minimal   : {OUT_MIN}")
    print(f"✅ Split     : {OUT_SPLIT}")

if __name__ == "__main__":
    main()

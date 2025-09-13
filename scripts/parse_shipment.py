# scripts/parse_shipment.py
import re
import pandas as pd
from pathlib import Path

RAW = Path("data/raw/SHIPMENT.TXT")
OUTDIR = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "master.csv"

ENCODINGS_TO_TRY = ["utf-8", "latin-1", "cp1252"]

def read_lines_any_enc(path: Path):
    last_err = None
    for enc in ENCODINGS_TO_TRY:
        try:
            txt = path.read_text(encoding=enc, errors="strict")
            print(f"✓ Lecture SHIPMENT.TXT avec encodage: {enc}")
            return txt.splitlines()
        except Exception as e:
            last_err = e
            print(f"✗ Erreur encodage {enc}: {e}")
    # fallback permissif
    print("! Fallback permissif (latin-1, ignore)")
    return path.read_text(encoding="latin-1", errors="ignore").splitlines()

NUM_RE = re.compile(r"^\s*-?\d+(?:\.\d+)?\s*$")
DATEF_RE = re.compile(r"^\s*\d{8}\.?\d*\s*$")  # 19981103.000000

def is_num(tok: str) -> bool:
    return bool(NUM_RE.match(tok))

def is_date_float(tok: str) -> bool:
    # YYYYMMDD(.xxxxxx) ; 0 ou vide -> pas une date utile
    tok = tok.strip()
    if not tok or tok == "0" or tok.startswith("0."):
        return False
    return bool(DATEF_RE.match(tok))

def take(tokens, i):
    # renvoie tokens[i] (ou None) + nouvel index i+1
    if i < len(tokens):
        return tokens[i].strip(), i + 1
    return "", i + 1

def parse_line(line: str):
    # split brut sur virgule, puis recompose les champs texte
    tks = [t for t in line.rstrip("\n").split(",")]
    i = 0

    shipment_no, i = take(tks, i)
    order_date_raw, i = take(tks, i)

    from_site_code, i = take(tks, i)

    # from_site_name = agrège jusqu'à rencontrer un code numérique (to_site_code)
    name_parts = []
    while i < len(tks):
        peek = tks[i]
        if is_num(peek):  # on considère que le prochain nombre = to_site_code
            break
        name_parts.append(peek)
        i += 1
    from_site_name = ",".join(p.strip() for p in name_parts).strip()

    to_site_code, i = take(tks, i)

    # to_site_name = agrège jusqu'à rencontrer une date (pickup_date)
    name_parts = []
    while i < len(tks):
        peek = tks[i]
        if is_date_float(peek):
            break
        name_parts.append(peek)
        i += 1
    to_site_name = ",".join(p.strip() for p in name_parts).strip()

    pickup_date_raw, i = take(tks, i)
    delivery_due_date_raw, i = take(tks, i)

    price_raw, i = take(tks, i)
    delivered_flag, i = take(tks, i)

    cost1_raw, i = take(tks, i)
    cost2_raw, i = take(tks, i)
    cost3_raw, i = take(tks, i)
    cost4_raw, i = take(tks, i)

    margin_placeholder, i = take(tks, i)
    service_flag, i = take(tks, i)

    return {
        "shipment_number": shipment_no,
        "order_date": order_date_raw,
        "from_site": from_site_code,
        "from_site_name": from_site_name,
        "to_site": to_site_code,
        "to_site_name": to_site_name,
        "pickup_date": pickup_date_raw,
        "delivery_due_date": delivery_due_date_raw,
        "price": price_raw,
        "delivered_flag": delivered_flag,
        "cost1": cost1_raw,
        "cost2": cost2_raw,
        "cost3": cost3_raw,
        "cost4": cost4_raw,
        "margin_placeholder": margin_placeholder,
        "service_flag": service_flag,
    }

def parse_yyyymmdd_float(s):
    # '19981103.000000' -> datetime ; '0.000000' ou vide -> NaT
    s = (s or "").strip()
    if not s or s == "0" or s.startswith("0."):
        return pd.NaT
    if "." in s:
        s = s.split(".", 1)[0]
    if len(s) != 8:
        return pd.NaT
    return pd.to_datetime(s, format="%Y%m%d", errors="coerce")

def to_float(x):
    x = (x or "").strip()
    if not x:
        return None
    try:
        return float(x.replace(" ", ""))
    except Exception:
        return None

# --- Lecture et parsing ligne à ligne
lines = read_lines_any_enc(RAW)
records = []
for ln in lines:
    if not ln.strip():
        continue
    try:
        rec = parse_line(ln)
        records.append(rec)
    except Exception as e:
        # On log l'erreur et on continue (pour ne pas bloquer tout le fichier)
        print(f"! Ligne ignorée (parse error): {e}\n{ln}")

df = pd.DataFrame.from_records(records)

# Conversions de types
for col in ["price", "cost1", "cost2", "cost3", "cost4"]:
    if col in df.columns:
        df[col] = df[col].apply(to_float)

for dcol in ["order_date", "pickup_date", "delivery_due_date"]:
    if dcol in df.columns:
        df[dcol] = df[dcol].apply(parse_yyyymmdd_float)

# Choix du coût réel (par défaut cost1 ; à ajuster si nécessaire)
cost_col = "cost1" if "cost1" in df.columns else None

# Construction du master minimal et normalisé
master = pd.DataFrame()
master["date"] = df.get("order_date")
master["order_no"] = df.get("shipment_number")
# Client facturé : si tu préfères l'autre extrémité, inverse les deux lignes ci-dessous
master["customer"] = df.get("from_site_name")  # ex: expéditeur
master["origin"] = df.get("from_site_name")
master["destination"] = df.get("to_site_name")
master["revenue"] = pd.to_numeric(df.get("price"), errors="coerce")

if cost_col:
    master["cost"] = pd.to_numeric(df.get(cost_col), errors="coerce")
else:
    master["cost"] = pd.NA

master["margin"] = master["revenue"] - master["cost"]

# Nettoyage final
master = master.dropna(subset=["order_no", "revenue"], how="all")
master = master.sort_values("date")

master.to_csv(OUT, index=False)
print(f"OK -> {OUT} ({len(master)} lignes)")

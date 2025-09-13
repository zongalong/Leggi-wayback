# scripts/parse_shipment.py
import pandas as pd
from pathlib import Path

RAW = Path("data/raw/SHIPMENT.TXT")
OUTDIR = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT = OUTDIR / "master.csv"

# Lis une ligne pour estimer le nombre de colonnes
sample = RAW.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
n_cols = len(sample.split(","))

# Noms provisoires en fonction de ton export montré (adapter si besoin)
# (tu peux élargir/réduire la liste mais elle doit avoir n_cols éléments)
proposed_cols = [
    "shipment_number", "order_date",
    "from_site", "to_site",
    "from_site_name", "to_site_name",
    "pickup_date", "delivery_due_date",
    "price", "delivered_flag",
    "cost1","cost2","cost3","cost4",
    "margin_placeholder","service_flag"
]

# Si l'export a plus/moins de colonnes, complète par col_XX
if n_cols > len(proposed_cols):
    proposed_cols += [f"col_{i}" for i in range(len(proposed_cols), n_cols)]
elif n_cols < len(proposed_cols):
    proposed_cols = proposed_cols[:n_cols]

# Charge
df = pd.read_csv(RAW, header=None, names=proposed_cols, dtype=str)

def parse_yyyymmdd_float(s):
    """ '19981103.000000' -> '1998-11-03' ; gère vides """
    if pd.isna(s) or not str(s).strip():
        return pd.NaT
    s = str(s).strip()
    if "." in s: s = s.split(".", 1)[0]
    if len(s) < 8: return pd.NaT
    return pd.to_datetime(s[:8], format="%Y%m%d", errors="coerce")

for col in ["order_date","pickup_date","delivery_due_date"]:
    if col in df.columns:
        df[col] = df[col].apply(parse_yyyymmdd_float)

# Montants
for col in ["price","cost1","cost2","cost3","cost4"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col].str.replace(",", "").str.strip(), errors="coerce")

# Choisis le coût réel (ajuste ici si ce n'est pas cost1)
cost_col = "cost1" if "cost1" in df.columns else None

# Construit le master minimal
cols = {
    "date": "order_date",
    "order_no": "shipment_number",
    "customer": "from_site_name",      # <- côté 'facturé par' si c'est mieux, mets 'bill_to_customer'
    "origin": "from_site_name",
    "destination": "to_site_name",
    "revenue": "price",
}
master = pd.DataFrame()
for new, old in cols.items():
    if old in df.columns:
        master[new] = df[old]
    else:
        master[new] = pd.NA

if cost_col:
    master["cost"] = df[cost_col]
else:
    master["cost"] = pd.NA

# Types
master["date"] = pd.to_datetime(master["date"], errors="coerce")
master["revenue"] = pd.to_numeric(master["revenue"], errors="coerce")
master["cost"] = pd.to_numeric(master["cost"], errors="coerce")

# Marge
master["margin"] = master["revenue"] - master["cost"]

# Trie, enlève lignes vides
master = master.sort_values("date").dropna(subset=["order_no","revenue"], how="all")

master.to_csv(OUT, index=False)
print(f"OK -> {OUT} ({len(master)} lignes)")

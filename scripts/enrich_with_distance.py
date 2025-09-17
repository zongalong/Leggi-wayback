#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, time, math, requests, pandas as pd
from pathlib import Path
from haversine import haversine
from unidecode import unidecode

# --- Chemins
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "processed" / "pdf_csv"
GEO_DIR = ROOT / "data" / "processed" / "geo"
GEO_DIR.mkdir(parents=True, exist_ok=True)

# --- Caches
LOC_CACHE = GEO_DIR / "locations.csv"   # location, norm, lat, lon, country
DIST_CACHE = GEO_DIR / "distances.csv"  # origin_norm, dest_norm, distance_km, method

# --- Distance (OpenRouteService en option, sinon haversine * 1.2)
ORS_API_KEY = os.getenv("ORS_API_KEY")
ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

PROV_STATE_TO_COUNTRY = {
    # Canada
    "AB":"Canada","BC":"Canada","MB":"Canada","NB":"Canada","NL":"Canada","NS":"Canada",
    "NT":"Canada","NU":"Canada","ON":"Canada","PE":"Canada","QC":"Canada","SK":"Canada","YT":"Canada",
    "PQ":"Canada",  # alias affichage ancien
    # USA
    "AL":"USA","AK":"USA","AZ":"USA","AR":"USA","CA":"USA","CO":"USA","CT":"USA","DE":"USA","FL":"USA",
    "GA":"USA","HI":"USA","ID":"USA","IL":"USA","IN":"USA","IA":"USA","KS":"USA","KY":"USA","LA":"USA",
    "ME":"USA","MD":"USA","MA":"USA","MI":"USA","MN":"USA","MS":"USA","MO":"USA","MT":"USA","NE":"USA",
    "NV":"USA","NH":"USA","NJ":"USA","NM":"USA","NY":"USA","NC":"USA","ND":"USA","OH":"USA","OK":"USA",
    "OR":"USA","PA":"USA","RI":"USA","SC":"USA","SD":"USA","TN":"USA","TX":"USA","UT":"USA","VT":"USA",
    "VA":"USA","WA":"USA","WV":"USA","WI":"USA","WY":"USA",
}

# ---------------------------
# Utilitaires de normalisation
# ---------------------------

def normalize_loc(raw: str):
    """Normalise 'VILLE,XX' (+pays) -> clef 'norm' stable, plus infos."""
    if not isinstance(raw, str) or not raw.strip():
        return {"norm":"", "city":"", "region":"", "country":""}
    txt = unidecode(raw.strip())
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})$", txt)
    if not m:
        city = txt
        return {"norm": city.lower(), "city": city.lower(), "region":"", "country":""}
    city, code = m.group(1).strip(), m.group(2).upper()
    if code == "PQ":
        code = "QC"
    country = PROV_STATE_TO_COUNTRY.get(code, "")
    norm = f"{city},{code}".lower()
    if country:
        norm = f"{city},{code},{country}".lower()
    return {"norm": norm, "city": city.lower(), "region": code, "country": country}

def parse_date_ddmmyyyy(s: str) -> str:
    """Transforme 'DD/MM/YYYY' -> 'YYYY-MM-DD' (retourne '' si KO)."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s or "")
    if not m:
        return ""
    dd, mm, yyyy = m.groups()
    return f"{yyyy}-{mm}-{dd}"

def to_float(s: str):
    if s is None:
        return math.nan
    s = str(s).strip()
    # retire monnaie éventuelle
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s) if s != "" else math.nan
    except:
        return math.nan

def split_origin_destination(text: str):
    """
    Extrait origin et destination d'un champ de type:
      'BOUCHERVILLE,PQ MONTREAL-NORD,PQ 225.00 CA'
    On enlève la partie revenue (dernier float), puis on découpe:
      ^(.*?,[A-Z]{2})\s+(.*?,[A-Z]{2})$
    """
    if not isinstance(text, str):
        return "", "", math.nan
    t = text.strip()

    # Récupère revenue (dernier float dans la chaîne)
    m_rev = list(re.finditer(r"(\d+(?:\.\d{1,2})?)", t))
    revenue = math.nan
    if m_rev:
        revenue = to_float(m_rev[-1].group(1))
        t = t[:m_rev[-1].start()].strip()  # on retire la partie chiffrée finale

    # Essaye de séparer origin et destination
    m = re.match(r"^(.*?,[A-Za-z]{2})\s+(.*?,[A-Za-z]{2})$", t)
    if m:
        origin = m.group(1).strip()
        destination = m.group(2).strip()
        return origin, destination, revenue

    # fallback : on ne sait pas séparer
    return "", "", revenue

def try_normalize_tabula4(df: pd.DataFrame) -> pd.DataFrame:
    """
    Patch rapide : si on reçoit un TSV 'brut' Tabula avec:
      ['Unnamed: 0', 'Unnamed: 1', '(By Requested Pickup date)', 'Unnamed: 4']
    on le convertit en DataFrame normalisée.
    - Col A: 'Order No Req P/U' -> '12345 04/01/2024'  -> order_no + date
    - Col B: 'Customer'
    - Col C: 'Origin Destination Revenue'            -> origin, destination, revenue
    - Col D: 'Cost Margin'                           -> cost, margin
    """
    cols = list(df.columns)
    ugly = {"Unnamed: 0", "Unnamed: 1", "(By Requested Pickup date)", "Unnamed: 4"}
    if not (set(cols) <= ugly and "(By Requested Pickup date)" in cols):
        return df  # rien à faire

    a = df.get("Unnamed: 0", pd.Series(dtype=object)).fillna("")
    b = df.get("Unnamed: 1", pd.Series(dtype=object)).fillna("")
    c = df.get("(By Requested Pickup date)", pd.Series(dtype=object)).fillna("")
    d = df.get("Unnamed: 4", pd.Series(dtype=object)).fillna("")

    out_rows = []
    for a_val, cust, c_val, d_val in zip(a, b, c, d):
        a_txt = str(a_val)

        # Order & date
        m = re.search(r"(\d+)\s+(\d{2}/\d{2}/\d{4})", a_txt)
        if not m:
            # saute en-têtes, 'Report Period', lignes vides…
            continue
        order_no = m.group(1)
        req_date = parse_date_ddmmyyyy(m.group(2))

        # Customer
        customer = str(cust).strip()

        # Origin / Destination / Revenue (depuis la colonne C)
        origin, destination, revenue = split_origin_destination(str(c_val))

        # Cost / Margin (colonne D -> “130.00 95.00”)
        d_nums = re.findall(r"(\d+(?:\.\d{1,2})?)", str(d_val))
        cost = to_float(d_nums[0]) if len(d_nums) >= 1 else math.nan
        margin = to_float(d_nums[1]) if len(d_nums) >= 2 else math.nan

        out_rows.append({
            "order_no": order_no,
            "req_pu_date": req_date,
            "customer": customer,
            "origin": origin,
            "destination": destination,
            "revenue": revenue,
            "cost": cost,
            "margin": margin,
        })

    if not out_rows:
        return df  # rien d'exploitable, on laisse tel quel (le check plus bas lèvera)
    norm = pd.DataFrame(out_rows, columns=[
        "order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"
    ])
    print(f"⚠️  Colonnes Tabula '4-col' normalisées automatiquement → {norm.shape[0]} lignes")
    return norm

# ---------------------------
# Géocodage / distance
# ---------------------------

def load_csv(path, cols):
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame(columns=cols)

def save_csv(df, path, subset):
    df.drop_duplicates(subset=subset, inplace=True)
    df.to_csv(path, index=False)

def geocode(norm: str, country_hint: str, session: requests.Session):
    # D'abord ORS si clé fournie
    if ORS_API_KEY:
        url = "https://api.openrouteservice.org/geocode/search"
        params = {"api_key": ORS_API_KEY, "text": norm}
        if country_hint:
            params["boundary.country"] = "CA" if country_hint=="Canada" else "US"
        r = session.get(url, params=params, timeout=20)
        if r.ok:
            js = r.json()
            feats = js.get("features", [])
            if feats:
                lon, lat = feats[0]["geometry"]["coordinates"]
                return (float(lat), float(lon))
    # Sinon Nominatim
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": norm, "format":"json", "limit":1}
    r = session.get(url, params=params, headers={"User-Agent":"leggi-distance/1.0"}, timeout=20)
    if r.ok:
        arr = r.json()
        if arr:
            return (float(arr[0]["lat"]), float(arr[0]["lon"]))
    return None

def get_coords(location_str: str, loc_cache: pd.DataFrame, session: requests.Session):
    info = normalize_loc(location_str)
    norm = info["norm"]
    if not norm:
        return None, loc_cache
    row = loc_cache.loc[loc_cache["norm"] == norm]
    if not row.empty:
        return (row.iloc[0]["lat"], row.iloc[0]["lon"]), loc_cache
    coords = geocode(norm, info["country"], session)
    if coords:
        new = {"location": location_str, "norm": norm, "lat": coords[0], "lon": coords[1], "country": info["country"]}
        loc_cache = pd.concat([loc_cache, pd.DataFrame([new])], ignore_index=True)
        time.sleep(0.3)  # petit throttle
        return coords, loc_cache
    return None, loc_cache

def ors_distance_km(a_latlon, b_latlon, session: requests.Session):
    if not ORS_API_KEY:
        return None
    body = {"coordinates": [[a_latlon[1], a_latlon[0]], [b_latlon[1], b_latlon[0]]], "units": "km"}
    r = session.post(ORS_URL, headers={"Authorization": ORS_API_KEY, "Content-Type":"application/json"},
                     data=json.dumps(body), timeout=30)
    if r.ok:
        try:
            return float(r.json()["routes"][0]["summary"]["distance"])
        except Exception:
            return None
    return None

def pair_distance(origin, dest, loc_cache, dist_cache, session):
    oinfo = normalize_loc(origin); dinfo = normalize_loc(dest)
    on, dn = oinfo["norm"], dinfo["norm"]
    if not on or not dn:
        return None, loc_cache, dist_cache

    row = dist_cache.loc[(dist_cache["origin_norm"]==on) & (dist_cache["dest_norm"]==dn)]
    if not row.empty:
        return float(row.iloc[0]["distance_km"]), loc_cache, dist_cache

    ocoords, loc_cache = get_coords(origin, loc_cache, session)
    dcoords, loc_cache = get_coords(dest, loc_cache, session)
    if not ocoords or not dcoords:
        return None, loc_cache, dist_cache

    dist_km = ors_distance_km(ocoords, dcoords, session)
    method = "ors" if dist_km is not None else "haversine_x1.2"
    if dist_km is None:
        dist_km = haversine(tuple(ocoords), tuple(dcoords)) * 1.2

    new = {"origin_norm": on, "dest_norm": dn, "distance_km": dist_km, "method": method}
    dist_cache = pd.concat([dist_cache, pd.DataFrame([new])], ignore_index=True)
    return dist_km, loc_cache, dist_cache

# ---------------------------
# Enrichissement principal
# ---------------------------

def safe_ratio(a, b):
    try:
        a = float(a); b = float(b)
        return round(a/b, 4) if b and b>0 else math.nan
    except:
        return math.nan

def enrich_file(tsv_path: Path):
    # Lecture TSV
    df = pd.read_csv(tsv_path, sep="\t", dtype=str, keep_default_na=False)

    # Patch rapide: si c'est le format 4-col Tabula, on normalise
    df = try_normalize_tabula4(df)

    # Convertit les types importants
    # (Si déjà normalisé, ces colonnes existent; sinon l'erreur ci-dessous dira ce qui manque)
    required = ["order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{tsv_path.name}: colonnes manquantes {missing}\nColonnes lues: {list(df.columns)}")

    # Cast numériques
    for col in ["revenue","cost","margin"]:
        df[col] = [to_float(x) for x in df[col]]

    # Préparation caches
    loc_cache = load_csv(LOC_CACHE, ["location","norm","lat","lon","country"])
    dist_cache = load_csv(DIST_CACHE, ["origin_norm","dest_norm","distance_km","method"])
    session = requests.Session()

    # Distances
    distances = []
    for _, row in df.iterrows():
        o, d = row.get("origin","").strip(), row.get("destination","").strip()
        if not o or not d:
            distances.append(math.nan); continue
        dkm, loc_cache, dist_cache = pair_distance(o, d, loc_cache, dist_cache, session)
        distances.append(dkm if dkm is not None else math.nan)

    df["distance_km"] = distances
    df["revenue_per_km"] = [safe_ratio(r, k) for r,k in zip(df["revenue"], df["distance_km"])]
    df["cost_per_km"]     = [safe_ratio(c, k) for c,k in zip(df["cost"], df["distance_km"])]
    margins_num = pd.to_numeric(df["revenue"], errors="coerce") - pd.to_numeric(df["cost"], errors="coerce")
    df["margin_per_km"]   = [safe_ratio(m, k) for m,k in zip(margins_num, df["distance_km"])]

    # Sauve caches + fichier enrichi
    save_csv(loc_cache, LOC_CACHE, ["norm"])
    save_csv(dist_cache, DIST_CACHE, ["origin_norm","dest_norm"])

    out = tsv_path.with_name(tsv_path.stem + "_enriched.tsv")
    df.to_csv(out, sep="\t", index=False)
    print(f"✅ Enrichi: {tsv_path.name} → {out.name} ({df.shape[0]} lignes)")

def main():
    # Ne traiter que les TSV "bruts", pas les *_enriched.tsv
    files = sorted([p for p in RAW_DIR.glob("orders*.tsv") if not p.name.endswith("_enriched.tsv")])
    if not files:
        print("Aucun TSV brut dans data/processed/pdf_csv/")
        return
    for f in files:
        print(f"▶ Traitement {f.name}")
        enrich_file(f)

if __name__ == "__main__":
    main()

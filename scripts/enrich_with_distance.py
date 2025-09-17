#!/usr/bin/env python3
import os, re, json, time, math, requests, pandas as pd
from pathlib import Path
from haversine import haversine
from unidecode import unidecode

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "processed" / "pdf_csv"
GEO_DIR = ROOT / "data" / "processed" / "geo"
GEO_DIR.mkdir(parents=True, exist_ok=True)

LOC_CACHE = GEO_DIR / "locations.csv"   # location, norm, lat, lon, country
DIST_CACHE = GEO_DIR / "distances.csv"  # origin_norm, dest_norm, distance_km, method

ORS_API_KEY = os.getenv("ORS_API_KEY")  # (optionnel) clé OpenRouteService
ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

# Provinces/États -> pays (inclut PQ->QC)
PROV_STATE_TO_COUNTRY = {
    # Canada
    "AB":"Canada","BC":"Canada","MB":"Canada","NB":"Canada","NL":"Canada","NS":"Canada",
    "NT":"Canada","NU":"Canada","ON":"Canada","PE":"Canada","QC":"Canada","SK":"Canada","YT":"Canada",
    "PQ":"Canada",  # ancien code -> QC
    # USA (courants)
    "AL":"USA","AK":"USA","AZ":"USA","AR":"USA","CA":"USA","CO":"USA","CT":"USA","DE":"USA","FL":"USA",
    "GA":"USA","HI":"USA","ID":"USA","IL":"USA","IN":"USA","IA":"USA","KS":"USA","KY":"USA","LA":"USA",
    "ME":"USA","MD":"USA","MA":"USA","MI":"USA","MN":"USA","MS":"USA","MO":"USA","MT":"USA","NE":"USA",
    "NV":"USA","NH":"USA","NJ":"USA","NM":"USA","NY":"USA","NC":"USA","ND":"USA","OH":"USA","OK":"USA",
    "OR":"USA","PA":"USA","RI":"USA","SC":"USA","SD":"USA","TN":"USA","TX":"USA","UT":"USA","VT":"USA",
    "VA":"USA","WA":"USA","WV":"USA","WI":"USA","WY":"USA",
}

def normalize_loc(raw: str):
    """'MONTREAL-NORD,PQ' -> dict with norm 'montreal-nord,qc,canada'"""
    if not isinstance(raw, str) or not raw.strip():
        return {"norm":"", "city":"", "region":"", "country":""}
    txt = unidecode(raw.strip())
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})$", txt)
    if not m:
        city = txt
        return {"norm": city.lower(), "city": city.lower(), "region":"", "country":""}
    city, code = m.group(1).strip(), m.group(2).upper()
    if code == "PQ":  # ancien code Québec
        code = "QC"
    country = PROV_STATE_TO_COUNTRY.get(code, "")
    norm = f"{city},{code}".lower()
    if country:
        norm = f"{city},{code},{country}".lower()
    return {"norm": norm, "city": city.lower(), "region": code, "country": country}

def load_csv(path, cols):
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame(columns=cols)

def save_csv(df, path, subset):
    df.drop_duplicates(subset=subset, inplace=True)
    df.to_csv(path, index=False)

def geocode(norm: str, country_hint: str, session: requests.Session):
    # 1) OpenRouteService geocode (si API key présente)
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
    # 2) Fallback Nominatim (public)
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
        time.sleep(0.3)
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

def enrich_file(tsv_path: Path):
    df = pd.read_csv(tsv_path, sep="\t", dtype={"order_no":str}, keep_default_na=False)
    req = ["order_no","req_pu_date","customer","origin","destination","revenue","cost","margin"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"{tsv_path.name}: colonnes manquantes {missing}")

    loc_cache = load_csv(LOC_CACHE, ["location","norm","lat","lon","country"])
    dist_cache = load_csv(DIST_CACHE, ["origin_norm","dest_norm","distance_km","method"])
    session = requests.Session()

    distances = []
    for _, row in df.iterrows():
        o, d = row.get("origin","").strip(), row.get("destination","").strip()
        if not o or not d:
            distances.append(math.nan)
            continue
        dkm, loc_cache, dist_cache = pair_distance(o, d, loc_cache, dist_cache, session)
        distances.append(dkm if dkm is not None else math.nan)

    df["distance_km"] = distances

    def safe_ratio(a, b):
        try:
            a = float(a); b = float(b)
            return round(a/b, 4) if b and b>0 else math.nan
        except:
            return math.nan

    df["revenue_per_km"] = [safe_ratio(r, k) for r,k in zip(df["revenue"], df["distance_km"])]
    df["cost_per_km"]     = [safe_ratio(c, k) for c,k in zip(df["cost"], df["distance_km"])]
    margins = pd.to_numeric(df["revenue"], errors="coerce") - pd.to_numeric(df["cost"], errors="coerce")
    df["margin_per_km"]   = [safe_ratio(m, k) for m,k in zip(margins, df["distance_km"])]

    save_csv(loc_cache, LOC_CACHE, ["norm"])
    save_csv(dist_cache, DIST_CACHE, ["origin_norm","dest_norm"])

    out = tsv_path.with_name(tsv_path.stem + "_enriched.tsv")
    df.to_csv(out, sep="\t", index=False)
    print(f"✅ Enrichi: {tsv_path.name} → {out.name} ({df.shape[0]} lignes)")

def main():
    files = sorted(RAW_DIR.glob("orders20*.tsv"))
    if not files:
        print("Aucun TSV dans data/processed/pdf_csv/")
        return
    for f in files:
        enrich_file(f)

if __name__ == "__main__":
    main()

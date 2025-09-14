# scripts/parse_shipment.py
from pathlib import Path
import pandas as pd
import io

RAW = Path("data/raw/SHIPMENT.TXT")      # <-- vérifie bien le NOM et la CASSE
OUTDIR = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTDIR / "master2.csv"

ENCODINGS = ["utf-8", "cp1252", "latin1"]

def read_text_any(path: Path) -> str:
    last = None
    for enc in ENCODINGS:
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception as e:
            last = e
    # fallback permissif
    return path.read_text(encoding="latin1", errors="ignore")

def looks_like_header(fields):
    """Heuristique: si >50% des champs contiennent des lettres, on considère que c'est un header."""
    import re
    alpha = sum(1 for f in fields if re.search(r"[A-Za-z]", f or ""))
    return alpha >= max(1, len(fields) // 2)

def main():
    if not RAW.exists():
        raise FileNotFoundError(f"Fichier introuvable: {RAW}. Vérifie le chemin exact (data/raw/SHIPMENT.TXT).")

    txt = read_text_any(RAW)

    # lecture ligne 1 pour décider header ou pas
    first_line = txt.splitlines()[0] if txt else ""
    sample_fields = first_line.split("\t")
    header_flag = 0 if looks_like_header(sample_fields) else None  # 0 = header ligne 1 ; None = pas de header

    # lecture robuste en tabulation
    df = pd.read_csv(
        io.StringIO(txt),
        sep="\t",
        header=header_flag,
        dtype=str,              # on garde tout en texte (évite 20951.000000 etc.)
        engine="python",        # plus tolérant
        quoting=3,              # QUOTE_NONE
        on_bad_lines="skip",    # si jamais une ligne est corrompue
    )

    # si pas de header: noms génériques
    if header_flag is None:
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]

    # nettoyage basique
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

    # write CSV propre pour Sheets/GPT
    df.to_csv(OUT_CSV, index=False)

    print(f"✅ Export terminé : {OUT_CSV} ({df.shape[0]} lignes, {df.shape[1]} colonnes})")
    # Petit aperçu pour debug (s'affiche dans les logs Actions)
    print("Aperçu colonnes:", list(df.columns)[:20])

if __name__ == "__main__":
    main()

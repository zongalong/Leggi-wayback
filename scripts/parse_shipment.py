import pandas as pd
from pathlib import Path

def main():
    # chemins
    RAW = Path("data/raw/SHIPMENT.TXT")
    OUTDIR = Path("data/processed")
    OUTDIR.mkdir(parents=True, exist_ok=True)

    OUT_CSV = OUTDIR / "master2.csv"

    # lecture du TXT exporté avec tabulation
    # dtype=str => garde tout en texte (pas de float 20951.000000 etc.)
    df = pd.read_csv(
        RAW,
        sep="\t",
        dtype=str,
        encoding="utf-8",   # tu peux mettre "latin1" si jamais tu vois des erreurs d'accents
    )

    # supprime les colonnes complètement vides
    df = df.dropna(axis=1, how="all")

    # supprime les lignes vides
    df = df.dropna(axis=0, how="all")

    # export en CSV propre
    df.to_csv(OUT_CSV, index=False)

    print(f"✅ Export terminé : {OUT_CSV} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")

if __name__ == "__main__":
    main()

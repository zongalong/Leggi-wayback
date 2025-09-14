import pandas as pd
import tabula
from pathlib import Path

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/pdf_csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MASTER = Path("data/processed/orders_master.tsv")

def convert_pdfs():
    all_dfs = []
    pdfs = list(RAW_DIR.glob("*.pdf"))
    if not pdfs:
        print("⚠️ Aucun PDF trouvé dans data/raw/")
        return
    
    for pdf in pdfs:
        print(f"📄 Conversion de {pdf}")
        try:
            # Lecture avec tabula
            dfs = tabula.read_pdf(str(pdf), pages="all", multiple_tables=True)
            for i, df in enumerate(dfs):
                if df.empty:
                    continue
                # Nettoyage basique
                df = df.dropna(how="all", axis=1)
                df = df.dropna(how="all", axis=0)

                # Sauvegarde TSV
                out_file = OUT_DIR / f"{pdf.stem}_{i}.tsv"
                df.to_csv(out_file, sep="\t", index=False)
                print(f"✅ {out_file} sauvegardé")
                all_dfs.append(df)
        except Exception as e:
            print(f"❌ Erreur sur {pdf}: {e}")

    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
        master_df.to_csv(MASTER, sep="\t", index=False)
        print(f"📊 Fichier master combiné: {MASTER} ({master_df.shape[0]} lignes)")
    else:
        print("⚠️ Aucun tableau valide trouvé dans les PDF.")

if __name__ == "__main__":
    convert_pdfs()

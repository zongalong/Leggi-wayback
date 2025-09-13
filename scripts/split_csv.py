# scripts/split_csv.py
import pandas as pd
from pathlib import Path
import math

SRC = Path("data/processed/master2.csv")
OUTDIR = Path("data/processed/chunks")
OUTDIR.mkdir(parents=True, exist_ok=True)

ROWS_PER_CHUNK = 1500  # ajuste si besoin

df = pd.read_csv(SRC, encoding="latin-1")
n = len(df)
nchunks = math.ceil(n / ROWS_PER_CHUNK)

for i in range(nchunks):
    start = i * ROWS_PER_CHUNK
    end = min((i + 1) * ROWS_PER_CHUNK, n)
    part = df.iloc[start:end]
    out = OUTDIR / f"master2_part_{i+1:03d}.csv"
    part.to_csv(out, index=False)
    print(f"Chunk {i+1}/{nchunks} -> {out} ({len(part)} lignes)")

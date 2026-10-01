"""Profile the raw Berka CSV files: look at the data BEFORE writing any loader.

Usage:
    python scripts/profile_berka.py
"""

from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "berka"

# Columns worth listing distinct values for (low cardinality)
CATEGORICAL = {
    "account": ["frequency"],
    "disp": ["type"],
    "loan": ["status", "duration"],
    "trans": ["type", "operation", "k_symbol"],
}


def section(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def profile(name: str) -> None:
    # Berka files use ';' as separator; read everything as text first so
    # pandas does not "guess" types and hide problems from us
    df = pd.read_csv(RAW_DIR / f"{name}.csv", sep=";", dtype=str, keep_default_na=False)

    section(f"{name}.csv — {len(df):,} rows × {len(df.columns)} columns")

    print("\nFirst 3 rows:")
    print(df.head(3).to_string(index=False))

    print("\nEmpty values per column:")
    empties = (df == "").sum()
    print(empties[empties > 0].to_string() if empties.any() else "  (none)")

    for col in CATEGORICAL.get(name, []):
        print(f"\nDistinct values of '{col}':")
        print(df[col].value_counts().to_string())

    if "date" in df.columns:
        print(f"\n'date' range: {df['date'].min()} → {df['date'].max()}")

    for col in ("amount", "balance"):
        if col in df.columns:
            values = pd.to_numeric(df[col])
            print(f"\n'{col}' stats:")
            print(values.describe().round(2).to_string())
            print(f"  rows with {col} < 0 : {(values < 0).sum():,}")
            print(f"  rows with {col} == 0: {(values == 0).sum():,}")


def main() -> None:
    for name in ["district", "client", "account", "disp", "loan", "trans"]:
        profile(name)


if __name__ == "__main__":
    main()

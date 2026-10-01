#!/usr/bin/env bash
# Download the Berka (PKDD'99 Czech bank) dataset into data/raw/berka/
# Original source (often offline): http://lisp.vse.cz/pkdd99/
# Mirror used here: https://github.com/Kusainov/czech-banking-fin-analysis
set -euo pipefail

BASE_URL="https://raw.githubusercontent.com/Kusainov/czech-banking-fin-analysis/HEAD"
TARGET_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/raw/berka"
FILES=(district client account disp loan trans)

mkdir -p "$TARGET_DIR"

for f in "${FILES[@]}"; do
    if [[ -s "$TARGET_DIR/$f.csv" ]]; then
        echo "✔ $f.csv already exists, skipping"
        continue
    fi
    echo "↓ Downloading $f.csv ..."
    curl -fsSL --retry 3 -o "$TARGET_DIR/$f.csv" "$BASE_URL/$f.csv"
done

echo
echo "Done. Row counts (including header):"
wc -l "$TARGET_DIR"/*.csv

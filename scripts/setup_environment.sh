#!/bin/bash
set -e

# This script prepares the PostgreSQL source tree and generates the symbol index.

POSTGRES_DIR="postgres"
POSTGRES_COMMIT="92268b35d04c2de416279f187d12f264afa22614"

if [ -d "$POSTGRES_DIR" ]; then
    echo "PostgreSQL source directory already exists. Skipping clone."
else
    echo "Cloning PostgreSQL repository..."
    git clone https://github.com/postgres/postgres.git
fi

cd "$POSTGRES_DIR"
echo "Checking out specific commit: $POSTGRES_COMMIT"
git checkout "$POSTGRES_COMMIT"

echo "Generating GNU GLOBAL tags (gtags)..."
# The --gtagslabel=ctags ensures compatibility.
gtags --gtagslabel=ctags

echo "Environment setup complete. GTAGS index is ready."

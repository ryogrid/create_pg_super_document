#!/bin/bash
"""
Autonomous Documentation Runner Script

This script ensures the autonomous documentation system runs properly
in GitHub Actions and handles all necessary setup steps.
"""

set -e  # Exit on any error

echo "🚀 Starting Autonomous PostgreSQL Documentation Runner"
echo "======================================================"

# Set up working directory
cd "$(dirname "$0")"
REPO_ROOT="$(pwd)"

echo "📁 Working in: $REPO_ROOT"

# Check Python and dependencies
echo "🐍 Checking Python environment..."
python3 --version
pip --version

# Install required dependencies if not present
echo "📦 Installing dependencies..."
pip install duckdb --quiet --user

# Check if documents database exists
if [ ! -f "data/documents.duckdb" ]; then
    echo "💾 Initializing documents database..."
    python3 scripts/init_documents_database.py
else
    echo "💾 Documents database found"
fi

# Get current documentation count
CURRENT_DOCS=$(python3 -c "
import duckdb
try:
    conn = duckdb.connect('data/documents.duckdb')
    count = conn.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
    conn.close()
    print(count)
except:
    print(0)
")

echo "📊 Current documented symbols: $CURRENT_DOCS/3164"

# Check if PostgreSQL source is available
if [ ! -d "postgres" ] || [ -z "$(ls -A postgres 2>/dev/null)" ]; then
    echo "📥 Cloning PostgreSQL source code..."
    rm -rf postgres
    git clone --depth=1 https://github.com/postgres/postgres.git postgres
    echo "✅ PostgreSQL source code ready"
else
    echo "✅ PostgreSQL source code already available"
fi

# Run the autonomous documentation system
echo "🤖 Starting autonomous documentation completion..."
python3 autonomous_complete_documentation.py

# Get final documentation count
FINAL_DOCS=$(python3 -c "
import duckdb
try:
    conn = duckdb.connect('data/documents.duckdb')
    count = conn.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
    conn.close()
    print(count)
except:
    print(0)
")

echo "📊 Final documented symbols: $FINAL_DOCS/3164"

# Calculate progress
if [ "$FINAL_DOCS" -gt 0 ]; then
    PROGRESS=$((FINAL_DOCS * 100 / 3164))
    echo "📈 Progress: $PROGRESS%"
    
    if [ "$FINAL_DOCS" -ge 3164 ]; then
        echo "🎉 Documentation completion achieved!"
        exit 0
    else
        echo "⏳ Documentation in progress - will continue in next workflow run"
        exit 0
    fi
else
    echo "❌ No documentation progress detected"
    exit 1
fi
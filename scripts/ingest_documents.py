# scripts/ingest_documents.py
import json
import duckdb
import re
from pathlib import Path

TEMP_DIR = Path('output/temp')

def extract_summary(content: str) -> str:
    """Extracts a brief summary from the 'Overview' section of the document."""
    match = re.search(r"##\s*Overview\s*\n+([^#]*)", content, re.IGNORECASE)
    if not match:
        return ""
    
    summary_text = match.group(1).strip()
    summary_lines = [line for line in summary_text.split('\n') if line.strip()]
    return ' '.join(summary_lines[:2])

def extract_relationships(content: str) -> tuple:
    """Extracts dependency and reference relationships from the document content."""
    # Extract "Functions called/Symbols referenced"
    deps_match = re.search(r"-\s*Functions called/Symbols referenced:\s*\n(.*?)(?=\n-|\n##|\Z)", content, re.DOTALL)
    deps_text = deps_match.group(1) if deps_match else ''
    deps_list = [item.strip() for item in re.findall(r'-\s*(\w+)', deps_text)]

    # Extract "Called from (representative examples)"
    related_match = re.search(r'-\s*Called from \(representative examples\):\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
    related_text = related_match.group(1) if related_match else ''
    related_list = [item.strip() for item in re.findall(r'-\s*(\w+)', related_text)]

    return list(set(deps_list)), list(set(related_list))


def ingest_all_documents(
    doc_db_file: str = 'data/documents.duckdb',
    symbols_db_file: str = 'global_symbols.db'
):
    """
    Scans the temp directory, parses and ingests markdown files into the
    documents DB, and cleans up the processed files.
    """
    if not TEMP_DIR.exists():
        print(json.dumps({"status": "no_action", "message": "Temporary directory not found. Nothing to ingest."}))
        return

    doc_con = duckdb.connect(doc_db_file)
    sym_con = duckdb.connect(symbols_db_file, read_only=True)
    
    ingested_symbols = []
    for doc_path in TEMP_DIR.glob("*.md"):
        symbol_name = doc_path.stem
        
        symbol_info = sym_con.execute(
            "SELECT id, symbol_type FROM symbol_definitions WHERE symbol_name = ?", (symbol_name,)
        ).fetchone()

        if not symbol_info:
            print(f"Warning: Symbol '{symbol_name}' not found in DB. Skipping.")
            continue
        
        sid, symbol_type = symbol_info
        if symbol_type is None:
            symbol_type = "unknown"  # Handle NULL symbol types
        
        content = doc_path.read_text(encoding='utf-8')
        
        # Parse the content to extract structured data
        summary = extract_summary(content)
        dependencies, related_symbols = extract_relationships(content)

        # Store the full content and the extracted metadata in the database
        doc_con.execute("""
            INSERT INTO documents (
                symbol_id, symbol_name, symbol_type, layer, content, summary, 
                dependencies, related_symbols
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol_id) DO UPDATE SET
                content = EXCLUDED.content,
                summary = EXCLUDED.summary,
                dependencies = EXCLUDED.dependencies,
                related_symbols = EXCLUDED.related_symbols,
                updated_at = now();
        """, (
            sid, symbol_name, symbol_type, 0, content, summary, 
            json.dumps(dependencies), json.dumps(related_symbols)
        ))
        
        doc_path.unlink() # Delete the temporary file
        ingested_symbols.append(symbol_name)
        
    doc_con.commit()
    doc_con.close()
    sym_con.close()
    
    result = {
        "status": "success",
        "ingested_count": len(ingested_symbols),
        "ingested_symbols": ingested_symbols
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    ingest_all_documents()

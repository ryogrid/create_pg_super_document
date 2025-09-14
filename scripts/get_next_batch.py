# scripts/get_next_batch.py
import json
import duckdb
import sys
from pathlib import Path

# Ensure snode_module can be imported from the root directory
sys.path.append(str(Path(__file__).parent.parent))
try:
    from snode_module import SNode, DatabaseConnection
except ImportError:
    print(json.dumps({"error": "FATAL: snode_module.py not found. Please place it in the project root."}))
    sys.exit(1)

def get_processed_symbol_ids(doc_db_file: str) -> set:
    """Gets a set of already processed symbol IDs from the documents database."""
    try:
        doc_con = duckdb.connect(doc_db_file)
        processed_ids = set(row[0] for row in doc_con.execute("SELECT symbol_id FROM documents").fetchall())
        doc_con.close()
        return processed_ids
    except duckdb.IOException:
        return set()

def get_symbol_details_map(db_file: str) -> dict:
    """Loads all symbol details into a dictionary for quick lookup."""
    con = duckdb.connect(db_file, read_only=True)
    details_map = {
        row[0]: {'id': row[0], 'name': row[1]} 
        for row in con.execute("SELECT id, symbol_name FROM symbol_definitions").fetchall()
    }
    con.close()
    return details_map

def get_processed_summaries(doc_db_file: str) -> dict:
    """Gets summaries of already processed symbols to provide context."""
    try:
        doc_con = duckdb.connect(doc_db_file)
        summaries = {
            row[0]: row[1] 
            for row in doc_con.execute("SELECT symbol_name, summary FROM documents WHERE summary IS NOT NULL AND summary != ''").fetchall()
        }
        doc_con.close()
        return summaries
    except duckdb.IOException:
        return {}


def get_next_unprocessed_batch_with_context(
    batches_file: str = 'data/processing_batches.json',
    doc_db_file: str = 'data/documents.duckdb',
    symbols_db_file: str = 'global_symbols.db',
    meta_db_file: str = 'data/metadata.duckdb'
) -> dict:
    """
    Finds the next unprocessed batch and enriches it with all necessary context
    for the AI agent to generate documentation.
    """
    processed_ids = get_processed_symbol_ids(doc_db_file)
    symbol_details_map = get_symbol_details_map(symbols_db_file)
    processed_summaries = get_processed_summaries(doc_db_file)
    
    # Establish a persistent connection for dependency lookups
    meta_con = duckdb.connect(meta_db_file, read_only=True)

    with open(batches_file) as f:
        all_batches = json.load(f)

    for batch in all_batches:
        unprocessed_ids = [sid for sid in batch['symbol_ids'] if sid not in processed_ids]
        if not unprocessed_ids:
            continue

        # Found a batch to process, now gather context for each symbol.
        enriched_symbols = []
        for symbol_id in unprocessed_ids:
            try:
                node = SNode(symbol_id)
                
                # Gather related symbol summaries
                relevant_summaries = []
                dep_ids = meta_con.execute("SELECT to_node FROM dependencies WHERE from_node = ?", (symbol_id,)).fetchall()
                for (dep_id,) in dep_ids:
                    dep_name = symbol_details_map.get(dep_id, {}).get('name')
                    if dep_name and dep_name in processed_summaries:
                        summary = processed_summaries[dep_name]
                        relevant_summaries.append(f"- {dep_name}: {summary[:120]}")

                enriched_symbols.append({
                    "symbol_name": node.symbol_name,
                    "definition": node.get_source_code(),
                    "references_from_this": node.get_references_from_this(),
                    "references_to_this": node.get_references_to_this(),
                    "related_symbol_summaries": sorted(list(set(relevant_summaries)))[:15],
                })
            except ValueError:
                # If a symbol is not found (should be rare), we skip it.
                continue
        
        meta_con.close()
        # Close the global DB connection used by SNode instances
        DatabaseConnection().close()

        return {
            "batch_id": batch['batch_id'],
            "symbols_to_process": enriched_symbols,
            "required_markdown_format": """
# [Symbol Name]

## Overview
(Briefly explain the purpose and role of this symbol in 1-2 sentences)

## Definition
(Provide the function signature or struct/enum definition)
Example: void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid, char *out_dbname)


## Detailed Description
(Provide specific explanation of the symbol's functionality, behavior, design philosophy, etc.)

## Parameters / Member Variables
(Explain the role and meaning of each function parameter or struct member in a bulleted list)
- `param1`: (description)
- `member1`: (description)

## Dependencies
- Functions called/Symbols referenced:
  - func_a
  - TYPE_B
- Called from (representative examples):
  - caller_func_x
  - caller_func_y

## Notes and Other Information
(Notable points, usage precautions, related background knowledge, etc.)
"""
        }

    meta_con.close()
    return {"message": "All batches have been processed."}


if __name__ == "__main__":
    next_batch_data = get_next_unprocessed_batch_with_context()
    print(json.dumps(next_batch_data, indent=2))
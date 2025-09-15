#!/usr/bin/env python3
"""
Improved batch retrieval with enhanced context and Copilot-optimized prompts
"""
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

def get_quality_examples() -> str:
    """Return high-quality examples for documentation to guide AI generation"""
    return """
EXAMPLE OF HIGH-QUALITY DOCUMENTATION:

# heap_insert

## Overview
Inserts a new tuple into a heap table, handling MVCC versioning and triggering necessary maintenance operations.

## Definition
```c
HeapTuple heap_insert(Relation relation, HeapTuple tup, CommandId cid, 
                      int options, BulkInsertState bistate)
```

## Detailed Description
heap_insert performs the fundamental operation of adding a new tuple to a PostgreSQL heap table. It manages transaction visibility information through MVCC (Multi-Version Concurrency Control) by setting the appropriate transaction ID and command ID in the tuple header. The function also handles various optimization strategies including bulk insert operations and handles toast operations for large values. It coordinates with the buffer manager to ensure proper page allocation and maintains WAL (Write-Ahead Log) records for crash recovery.

## Parameters / Member Variables
- `relation`: The target relation (table) where the tuple will be inserted
- `tup`: The HeapTuple structure containing the data to be inserted  
- `cid`: Command ID for MVCC visibility determination
- `options`: Bitmask of insert options (e.g., HEAP_INSERT_SKIP_WAL)
- `bistate`: State for bulk insert optimizations, can be NULL for single inserts

## Dependencies
- **Called functions/Referenced symbols**:
  - `RelationGetBufferForTuple` - Allocates buffer space for the new tuple
  - `PageAddItem` - Adds the tuple to the page
  - `XLogInsert` - Creates WAL record for the insertion
  - `toast_insert_or_update` - Handles large attribute values
- **Called from (representative examples)**:
  - `table_tuple_insert` - Generic table insertion interface
  - `CatalogTupleInsert` - System catalog insertions
  - `ExecInsert` - Executor node for INSERT statements

## Notes & Other Information
This function is performance-critical and includes several optimizations for bulk operations. When using BulkInsertState, multiple insertions can share buffer and WAL optimizations. The function must be called within a valid transaction context and proper locking must be established on the relation before calling.
"""

def get_next_unprocessed_batch_with_context(
    batches_file: str = 'data/processing_batches.json',
    doc_db_file: str = 'data/documents.duckdb',
    symbols_db_file: str = 'global_symbols.db',
    meta_db_file: str = 'data/metadata.duckdb'
) -> dict:
    """
    Finds the next unprocessed batch and enriches it with all necessary context
    for the Copilot Coding Agent to generate high-quality documentation.
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
                # Get symbol name from the details map
                symbol_name = symbol_details_map.get(symbol_id, {}).get('name')
                if not symbol_name:
                    print(f"Warning: Symbol ID {symbol_id} not found in details map")
                    continue
                
                node = SNode(symbol_name)
                
                # Gather related symbol summaries
                relevant_summaries = []
                dep_ids = meta_con.execute("SELECT to_node FROM dependencies WHERE from_node = ?", (symbol_id,)).fetchall()
                for (dep_id,) in dep_ids:
                    dep_name = symbol_details_map.get(dep_id, {}).get('name')
                    if dep_name and dep_name in processed_summaries:
                        summary = processed_summaries[dep_name]
                        relevant_summaries.append(f"- {dep_name}: {summary[:150]}")

                try:
                    definition = node.get_source_code()
                except (FileNotFoundError, OSError):
                    definition = "// Source file not available - run setup_environment.sh first"
                
                try:
                    references_from = node.get_references_from_this()
                except (FileNotFoundError, OSError):
                    references_from = []
                    
                try:
                    references_to = node.get_references_to_this()
                except (FileNotFoundError, OSError):
                    references_to = []

                enriched_symbols.append({
                    "symbol_name": node.symbol_name,
                    "definition": definition,
                    "references_from_this": references_from,
                    "references_to_this": references_to,
                    "related_symbol_summaries": sorted(list(set(relevant_summaries)))[:20],
                })
            except (ValueError, FileNotFoundError, OSError):
                # If a symbol is not found or source files are missing, we skip it.
                continue
        
        meta_con.close()
        # Close the global DB connection used by SNode instances
        DatabaseConnection().close()

        return {
            "batch_id": batch['batch_id'],
            "symbols_to_process": enriched_symbols,
            "context_reset_instruction": "IMPORTANT: You are starting fresh with this batch. Previous conversations are irrelevant.",
            "copilot_specific_instructions": {
                "focus_areas": [
                    "Write comprehensive, detailed explanations",
                    "Include specific technical details and implementation context",
                    "Provide thorough parameter/member descriptions", 
                    "Explain the 'why' behind each symbol's design",
                    "Include concrete usage examples where possible"
                ],
                "quality_requirements": [
                    "Each overview should be 2-3 sentences minimum",
                    "Detailed descriptions should be at least 3-4 sentences",
                    "Every parameter must be thoroughly explained",
                    "Dependencies section should include reasoning for relationships"
                ]
            },
            "quality_example": get_quality_examples(),
            "required_markdown_format": """
# [Symbol Name]

## Overview
(Provide a comprehensive 2-3 sentence explanation of the symbol's purpose, role in PostgreSQL, and its significance in the codebase. Be specific about what it does and why it exists.)

## Definition
(Provide the complete function signature, struct definition, or enum definition in a code block)
```c
// Example: void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid, char *out_dbname)
```

## Detailed Description
(Write a comprehensive 4-6 sentence explanation covering:
- What the symbol does technically
- How it fits into PostgreSQL's architecture
- Key algorithms or logic it implements
- Important behavioral characteristics
- Performance considerations if relevant)

## Parameters / Member Variables
(For each parameter/member, provide detailed explanations including purpose, expected values, constraints, and relationships to other parameters)
- `param1`: (Detailed description including type information, purpose, valid range/values, and how it affects behavior)
- `member1`: (For struct members, explain the data stored, when it's set, and how it's used)

## Dependencies
- **Functions called/Symbols referenced**:
  (List key functions this symbol calls, with brief explanations of why)
  - `func_a` - Brief explanation of why this dependency exists
  - `TYPE_B` - Description of how this type is used
- **Called from (representative examples)**:
  (List important callers, explaining the context of the calls)
  - `caller_func_x` - Context of when/why this calls our symbol
  - `caller_func_y` - Another important usage context

## Notes & Other Information
(Include important implementation notes, performance considerations, thread safety notes, error handling behavior, historical context, or related design decisions. This section should provide insights that help developers understand the broader context.)
"""
        }

    meta_con.close()
    return {"message": "All batches have been processed."}


if __name__ == "__main__":
    next_batch_data = get_next_unprocessed_batch_with_context()
    print(json.dumps(next_batch_data, indent=2))
#!/usr/bin/env python3
"""
Script to add symbol types to the symbol definition database

This script uses snode_module to get source code for each symbol and
determines whether it is a function, macro, variable, struct, union, enum, typedef, etc.,
and records it in the 'symbol_type' column of the database.
"""

import re
import duckdb
from tqdm import tqdm
import snode_module

# --- Global configuration ---
DB_FILE = "global_symbols.db"
SYMBOL_TABLE = "symbol_definitions"


def classify_source_code(source_code: str) -> str:
    """
    Determine the type of symbol from source code string

    Args:
        source_code: Source code with header obtained from get_source_code()

    Returns:
        str: One of the classification codes 'f', 'm', 'c', 's', 'k', 'e', 't', 'v', 'u'
    """
    # Remove header line ("Source: ...")
    lines = source_code.splitlines()
    code_lines = lines[1:] if lines and lines[0].startswith("Source:") else lines

    # Find the first meaningful line excluding comments and empty lines
    first_meaningful_line = ""
    for line in code_lines:
        stripped_line = line.strip()
        if stripped_line and not stripped_line.startswith(('//', '/*')) and not stripped_line.startswith('*'):
            first_meaningful_line = stripped_line
            break
    
    # Combine multiple lines into one string for easier analysis
    full_code = "\n".join(code_lines).strip()

    # 1. Macro detection
    if first_meaningful_line.startswith("#define"):
        # Check if it's in the format #define SYMBOL( ... )
        if re.search(r'#define\s+\w+\(', first_meaningful_line):
            return 'm'  # Function-like macro
        else:
            return 'c'  # Constant macro

    # 2. Struct detection
    if re.match(r'^\s*(typedef\s+)?struct', first_meaningful_line, re.IGNORECASE):
        return 's'

    # 3. Union detection
    if re.match(r'^\s*(typedef\s+)?union', first_meaningful_line, re.IGNORECASE):
        return 'k'

    # 4. Enum detection
    if re.match(r'^\s*(typedef\s+)?enum', first_meaningful_line, re.IGNORECASE):
        return 'e'

    # 5. Other typedef detection
    # Pure typedef not involving struct, union, enum
    if re.match(r'^\s*typedef', first_meaningful_line, re.IGNORECASE):
        return 't'

    # 6. Function detection
    # heuristic: pattern of return type, function name, argument list '()' followed by '{'
    if '{' in full_code and '}' in full_code:
        # struct/union/enum definitions are already handled above, so no need to exclude
        # Example: `int my_func(void) { ... }`
        if re.search(r'[\w\s\*]+\s+\w+\s*\([^;{}]*\)\s*\{', full_code, re.DOTALL):
            return 'f'  # Function

    # 7. Global variable detection
    # heuristic: not any of the above and ends with ';'
    if full_code.endswith(';'):
         return 'v' # Global variable

    # 8. Those that cannot be classified into any of the above
    return 'u'


def main():
    """Main processing"""
    print("Starting symbol type classification script.")

    # 1. Connect to database
    try:
        # snode_module connects read-only, so connect separately for updates
        db = snode_module.DatabaseConnection()
        con = db.get_connection()  # Initialize and get singleton connection for snode_module
    except duckdb.IOException as e:
        print(f"Error: Could not connect to database '{DB_FILE}'. {e}")
        return

    # 2. Check existence of 'symbol_type' column, add if not exists and create index
    try:
        table_info = con.execute(f"PRAGMA table_info('{SYMBOL_TABLE}')").fetchall()
        column_names = [col[1] for col in table_info]

        if 'symbol_type' not in column_names:
            print(f"Adding 'symbol_type' column to '{SYMBOL_TABLE}' table...")
            con.execute(f"ALTER TABLE {SYMBOL_TABLE} ADD COLUMN symbol_type VARCHAR;")
            print("Creating index on 'symbol_type' column...")
            con.execute(f"CREATE INDEX idx_symbol_type ON {SYMBOL_TABLE} (symbol_type);")
            print("Column and index created successfully.")
        else:
            print("'symbol_type' column already exists.")
    except duckdb.Error as e:
        print(f"Error during database setup: {e}")
        con.close()
        return

    # 3. Get all symbol IDs
    try:
        print("Fetching all symbol IDs...")
        all_ids = con.execute(f"SELECT id FROM {SYMBOL_TABLE} ORDER BY id").fetchall()
        symbol_ids = [row[0] for row in all_ids]
        print(f"Found {len(symbol_ids)} symbols to process.")
    except duckdb.Error as e:
        print(f"Error fetching symbol IDs: {e}")
        con.close()
        return

    # 4. Classify each symbol
    updates_to_perform = []
    print("\nClassifying symbol types...")
    for symbol_id in tqdm(symbol_ids, desc="Classifying"):
        try:
            node = snode_module.SNode.from_id(symbol_id)
            source_code = node.get_source_code()
            symbol_type = classify_source_code(source_code)
            updates_to_perform.append((symbol_type, symbol_id))
        except (ValueError, FileNotFoundError) as e:
            print(f"\nWarning: Skipping symbol ID {symbol_id} ({getattr(node, 'symbol_name', 'N/A')}). Reason: {e}")
        except Exception as e:
            print(f"\nError: An unexpected error occurred for symbol ID {symbol_id}: {e}")

    # 5. Batch update database
    if updates_to_perform:
        print(f"\nUpdating {len(updates_to_perform)} records in the database...")
        try:
            con.begin() # Start transaction
            for symbol_type, symbol_id in tqdm(updates_to_perform, desc="Updating DB"):
                con.execute(f"UPDATE {SYMBOL_TABLE} SET symbol_type = ? WHERE id = ?", (symbol_type, symbol_id))
            con.commit() # Commit transaction
            print("Database update completed successfully.")
        except duckdb.Error as e:
            print(f"Error during database update: {e}")
            con.rollback()
            print("Changes were rolled back.")
    else:
        print("No symbols were classified or updated.")

    ## Close connection
    #con.close()
    # Also close singleton connection used by snode_module
    snode_module.DatabaseConnection().close()
    print("Script finished.")


if __name__ == "__main__":
    main()

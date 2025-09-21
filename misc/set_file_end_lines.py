#!/usr/bin/env python3
"""
Script to set the total number of lines in a file to line_num_end 
for the last symbol in the file among records where line_num_end is 0
"""

import sys
import subprocess
import duckdb
from pathlib import Path
from typing import Optional, List, Tuple

# Database configuration
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_definitions"


def run_global_command(file_path: str) -> Optional[str]:
    """Execute global command to get symbol information"""
    try:
        result = subprocess.run(
            ['global', '-fx', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running global command for {file_path}: {e}", file=sys.stderr)
        return None
    except FileNotFoundError:
        print("Error: 'global' command not found. Please ensure GNU GLOBAL is installed.", file=sys.stderr)
        sys.exit(1)


def get_last_symbol_from_global(output: str) -> Optional[str]:
    """Get the last symbol name from global command output"""
    if not output:
        return None
    
    lines = output.strip().split('\n')
    if not lines:
        return None
    
    # Parse the last line
    last_line = lines[-1]
    parts = last_line.split(None, 3)  # Split by space (max 4 elements)
    
    if len(parts) >= 1:
        return parts[0]  # Symbol name
    
    return None


def count_file_lines(file_path: str) -> int:
    """Get the total number of lines in a file"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Error counting lines in {file_path}: {e}", file=sys.stderr)
        return 0


def process_zero_end_lines(conn: duckdb.DuckDBPyConnection) -> None:
    """Process records with line_num_end = 0"""
    
    print("=" * 60)
    print("Processing records with line_num_end = 0")
    print("=" * 60)
    
    # Get records with line_num_end = 0
    records = conn.execute(f"""
        SELECT id, symbol_name, file_path, line_num_start
        FROM {TABLE_NAME}
        WHERE line_num_end = 0
        ORDER BY file_path, id
    """).fetchall()
    
    if not records:
        print("No records found with line_num_end = 0")
        return
    
    print(f"Found {len(records)} records with line_num_end = 0")
    
    # Group by file and process
    file_groups = {}
    for record in records:
        file_path = record[2]
        if file_path not in file_groups:
            file_groups[file_path] = []
        file_groups[file_path].append(record)
    
    print(f"Processing {len(file_groups)} unique files...")
    
    updates = []  # List of (id, line_num_end)
    processed_files = 0
    
    for file_path, file_records in file_groups.items():
        processed_files += 1
        
        # Check if file exists
        if not Path(file_path).exists():
            print(f"  Warning: File not found: {file_path}")
            continue
        
        # Execute global command
        output = run_global_command(file_path)
        if output is None:
            continue
        
        # Get the last symbol
        last_symbol = get_last_symbol_from_global(output)
        if last_symbol is None:
            print(f"  Warning: Could not determine last symbol for {file_path}")
            continue
        
        # Find the record that matches the last symbol from this file's records
        last_symbol_found = False
        for record in file_records:
            record_id = record[0]
            symbol_name = record[1]
            
            if symbol_name == last_symbol:
                # Get total number of lines in file
                total_lines = count_file_lines(file_path)
                if total_lines > 0:
                    updates.append((record_id, total_lines))
                    print(f"  {file_path}: Setting line_num_end={total_lines} for symbol '{symbol_name}' (last symbol in file)")
                    last_symbol_found = True
                    break
        
        if not last_symbol_found:
            # When the last symbol is not found in records with line_num_end=0
            # (possibly line_num_end is already set by other processing)
            print(f"  {file_path}: Last symbol '{last_symbol}' already has line_num_end set or not found")
        
        # Progress display
        if processed_files % 100 == 0:
            print(f"  Progress: {processed_files}/{len(file_groups)} files processed...")
    
    # Execute updates
    if updates:
        print(f"\nApplying {len(updates)} updates...")
        for id_val, line_end in updates:
            conn.execute(f"""
                UPDATE {TABLE_NAME}
                SET line_num_end = ?
                WHERE id = ?
            """, (line_end, id_val))
        
        conn.commit()
        print(f"Successfully updated {len(updates)} records")
    else:
        print("No updates needed")


def show_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    """Display statistics"""
    print("\n" + "=" * 60)
    print("Statistics")
    print("=" * 60)
    
    # line_num_end statistics
    total_records = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    records_with_end = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE line_num_end > 0").fetchone()[0]
    records_without_end = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE line_num_end = 0").fetchone()[0]
    
    print(f"Total records: {total_records}")
    print(f"Records with line_num_end > 0: {records_with_end}")
    print(f"Records with line_num_end = 0: {records_without_end}")
    
    if records_with_end > 0:
        percentage = (records_with_end / total_records) * 100
        print(f"Completion rate: {percentage:.2f}%")
    
    # Details of records with line_num_end = 0 (first 10)
    if records_without_end > 0:
        print(f"\nSample of remaining records with line_num_end = 0:")
        remaining = conn.execute(f"""
            SELECT symbol_name, file_path, line_num_start
            FROM {TABLE_NAME}
            WHERE line_num_end = 0
            ORDER BY file_path, line_num_start
            LIMIT 10
        """).fetchall()
        
        for symbol, file_path, line_start in remaining:
            # Shorten file path for display
            short_path = file_path
            if len(file_path) > 60:
                short_path = "..." + file_path[-57:]
            print(f"  {symbol:30s} {short_path:60s} line {line_start}")


def main():
    """Main processing"""
    # Check database file
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # Database connection
    conn = duckdb.connect(DB_FILE)
    
    try:
        # Check table existence
        table_exists = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{TABLE_NAME}'
        """).fetchone()[0]
        
        if not table_exists:
            print(f"Error: Table '{TABLE_NAME}' not found in database.", file=sys.stderr)
            sys.exit(1)
        
        # Statistics before processing
        print("Initial state:")
        show_statistics(conn)
        
        # Process records with line_num_end = 0
        process_zero_end_lines(conn)
        
        # Statistics after processing
        print("\nFinal state:")
        show_statistics(conn)
        
        print("\n" + "=" * 60)
        print("Processing completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

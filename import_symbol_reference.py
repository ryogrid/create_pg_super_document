#!/usr/bin/env python3
"""
Script to import symbol_references_filtered.csv into DuckDB's symbol_reference table
"""

import sys
import csv
import duckdb
from pathlib import Path
from typing import List, Tuple

# Database configuration
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_reference"
CSV_FILE = "symbol_references_filtered.csv"  # Filename as specified by user


def create_table_if_not_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Create symbol_reference table and indexes if they don't exist
    Returns: True if table was created, False if it already existed
    """
    
    # Check table existence
    table_exists = conn.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '{TABLE_NAME}'
    """).fetchone()[0]
    
    if table_exists:
        print(f"Table '{TABLE_NAME}' already exists. Skipping creation.")
        return False
    
    print(f"Creating table '{TABLE_NAME}'...")
    
    # Create table
    conn.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            from_node INTEGER NOT NULL,
            to_node INTEGER NOT NULL,
            line_num_in_from INTEGER NOT NULL
        )
    """)
    
    # Create indexes
    print("Creating indexes...")
    
    # Index for from_node column
    conn.execute(f"""
        CREATE INDEX idx_{TABLE_NAME}_from_node ON {TABLE_NAME} (from_node)
    """)
    
    # Index for to_node column
    conn.execute(f"""
        CREATE INDEX idx_{TABLE_NAME}_to_node ON {TABLE_NAME} (to_node)
    """)
    
    print(f"Table '{TABLE_NAME}' and indexes created successfully.")
    return True


def read_csv_file(csv_file: str) -> List[Tuple[int, int, int]]:
    """
    Read CSV file and return as a list of integer value tuples
    """
    records = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            for row_num, row in enumerate(reader, 1):
                # Skip empty rows
                if not row:
                    continue
                
                # Check for 3 elements
                if len(row) != 3:
                    print(f"Warning: Line {row_num} has {len(row)} elements instead of 3. Skipping.")
                    continue
                
                try:
                    # Convert to integer values
                    from_id = int(row[0])
                    to_id = int(row[1])
                    line_num = int(row[2])
                    
                    records.append((from_id, to_id, line_num))
                    
                except ValueError as e:
                    print(f"Warning: Line {row_num} contains non-integer values: {row}. Skipping.")
                    continue
                    
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        sys.exit(1)
    
    return records


def insert_records(conn: duckdb.DuckDBPyConnection, 
                  records: List[Tuple[int, int, int]]) -> None:
    """
    Insert records into table
    """
    if not records:
        print("No records to insert.")
        return
    
    print(f"Inserting {len(records)} records into {TABLE_NAME}...")
    
    # Batch insert (efficient)
    conn.executemany(f"""
        INSERT INTO {TABLE_NAME} (from_node, to_node, line_num_in_from)
        VALUES (?, ?, ?)
    """, records)
    
    conn.commit()
    print(f"Successfully inserted {len(records)} records.")


def show_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Display table statistics
    """
    print("\n" + "=" * 60)
    print("Table Statistics")
    print("=" * 60)
    
    # Total record count
    total_records = conn.execute(f'SELECT COUNT(*) FROM {TABLE_NAME}').fetchone()[0]
    print(f"Total records: {total_records}")
    
    if total_records > 0:
        # Number of unique from_node and to_node
        unique_from = conn.execute(f'SELECT COUNT(DISTINCT from_node) FROM {TABLE_NAME}').fetchone()[0]
        unique_to = conn.execute(f'SELECT COUNT(DISTINCT to_node) FROM {TABLE_NAME}').fetchone()[0]
        
        print(f"Unique 'from_node' values: {unique_from}")
        print(f"Unique 'to_node' values: {unique_to}")
        
        # Top 10 most referenced symbols (by to_node column)
        print("\nTop 10 most referenced symbols (by 'to_node' ID):")
        top_referenced = conn.execute(f"""
            SELECT to_node, COUNT(*) as ref_count
            FROM {TABLE_NAME}
            GROUP BY to_node
            ORDER BY ref_count DESC
            LIMIT 10
        """).fetchall()
        
        for to_id, count in top_referenced:
            # Get name from symbol_definitions table (if exists)
            try:
                symbol_info = conn.execute("""
                    SELECT symbol_name, file_path, line_num_start
                    FROM symbol_definitions
                    WHERE id = ?
                """, (to_id,)).fetchone()
                
                if symbol_info:
                    symbol_name, file_path, line_start = symbol_info
                    # Shorten file path for display
                    if len(file_path) > 40:
                        short_path = "..." + file_path[-37:]
                    else:
                        short_path = file_path
                    print(f"  ID {to_id:6d}: {symbol_name:30s} ({short_path}:{line_start}) - {count} references")
                else:
                    print(f"  ID {to_id:6d}: [Symbol not found] - {count} references")
            except:
                print(f"  ID {to_id:6d}: - {count} references")
        
        # Top 10 symbols with most references (by from_node column)
        print("\nTop 10 symbols with most references (by 'from_node' ID):")
        top_referencing = conn.execute(f"""
            SELECT from_node, COUNT(*) as ref_count
            FROM {TABLE_NAME}
            GROUP BY from_node
            ORDER BY ref_count DESC
            LIMIT 10
        """).fetchall()
        
        for from_id, count in top_referencing:
            # Get name from symbol_definitions table (if exists)
            try:
                symbol_info = conn.execute("""
                    SELECT symbol_name, file_path, line_num_start
                    FROM symbol_definitions
                    WHERE id = ?
                """, (from_id,)).fetchone()
                
                if symbol_info:
                    symbol_name, file_path, line_start = symbol_info
                    # Shorten file path for display
                    if len(file_path) > 40:
                        short_path = "..." + file_path[-37:]
                    else:
                        short_path = file_path
                    print(f"  ID {from_id:6d}: {symbol_name:30s} ({short_path}:{line_start}) - {count} references")
                else:
                    print(f"  ID {from_id:6d}: [Symbol not found] - {count} references")
            except:
                print(f"  ID {from_id:6d}: - {count} references")


def main():
    """Main processing"""
    print("=" * 60)
    print("Symbol References Import Tool")
    print("=" * 60)
    print(f"Database: {DB_FILE}")
    print(f"Table: {TABLE_NAME}")
    print(f"CSV file: {CSV_FILE}")
    print()
    
    # Check database file
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.", file=sys.stderr)
        print("Please run global_to_duckdb.py first to create the database.", file=sys.stderr)
        sys.exit(1)
    
    # Check CSV file
    if not Path(CSV_FILE).exists():
        print(f"Error: CSV file '{CSV_FILE}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # Database connection
    conn = duckdb.connect(DB_FILE)
    
    try:
        # Create table (if needed)
        table_created = create_table_if_not_exists(conn)
        
        # Read CSV file
        print(f"\nReading CSV file '{CSV_FILE}'...")
        records = read_csv_file(CSV_FILE)
        
        if records:
            print(f"Read {len(records)} valid records from CSV.")
            
            # Check existing record count
            existing_records = conn.execute(f'SELECT COUNT(*) FROM {TABLE_NAME}').fetchone()[0]
            if existing_records > 0 and not table_created:
                print(f"\nWarning: Table already contains {existing_records} records.")
                response = input("Do you want to append new records? (y/n): ").strip().lower()
                if response != 'y':
                    print("Import cancelled.")
                    return
            
            # Insert records
            insert_records(conn, records)
            
            # Display statistics
            show_statistics(conn)
        else:
            print("No valid records found in CSV file.")
        
        print("\n" + "=" * 60)
        print("Import completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

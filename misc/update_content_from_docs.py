#!/usr/bin/env python3
"""
Update DuckDB documents table content column with markdown files from generated_docs directory.

This script reads markdown files from the generated_docs directory and updates the
corresponding content column in the DuckDB documents table based on symbol_name matching.
"""

import os
import glob
import duckdb
from pathlib import Path

def find_markdown_file(symbol_name, docs_dir):
    """
    Find the markdown file for a given symbol name in the generated_docs directory.

    Args:
        symbol_name (str): The symbol name to search for
        docs_dir (str): Path to the generated_docs directory

    Returns:
        str or None: Path to the markdown file if found, None otherwise
    """
    # Try direct path first: generated_docs/{first_letter}/{symbol_name}.md
    first_letter = symbol_name[0].upper() if symbol_name else 'A'
    direct_path = os.path.join(docs_dir, first_letter, f"{symbol_name}.md")

    if os.path.exists(direct_path):
        return direct_path

    # If not found, search all markdown files
    pattern = os.path.join(docs_dir, "**", f"{symbol_name}.md")
    matches = glob.glob(pattern, recursive=True)

    if matches:
        return matches[0]  # Return first match

    return None

def read_markdown_content(file_path):
    """
    Read the content of a markdown file.

    Args:
        file_path (str): Path to the markdown file

    Returns:
        str: Content of the file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def update_database_content(db_path, docs_dir):
    """
    Update the content column in the documents table with markdown file contents.

    Args:
        db_path (str): Path to the DuckDB database file
        docs_dir (str): Path to the generated_docs directory
    """
    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)

        # Get all symbol names from the database
        symbols_query = "SELECT symbol_id, symbol_name FROM documents WHERE symbol_name IS NOT NULL"
        symbols = conn.execute(symbols_query).fetchall()

        print(f"Found {len(symbols)} symbols in the database")

        updated_count = 0
        not_found_count = 0

        for symbol_id, symbol_name in symbols:
            # Find the corresponding markdown file
            md_file_path = find_markdown_file(symbol_name, docs_dir)

            if md_file_path:
                # Read the markdown content
                content = read_markdown_content(md_file_path)

                if content is not None:
                    # Update the database
                    update_query = """
                    UPDATE documents
                    SET content = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE symbol_id = ?
                    """
                    conn.execute(update_query, [content, symbol_id])
                    updated_count += 1
                    print(f"Updated {symbol_name} (ID: {symbol_id})")
                else:
                    print(f"Failed to read content for {symbol_name}")
            else:
                not_found_count += 1
                print(f"Markdown file not found for symbol: {symbol_name}")

        # Commit changes
        conn.commit()

        print(f"\nUpdate completed:")
        print(f"- Updated: {updated_count} symbols")
        print(f"- Not found: {not_found_count} symbols")

    except Exception as e:
        print(f"Error updating database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Main function to execute the update process."""
    # Define paths
    db_path = "data/documents.duckdb"
    docs_dir = "generated_docs"

    # Check if paths exist
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return

    if not os.path.exists(docs_dir):
        print(f"Documentation directory not found: {docs_dir}")
        return

    print(f"Updating database: {db_path}")
    print(f"Using documentation from: {docs_dir}")
    print("-" * 50)

    # Perform the update
    update_database_content(db_path, docs_dir)

if __name__ == "__main__":
    main()
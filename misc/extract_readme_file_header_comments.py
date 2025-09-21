#!/usr/bin/env python3
"""
Script to analyze a C language code tree and store documentation information in DuckDB.

- Recursively traverses src/ and contrib/ directories.
- Aggregates README* file contents of each directory into the 'dir_info' table.
- Extracts header comments of each C/H file into the 'file_info' table.
"""

import duckdb
import os
import re
from pathlib import Path
from typing import List, Optional

# --- Configuration ---
DB_FILE = "assistive_info.db"
TARGET_DIRS = ["src", "contrib"]
README_SEPARATOR = "\n\n---\n\n"

def setup_database(conn: duckdb.DuckDBPyConnection):
    """Perform initial setup of database and tables"""
    print("Setting up database tables...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dir_info (
            path VARCHAR PRIMARY KEY,
            readme_contents VARCHAR
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_info (
            path VARCHAR PRIMARY KEY,
            header_comment VARCHAR
        );
    """)
    print("Database setup complete.")

def extract_header_comment(file_path: Path) -> Optional[str]:
    """
    Extract and format C-style block comments from the beginning of a file.
    Returns None if no comment is found or if code appears before the comment.
    """
    try:
        with file_path.open('r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except IOError:
        return None

    comment_lines = []
    in_comment = False

    for line in lines:
        stripped_line = line.strip()

        # Exit if code or preprocessor directives appear before comment starts
        if not in_comment and stripped_line and not stripped_line.startswith('/*'):
            return None

        if stripped_line.startswith('/*'):
            in_comment = True
        
        if in_comment:
            comment_lines.append(line)

        if '*/' in stripped_line:
            break
    
    if not comment_lines:
        return None

    # Combine entire comment block into one string
    full_comment = "".join(comment_lines)
    
    # Remove /* and */ and hyphens in between
    match = re.search(r'/\*(-*)\n(.*?)\n\s*(-*)\*/', full_comment, re.DOTALL)
    if not match:
        # For simple /* comment */ format
        match = re.search(r'/\*\s*(.*?)\s*\*/', full_comment, re.DOTALL)
        if not match:
            return None # Not the expected comment format
        content = match.group(1)
    else:
        content = match.group(2)

    # Remove leading '*' from each line
    lines_without_stars = [re.sub(r'^\s*\*\s?', '', line) for line in content.splitlines()]
    
    # ★★★ Modified Section ★★★
    # Remove lines containing "Copyright" (case insensitive)
    final_lines = [line for line in lines_without_stars if 'copyright' not in line.lower()]

    # Generate final string
    final_comment = "\n".join(final_lines).strip()
    
    # Return None if comment becomes empty after removing Copyright lines
    return final_comment if final_comment else None

def process_directory(base_path: Path, conn: duckdb.DuckDBPyConnection):
    """Process the specified directory recursively"""
    if not base_path.is_dir():
        print(f"Warning: Directory '{base_path}' not found. Skipping.")
        return

    print(f"Processing directory: {base_path}...")
    
    cwd = Path.cwd()

    for dirpath, _, filenames in os.walk(base_path):
        current_dir = Path(dirpath)
        relative_dir_path = current_dir.relative_to(cwd).as_posix()

        # --- README file processing ---
        readme_files = sorted([
            f for f in filenames if f.lower().startswith('readme')
        ])

        if readme_files:
            all_readme_contents = []
            for fname in readme_files:
                try:
                    content = (current_dir / fname).read_text(encoding='utf-8', errors='ignore')
                    all_readme_contents.append(content)
                except Exception as e:
                    print(f"Warning: Could not read {current_dir / fname}: {e}")
            
            if all_readme_contents:
                aggregated_content = README_SEPARATOR.join(all_readme_contents)
                conn.execute(
                    "INSERT INTO dir_info (path, readme_contents) VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET readme_contents = excluded.readme_contents",
                    (relative_dir_path, aggregated_content)
                )

        # --- .c, .h file processing ---
        for filename in filenames:
            if filename.endswith(('.c', '.h')):
                file_path = current_dir / filename
                relative_file_path = file_path.relative_to(cwd).as_posix()
                
                comment = extract_header_comment(file_path)
                if comment:
                    conn.execute(
                        "INSERT INTO file_info (path, header_comment) VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET header_comment = excluded.header_comment",
                        (relative_file_path, comment)
                    )

def main():
    """Main processing"""
    print("Starting codebase analysis script.")
    try:
        with duckdb.connect(DB_FILE) as conn:
            setup_database(conn)
            
            for dir_name in TARGET_DIRS:
                absolute_path = Path.cwd() / dir_name
                process_directory(absolute_path, conn)

        print("\nAnalysis complete.")
        print(f"Results have been stored in '{DB_FILE}'.")

    except duckdb.Error as e:
        print(f"A database error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

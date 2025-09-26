import duckdb
from pathlib import Path
from collections import defaultdict
import sys
from datetime import datetime

# --- Configuration ---
DOCS_DB_FILE = Path("data/documents.duckdb")
# Assume global_symbols.db is in the current directory as requested
GLOBAL_DB_FILE = Path("global_symbols.db")
OUTPUT_DIR = Path("generated_docs")
DOCS_TABLE_NAME = "documents"
SYMBOLS_TABLE_NAME = "symbol_definitions"

def get_first_alnum_char(text: str) -> str:
    """
    Returns the first alphanumeric character in a string.
    Returns '_' if no alphanumeric character is found.
    This is case-sensitive.
    """
    for char in text:
        if char.isalnum():
            return char
    return "_" # Default directory if no alphanumeric character is found

def load_symbol_locations(db_path: Path) -> dict:
    """
    Loads all symbol definitions from global_symbols.db into a dictionary
    for quick lookups.
    """
    print(f"Loading symbol locations from '{db_path}'...")
    locations = {}
    try:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            # Fetch all necessary fields from the symbol_definitions table
            results = conn.execute(
                f"SELECT id, file_path, line_num_start, line_num_end FROM {SYMBOLS_TABLE_NAME}"
            ).fetchall()
            for sid, file_path, start, end in results:
                locations[sid] = {
                    "file_path": file_path,
                    "line_num_start": start,
                    "line_num_end": end,
                }
    except duckdb.Error as e:
        print(f"Error: Could not read from '{db_path}'. {e}")
        sys.exit(1)
    
    print(f"Loaded {len(locations)} symbol locations into memory.")
    return locations

def insert_location_section(content: str, symbol_name: str, location_info: dict) -> str:
    """
    Inserts a '## Location' section into the document content.
    The section is placed right after the main symbol header (e.g., '# SymbolName').
    """
    header_line = f"# {symbol_name}"
    lines = content.splitlines()
    
    try:
        # Find the index of the main header line
        header_index = lines.index(header_line)
    except ValueError:
        # If header is not found, print a warning and return original content
        print(f"Warning: Could not find header '{header_line}' for insertion. Skipping location section.")
        return content

    # Format the new section to be inserted
    location_section = [
        "",  # Empty line for spacing
        "## Location",
        (f"{location_info['file_path']}: {location_info['line_num_start']} - "
         f"{location_info['line_num_end']}"),
    ]

    # Insert the new section into the list of lines
    lines[header_index + 1:header_index + 1] = location_section
    
    return "\n".join(lines)


def main():
    """
    Main function to execute the script.
    """
    # 1. Create the output directory if it does not exist
    print(f"Ensuring output directory '{OUTPUT_DIR}' exists...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Check if both database files exist
    if not DOCS_DB_FILE.exists():
        print(f"Error: Documents database not found at '{DOCS_DB_FILE}'")
        sys.exit(1)
    if not GLOBAL_DB_FILE.exists():
        print(f"Error: Global symbols database not found at '{GLOBAL_DB_FILE}'")
        sys.exit(1)

    # Pre-load all symbol locations from the global symbols database
    symbol_locations = load_symbol_locations(GLOBAL_DB_FILE)

    # Dictionary to count the number of files per directory
    dir_counts = defaultdict(int)
    
    # prepare log file for recording generated symbols with timestamp suffix    
    log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = f"generation_log_{ log_timestamp }.txt"
    log_file = open(log_file_path, "w", encoding="utf-8")

    connection = None
    try:
        # 2. Connect to the documents DuckDB database
        print(f"Connecting to documents database '{DOCS_DB_FILE}'...")
        connection = duckdb.connect(str(DOCS_DB_FILE), read_only=True)
        
        # Fetch records including symbol_id for the lookup
        print(f"Fetching records from '{DOCS_TABLE_NAME}' table...")
        records = connection.execute(
            f"SELECT symbol_id, symbol_name, content FROM {DOCS_TABLE_NAME}"
        ).fetchall()
        
        if not records:
            print("No records found in the documents table. Exiting.")
            return

        print(f"Found {len(records)} records. Generating markdown files...")

        # 3. Loop through the records to create files
        for symbol_id, symbol_name, content in records:
            if not symbol_name or content is None:
                print(f"Skipping record with empty symbol_name or content (ID: {symbol_id}).")
                continue

            # Look up location info for the current symbol
            location_info = symbol_locations.get(symbol_id)
            modified_content = content

            if location_info:
                # Insert the location section into the content
                modified_content = insert_location_section(content, symbol_name, location_info)
            else:
                print(f"Warning: No location info found for symbol '{symbol_name}' (ID: {symbol_id}).")

            # Determine the subdirectory name
            subdir_char = get_first_alnum_char(symbol_name)
            subdir_path = OUTPUT_DIR / subdir_char
            subdir_path.mkdir(exist_ok=True)
            
            # Create the full file path
            file_name = f"{symbol_name}.md"
            file_path = subdir_path / file_name
            
            # avoid overwriting existing files
            if file_path.exists():
                print(f"Warning: File '{file_path}' already exists. Skipping to avoid overwrite.")
                continue
            
            try:
                # Write the modified content to the file
                file_path.write_text(modified_content, encoding='utf-8')
                dir_counts[subdir_char] += 1
            except Exception as e:
                print(f"Warning: Could not write file for '{symbol_name}'. Error: {e}")

            # Log the generated symbol
            log_file.write(f"{symbol_name}\n")

        print("\nAll markdown files have been created successfully.")

    except duckdb.Error as e:
        print(f"An error occurred with the DuckDB database: {e}")
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

    # 4. Print the file count for each directory
    if dir_counts:
        print("\n--- File Counts per Directory ---")
        for dir_name in sorted(dir_counts.keys()):
            count = dir_counts[dir_name]
            print(f"  Directory '{dir_name}': {count} files")
    else:
        print("\nNo files were generated.")


if __name__ == "__main__":
    main()

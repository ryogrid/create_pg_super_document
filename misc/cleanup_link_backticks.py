import re
import sys
from pathlib import Path

# --- Configuration ---
# The root directory to scan for markdown files.
ROOT_DIR = Path("generated_docs")

def load_symbol_names(symbol_file_path):
    """
    Load symbol names from a text file, one symbol per line.
    Returns a set of symbol names (without .md extension).
    """
    try:
        with open(symbol_file_path, 'r', encoding='utf-8') as f:
            # Read lines, strip whitespace, and filter out empty lines
            symbols = {line.strip() for line in f if line.strip()}
            return symbols
    except Exception as e:
        print(f"Error reading symbol file '{symbol_file_path}': {e}")
        sys.exit(1)

def get_markdown_files_to_process(symbols=None):
    """
    Get the list of markdown files to process.
    If symbols is provided, only return files corresponding to those symbols.
    Otherwise, return all .md files in the directory.
    """
    if symbols is None:
        # Process all markdown files
        return list(ROOT_DIR.rglob('*.md'))
    
    files_to_process = []
    for symbol in symbols:
        # Try to find the markdown file for this symbol
        # Look for files with the symbol name (case-sensitive)
        symbol_file = ROOT_DIR / f"{symbol}.md"
        if symbol_file.exists():
            files_to_process.append(symbol_file)
        else:
            # Also search recursively in subdirectories
            found_files = list(ROOT_DIR.rglob(f"{symbol}.md"))
            if found_files:
                files_to_process.extend(found_files)
            else:
                print(f"Warning: No markdown file found for symbol '{symbol}'")
    
    return files_to_process

def main():
    """
    Main function to find and remove backticks surrounding markdown links.
    
    Usage:
    - python cleanup_link_backticks.py                    : Process all markdown files
    - python cleanup_link_backticks.py symbols.txt       : Process only files for symbols listed in symbols.txt
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory not found at '{ROOT_DIR}'. Please run this script from the correct location.")
        return

    # Check if a symbol file was provided as command line argument
    symbols = None
    if len(sys.argv) > 1:
        symbol_file_path = sys.argv[1]
        print(f"Loading symbol names from '{symbol_file_path}'...")
        symbols = load_symbol_names(symbol_file_path)
        print(f"Loaded {len(symbols)} symbol names.")
        print(f"Processing only markdown files for specified symbols...")
    else:
        print(f"Scanning all markdown files in '{ROOT_DIR}'...")

    # Get the list of files to process
    files_to_process = get_markdown_files_to_process(symbols)
    
    if not files_to_process:
        print("No markdown files found to process.")
        return

    # Regex to find a markdown link enclosed in backticks.
    # - `\``: Matches the opening backtick.
    # - `(`: Starts a capturing group. We want to keep what's inside.
    # - `\[.*?\]`: Matches the link text part (e.g., [HandleChildCrash]).
    # - `\(.*?\) `: Matches the link path part (e.g., (../H/HandleChildCrash.md)).
    # - `)`: Ends the capturing group.
    # - `\``: Matches the closing backtick.
    # The replacement `r'\1'` will replace the entire match with just the content
    # of the first capturing group (the link itself).
    pattern = re.compile(r"`(\[.*?\]\(.*?\))`")

    # --- Counters for the final report ---
    files_scanned = 0
    files_changed = 0
    backticks_removed_total = 0

    # Process the selected files
    for file_path in files_to_process:
        files_scanned += 1
        try:
            # Read the original content of the file
            content = file_path.read_text(encoding='utf-8')

            # Use re.subn which returns the new string and the number of substitutions made
            updated_content, num_replacements = pattern.subn(r'\1', content)

            # If any replacements were made, the content will be different
            if num_replacements > 0:
                files_changed += 1
                backticks_removed_total += num_replacements
                
                # Write the modified content back to the file
                file_path.write_text(updated_content, encoding='utf-8')
                
        except Exception as e:
            print(f"Error processing file '{file_path}': {e}")

        if files_scanned % 500 == 0:
            print(f"  ...scanned {files_scanned} files...")

    print("\n--- Cleanup Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print(f"Total instances of backticks removed: {backticks_removed_total}")


if __name__ == "__main__":
    main()
    
import re
import sys
from pathlib import Path

# --- Configuration ---
# The root directory to scan for markdown files.
ROOT_DIR = Path("generated_docs")
# The specific commit hash for the GitHub link
GITHUB_COMMIT_HASH = "92268b35d04c2de416279f187d12f264afa22614"
# The base URL for the GitHub repository
GITHUB_BASE_URL = f"https://github.com/postgres/postgres/tree/{GITHUB_COMMIT_HASH}"


def create_github_link_and_clean_text(match: re.Match) -> str:
    """
    This function is used as a replacement for re.sub.
    It takes a regex match, constructs the GitHub link, and also
    removes whitespace from the visible link text.
    """
    # The match object contains groups captured by the regex:
    # group(1) will be the '## Location' header and newlines.
    # group(2) will be the original location line, e.g., "src/bin/...: 1753 - 2354"
    header_part = match.group(1)
    location_line = match.group(2)

    try:
        # 1. Create the "clean" link text by removing all spaces from the original line.
        link_text = location_line.replace(' ', '')

        # 2. Split the original location line to get parts for the URL.
        file_path_str, line_numbers_str = location_line.split(':', 1)
        start_line_str, end_line_str = line_numbers_str.split('-', 1)

        # Clean up any extra whitespace from the parts before building the URL.
        file_path = file_path_str.strip()
        start_line = start_line_str.strip()
        end_line = end_line_str.strip()

        # 3. Construct the final GitHub URL. GitHub uses #L<start>-L<end> for line ranges.
        github_url = f"{GITHUB_BASE_URL}/{file_path}#L{start_line}-L{end_line}"

        # 4. Return the new structure using the cleaned link_text for display.
        return f"{header_part}[{link_text}]({github_url})"

    except ValueError:
        # If the line doesn't split correctly, it's not in the expected format.
        # Return the original matched string without any changes.
        return match.group(0)


def load_symbol_list(file_path: str) -> set:
    """
    Load symbol names from a text file, one per line.
    Returns a set of symbol names.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read lines, strip whitespace, and filter out empty lines
            symbols = {line.strip() for line in f if line.strip()}
        print(f"Loaded {len(symbols)} symbols from '{file_path}'")
        return symbols
    except Exception as e:
        print(f"Error reading symbol file '{file_path}': {e}")
        sys.exit(1)


def main():
    """
    Main function to find and update location lines with GitHub links.
    """
    # Check for command line argument
    target_symbols = None
    if len(sys.argv) > 1:
        symbol_file = sys.argv[1]
        target_symbols = load_symbol_list(symbol_file)
        print(f"Processing only files for symbols specified in '{symbol_file}'")
    else:
        print("Processing all markdown files in the directory")

    if not ROOT_DIR.is_dir():
        print(f"Error: Directory not found at '{ROOT_DIR}'. Please run this script from the correct location.")
        return

    print(f"Scanning markdown files in '{ROOT_DIR}' to add and clean location links...")

    # Regex to find the "Location" section and the specific line format.
    # This remains the same as it correctly identifies the target line.
    pattern = re.compile(r"(^## Location\s*\n+)([^:\n]+:\s*\d+\s*-\s*\d+)", flags=re.MULTILINE)

    # --- Counters for the final report ---
    files_scanned = 0
    files_changed = 0
    links_created = 0

    # Recursively find all .md files in the directory
    for file_path in ROOT_DIR.rglob('*.md'):
        # If target symbols are specified, check if this file should be processed
        if target_symbols is not None:
            # Extract symbol name from filename (remove .md extension)
            symbol_name = file_path.stem
            if symbol_name not in target_symbols:
                continue  # Skip this file

        files_scanned += 1
        try:
            content = file_path.read_text(encoding='utf-8')

            # Use re.subn with our updated replacer function
            updated_content, num_replacements = pattern.subn(create_github_link_and_clean_text, content)

            if num_replacements > 0:
                files_changed += 1
                links_created += num_replacements
                
                # Write the modified content back to the file
                file_path.write_text(updated_content, encoding='utf-8')
                
        except Exception as e:
            print(f"Error processing file '{file_path}': {e}")

        if files_scanned % 500 == 0:
            print(f"  ...scanned {files_scanned} files...")

    print("\n--- Link Update Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print(f"Total location links created/updated: {links_created}")


if __name__ == "__main__":
    main()

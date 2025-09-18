import os
import re
from pathlib import Path
from collections import defaultdict

# --- Configuration ---
# The root directory containing the markdown files.
ROOT_DIR = Path("generated_docs")

def get_first_alnum_char(text: str) -> str:
    """
    Returns the first alphanumeric character in a string.
    Returns '_' if no alphanumeric character is found.
    This is case-sensitive. This logic must match how the files were originally created.
    """
    for char in text:
        if char.isalnum():
            return char
    return "_"

def build_symbol_map(root_dir: Path) -> dict:
    """
    Scans the directory and builds a map of symbol_name -> absolute_file_path.
    """
    print(f"Scanning '{root_dir}' to build a symbol map...")
    if not root_dir.is_dir():
        print(f"Error: Directory not found at '{root_dir}'")
        return {}
        
    symbol_map = {}
    # Use rglob to recursively find all .md files
    for file_path in root_dir.rglob('*.md'):
        # The symbol name is the filename without the .md extension
        symbol_name = file_path.stem
        symbol_map[symbol_name] = file_path
    
    print(f"Found {len(symbol_map)} markdown files.")
    return symbol_map

def main():
    """
    Main function to update links in all markdown files.
    """
    symbol_to_path_map = build_symbol_map(ROOT_DIR)
    if not symbol_to_path_map:
        return

    print("\nStarting to process files and update links...")
    files_processed = 0
    links_updated = 0
    files_changed = set()

    # The regex will find list items that contain a potential symbol.
    # It captures the prefix (like '- '), the symbol name, and an optional backtick.
    # This handles both `SymbolName` and SymbolName.
    # We look for words that could be C identifiers.
    pattern = re.compile(r"(- \s*`?)([a-zA-Z_][a-zA-Z0-9_]*)(`?)")

    for current_file_path in symbol_to_path_map.values():
        files_processed += 1
        try:
            content = current_file_path.read_text(encoding='utf-8')
            original_content = content
            
            # We only want to modify the content within the "Dependencies" section.
            if '## Dependencies' in content:
                # Split the content into two parts: before and after the section header
                parts = content.split('## Dependencies', 1)
                head = parts[0] + '## Dependencies'
                deps_section = parts[1]
                
                # This function will be called for each match found in the deps_section
                def create_link(match):
                    nonlocal links_updated
                    prefix, symbol_name, suffix = match.groups()
                    
                    # Check if the found symbol exists in our map
                    if symbol_name in symbol_to_path_map:
                        target_path = symbol_to_path_map[symbol_name]
                        
                        # Calculate the relative path from the current file's directory
                        # to the target file.
                        relative_path = os.path.relpath(target_path, start=current_file_path.parent)
                        
                        # Return the full markdown link
                        links_updated += 1
                        files_changed.add(current_file_path)
                        return f"{prefix}[{symbol_name}]({relative_path}){suffix}"
                    else:
                        # If the symbol is not in our map, return it unchanged
                        return match.group(0)

                # Perform the substitution only on the dependencies section
                updated_deps_section = pattern.sub(create_link, deps_section)
                
                # Reassemble the full content
                content = head + updated_deps_section

            # If the content has changed, write it back to the file
            if content != original_content:
                current_file_path.write_text(content, encoding='utf-8')
                
        except Exception as e:
            print(f"Error processing file '{current_file_path}': {e}")
            
        if files_processed % 500 == 0:
            print(f"  ...processed {files_processed} files...")

    print("\n--- Update Complete ---")
    print(f"Total files scanned: {files_processed}")
    print(f"Total files modified: {len(files_changed)}")
    print(f"Total links created: {links_updated}")


if __name__ == "__main__":
    main()
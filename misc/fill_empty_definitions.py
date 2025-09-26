import re
from pathlib import Path
import sys

# --- Configuration ---
ROOT_DIR = Path("generated_docs")
# Max number of lines to search upwards from the original start line
MAX_RETRY_SEARCH = 20 
# How many extra lines to read past the original end line, to handle multiline definitions
END_LINE_BUFFER = 30

def load_target_symbols(file_path: str) -> set:
    """
    Load a list of symbol names from the specified file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            symbols = {line.strip() for line in f if line.strip()}
        return symbols
    except Exception as e:
        print(f"Error: Could not read symbol list file '{file_path}': {e}")
        sys.exit(1)

def parse_markdown_file(content: str):
    """
    Parses the markdown content to find the symbol name, location,
    and check if the Definition section is empty.
    An empty section is one with only whitespace between its header and the next.
    """
    def_header_match = re.search(r"^## Definition\s*$", content, flags=re.MULTILINE)
    if not def_header_match:
        return None

    start_pos = def_header_match.end()
    next_header_match = re.search(r"^## \w+", content[start_pos:], flags=re.MULTILINE)
    
    replace_end_pos = 0
    content_between = ""
    if next_header_match:
        end_pos = start_pos + next_header_match.start()
        content_between = content[start_pos:end_pos]
        replace_end_pos = end_pos
    else:
        content_between = content[start_pos:]
        replace_end_pos = len(content)

    if content_between.strip():
        return None

    symbol_match = re.search(r"^#\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$", content, flags=re.MULTILINE)
    location_match = re.search(
        r"^## Location\s*\n.*?([a-zA-Z0-9_./-]+):\s*(\d+)\s*-\s*(\d+)", 
        content, 
        flags=re.MULTILINE
    )

    if not symbol_match or not location_match:
        return None

    return {
        "symbol_name": symbol_match.group(1),
        "file_path": Path(location_match.group(1)),
        "start_line": int(location_match.group(2)),
        "end_line": int(location_match.group(3)),
        "replace_start_pos": def_header_match.start(),
        "replace_end_pos": replace_end_pos,
    }

def extract_definition(source_lines: list, start_idx: int, end_idx: int, symbol_name: str):
    """
    Extracts a C definition from a slice of source code lines.
    Returns None if the definition appears incomplete, allowing the caller to retry with a larger slice.
    """
    code_slice = source_lines[start_idx : end_idx + END_LINE_BUFFER]
    full_text = "".join(code_slice).strip()

    # Rule 1: Handle #define macros
    if full_text.startswith('#define'):
        definition_lines = []
        for line in code_slice:
            stripped_line = line.rstrip()
            definition_lines.append(stripped_line)
            if not stripped_line.endswith('\\'):
                break
        return "\n".join(definition_lines).strip()

    # Rule 2: Handle typedef, struct, enum, union
    type_keywords = ['typedef', 'struct', 'enum', 'union']
    text_to_search = "".join(code_slice)
    start_keyword_pos = -1
    for keyword in type_keywords:
        pos = text_to_search.find(keyword)
        if pos != -1 and (start_keyword_pos == -1 or pos < start_keyword_pos):
            start_keyword_pos = pos
            
    if start_keyword_pos != -1:
        search_text = text_to_search[start_keyword_pos:]
        brace_level = 0
        end_pos = -1
        for i, char in enumerate(search_text):
            if char == '{': brace_level += 1
            elif char == '}': brace_level -= 1
            elif char == ';' and brace_level <= 0:
                end_pos = i
                break
        if end_pos != -1:
            return search_text[:end_pos + 1].strip()

    # Rule 3: Handle function signatures using the new, more robust heuristic
    brace_pos = full_text.find('{')
    if brace_pos != -1:
        signature_candidate = full_text[:brace_pos].strip()
        
        # Basic validation: must contain the symbol name and balanced parentheses
        if (symbol_name not in signature_candidate or
            signature_candidate.count('(') == 0 or
            signature_candidate.count('(') != signature_candidate.count(')')):
            return None

        # **CRITICAL HEURISTIC**: If the symbol name is on the very first line
        # of our candidate text, we assume the return type is missing.
        # We signal this by returning None, forcing the caller to search higher.
        first_line = signature_candidate.splitlines()[0]
        if symbol_name in first_line:
            return None # Incomplete, need to expand the search window upwards.

        # If the symbol is NOT on the first line, we assume the lines above it
        # contain the return type, so the signature is complete.
        return signature_candidate
    
    return None


def main():
    """
    Main function: scans files and fills in empty '## Definition' sections.
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory '{ROOT_DIR}' not found. Please run this script from the correct location.")
        return

    # Check if a symbol list file is specified as command line argument
    target_symbols = None
    if len(sys.argv) > 1:
        symbol_list_file = sys.argv[1]
        target_symbols = load_target_symbols(symbol_list_file)
        print(f"Loaded {len(target_symbols)} target symbols from '{symbol_list_file}'")
        print(f"Scanning markdown files in '{ROOT_DIR}' to fill empty definitions for specified symbols...")
    else:
        print(f"Scanning markdown files in '{ROOT_DIR}' to fill empty definitions...")
    
    files_scanned = 0
    files_changed = 0
    files_failed = 0

    for md_path in ROOT_DIR.rglob("*.md"):
        files_scanned += 1
        try:
            content = md_path.read_text(encoding='utf-8')
            
            info = parse_markdown_file(content)
            if not info:
                continue

            # If target symbols are specified, process only those symbols
            if target_symbols is not None and info["symbol_name"] not in target_symbols:
                continue

            if not info["file_path"].exists():
                print(f"Warning: Source file for '{info['symbol_name']}' not found: {info['file_path']}")
                files_failed += 1
                continue
            
            source_lines = info["file_path"].read_text(encoding='utf-8', errors='ignore').splitlines(True)
            
            definition = None
            original_start_idx = info["start_line"] - 1 # Convert to 0-based index
            original_end_idx = info["end_line"] - 1

            for i in range(MAX_RETRY_SEARCH):
                current_start_idx = original_start_idx - i
                if current_start_idx < 0:
                    break
                
                definition = extract_definition(source_lines, current_start_idx, original_end_idx, info["symbol_name"])
                if definition:
                    break # Found a valid and complete definition, stop searching
            
            if definition:
                print(f"  -> Inserting definition for '{info['symbol_name']}' into {md_path.name}")
                new_section_content = f"## Definition\n\n```c\n{definition}\n```\n\n"
                
                before_section = content[:info["replace_start_pos"]]
                after_section = content[info["replace_end_pos"]:]
                
                if after_section.startswith("\n"):
                    after_section = after_section[1:]
                
                updated_content = before_section + new_section_content.rstrip() + "\n" + after_section
                
                md_path.write_text(updated_content, encoding='utf-8')
                files_changed += 1
            else:
                print(f"Warning: Could not extract a valid definition for '{info['symbol_name']}' after {MAX_RETRY_SEARCH} retries.")
                files_failed += 1

        except Exception as e:
            print(f"An error occurred while processing '{md_path}': {e}")
            files_failed += 1

    print("\n--- Definition Fill Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print(f"Files failed to process or did not need updates: {files_failed}")

if __name__ == "__main__":
    main()
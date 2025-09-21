import re
from pathlib import Path

# --- Configuration ---
# The root directory to scan for markdown files.
ROOT_DIR = Path("generated_docs")

def main():
    """
    Main function to find and remove backticks surrounding markdown links.
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory not found at '{ROOT_DIR}'. Please run this script from the correct location.")
        return

    print(f"Scanning all markdown files in '{ROOT_DIR}'...")

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

    # Recursively find all .md files in the directory
    for file_path in ROOT_DIR.rglob('*.md'):
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
    
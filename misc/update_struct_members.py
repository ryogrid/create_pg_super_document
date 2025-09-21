import re
from pathlib import Path

# --- Configuration ---
ROOT_DIR = Path("generated_docs")

def extract_struct_members(code: str) -> list or None:
    """
    Extracts member variable names from a struct definition in a C code block.
    This version correctly handles complex declarations with spaces in array specifiers.
    """
    body_match = re.search(r"\{([\s\S]*?)\}", code)
    if not body_match:
        return None
    
    struct_body = body_match.group(1)
    members = []
    # Remove block comments to simplify parsing
    struct_body = re.sub(r'/\*[\s\S]*?\*/', '', struct_body)
    
    for line in struct_body.splitlines():
        # Remove line comments
        line = line.split('//')[0].strip()
        # Ignore empty lines or preprocessor directives
        if not line or line.startswith('#'):
            continue
        
        if line.endswith(';'):
            declaration = line[:-1].strip()
            array_bracket_pos = declaration.find('[')
            
            full_member_name = ""
            if array_bracket_pos != -1:
                before_array = declaration[:array_bracket_pos].strip()
                array_part = declaration[array_bracket_pos:]
                base_name = before_array.split()[-1]
                full_member_name = base_name + array_part
            else:
                parts = declaration.split()
                if parts:
                    full_member_name = parts[-1]

            if full_member_name:
                members.append(full_member_name)

    return members if members else None


def main():
    """
    Main function to scan files and update member variable names with detailed skip reporting.
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory '{ROOT_DIR}' not found. Please run this script from the correct location.")
        return

    print(f"Scanning markdown files in '{ROOT_DIR}' to update struct member names...")

    files_scanned = 0
    files_changed = 0
    # Detailed skip counters
    skipped_no_members = 0
    skipped_mismatch = 0
    skipped_annotated = 0
    skipped_other_errors = 0
    
    definition_pattern = re.compile(r"## Definition\s*\n+```c\n([\s\S]*?)\n```", flags=re.DOTALL)
    params_header_pattern = re.compile(r"^## Parameters / Member Variables\s*$", flags=re.MULTILINE)
    next_header_pattern = re.compile(r"^## \w+", flags=re.MULTILINE)

    for md_path in ROOT_DIR.rglob("*.md"):
        files_scanned += 1
        try:
            content = md_path.read_text(encoding='utf-8')
            
            def_match = definition_pattern.search(content)
            params_header_match = params_header_pattern.search(content)

            if not def_match or not params_header_match:
                continue

            definition_code = def_match.group(1)

            if 'struct' not in definition_code:
                continue

            # --- Parameter line extraction ---
            params_section_start = params_header_match.end()
            next_header_match = next_header_pattern.search(content, pos=params_section_start)
            
            params_block_original = ""
            if next_header_match:
                params_section_end = next_header_match.start()
                params_block_original = content[params_section_start:params_section_end]
            else:
                params_section_end = len(content)
                params_block_original = content[params_section_start:]

            param_lines = [
                line.strip() for line in params_block_original.strip().splitlines()
                if line.strip().startswith('-')
            ]
            
            # --- Validation Stages ---

            # Case 1: No members found in the definition (e.g., typedef struct a b;)
            members = extract_struct_members(definition_code)
            if not members:
                print(f"  - Skipping '{md_path.name}': No member variables found in the struct definition.")
                skipped_no_members += 1
                continue
            
            # Case 3 (Mismatch): Member count does not match parameter line count
            if len(members) != len(param_lines):
                print(f"  - Skipping '{md_path.name}': Mismatch! Found {len(members)} members but {len(param_lines)} descriptions.")
                skipped_mismatch += 1
                continue
            
            # Case 2 (Already Annotated): Check if parameters are already filled in
            is_already_annotated = False
            for line in param_lines:
                if ':' not in line:
                    is_already_annotated = True # Should not happen with current logic, but a good safeguard
                    break
                
                left_part = line.split(':', 1)[0]
                validation_str = left_part.lstrip('-').strip()
                if validation_str:
                    is_already_annotated = True
                    break
            
            if is_already_annotated:
                print(f"  - Skipping '{md_path.name}': Parameters appear to be already annotated or manually formatted.")
                skipped_annotated += 1
                continue

            # --- Processing ---
            
            print(f"  -> Processing '{md_path.name}': Found {len(members)} matching and valid descriptions.")
            
            updated_param_lines = []
            for i, member_name_from_code in enumerate(members):
                original_line = param_lines[i]
                right_part = original_line.split(':', 1)[1]
                cleaned_member_name = member_name_from_code.replace('/', '')
                new_line = f"- `{cleaned_member_name}`:{right_part}"
                updated_param_lines.append(new_line)

            new_params_block = "\n" + "\n".join(updated_param_lines) + "\n"
            
            before_section = content[:params_section_start]
            after_section = content[params_section_end:]
            updated_content = before_section + new_params_block + after_section

            if updated_content != content:
                md_path.write_text(updated_content, encoding='utf-8')
                files_changed += 1

        except Exception as e:
            print(f"An error occurred while processing '{md_path}': {e}")
            skipped_other_errors += 1

    print("\n--- Member Name Update Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print("\nBreakdown of skipped files:")
    print(f"  - Skipped (no members found in definition): {skipped_no_members}")
    print(f"  - Skipped (member/parameter count mismatch): {skipped_mismatch}")
    print(f"  - Skipped (already annotated or manually formatted): {skipped_annotated}")
    print(f"  - Skipped (other errors): {skipped_other_errors}")


if __name__ == "__main__":
    main()

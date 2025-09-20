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
    struct_body = re.sub(r'/\*[\s\S]*?\*/', '', struct_body)
    
    for line in struct_body.splitlines():
        line = line.split('//')[0].strip()
        if not line or line.startswith('#'):
            continue
        
        if line.endswith(';'):
            declaration = line[:-1].strip()
            
            # --- NEW, MORE ROBUST PARSING LOGIC ---
            
            # Find the start of an array specifier, if any
            array_bracket_pos = declaration.find('[')
            
            full_member_name = ""
            if array_bracket_pos != -1:
                # Case: This is an array declaration, e.g., "unsigned statesarea[...]"
                
                # Part before the array bracket, e.g., "unsigned statesarea"
                before_array = declaration[:array_bracket_pos].strip()
                
                # The rest of the string is the array part, e.g., "[FEWSTATES * 2 + WORK]"
                array_part = declaration[array_bracket_pos:]
                
                # The base name is the last word before the array part
                base_name = before_array.split()[-1]
                
                full_member_name = base_name + array_part
            else:
                # Case: This is a simple variable or pointer, e.g., "int member" or "Type *ptr"
                # The last whitespace-separated word is the name.
                parts = declaration.split()
                if parts:
                    full_member_name = parts[-1]

            if full_member_name:
                members.append(full_member_name)

    return members if members else None


def main():
    """
    Main function to scan files and update member variable names.
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory '{ROOT_DIR}' not found. Please run this script from the correct location.")
        return

    print(f"Scanning markdown files in '{ROOT_DIR}' to update struct member names...")

    files_scanned = 0
    files_changed = 0
    files_skipped = 0
    
    definition_pattern = re.compile(r"## Definition\s*\n+```c\n(struct[\s\S]*?)\n```", flags=re.DOTALL)
    params_pattern = re.compile(r"(^## Parameters / Member Variables\s*\n)((?:^\s*-.*\n)+)", flags=re.MULTILINE)

    for md_path in ROOT_DIR.rglob("*.md"):
        files_scanned += 1
        try:
            content = md_path.read_text(encoding='utf-8')
            
            def_match = definition_pattern.search(content)
            params_match = params_pattern.search(content)

            if not def_match or not params_match:
                continue

            definition_code = def_match.group(1)
            params_block = params_match.group(2)
            
            members = extract_struct_members(definition_code)
            param_lines = [line.strip() for line in params_block.strip().splitlines()]

            if not members:
                continue

            if len(members) != len(param_lines):
                print(f"  - Skipping '{md_path.name}': Mismatch! Found {len(members)} members but {len(param_lines)} descriptions.")
                files_skipped += 1
                continue

            is_valid_for_update = True
            for line in param_lines:
                if ':' not in line:
                    print(f"  - Skipping '{md_path.name}': Line is missing a colon: '{line}'")
                    is_valid_for_update = False
                    break
                
                left_part = line.split(':', 1)[0]
                validation_str = left_part.lstrip('-').strip()
                if validation_str:
                    print(f"  - Skipping '{md_path.name}': Unexpected content found before colon: '{validation_str}'")
                    is_valid_for_update = False
                    break
            
            if not is_valid_for_update:
                files_skipped += 1
                continue

            print(f"  -> Processing '{md_path.name}': Found {len(members)} matching and valid descriptions.")
            
            updated_param_lines = []
            for i, member_name_from_code in enumerate(members):
                original_line = param_lines[i]
                right_part = original_line.split(':', 1)[1]
                cleaned_member_name = member_name_from_code.replace('/', '')
                new_line = f"- `{cleaned_member_name}`:{right_part}"
                updated_param_lines.append(new_line)

            new_params_block = "\n".join(updated_param_lines) + "\n"
            updated_content = content.replace(params_block, new_params_block, 1)

            md_path.write_text(updated_content, encoding='utf-8')
            files_changed += 1

        except Exception as e:
            print(f"An error occurred while processing '{md_path}': {e}")
            files_skipped += 1

    print("\n--- Member Name Update Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print(f"Total files skipped (due to mismatch, invalid format, or errors): {files_skipped}")


if __name__ == "__main__":
    main()
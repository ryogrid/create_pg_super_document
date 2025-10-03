import re
import sys
from pathlib import Path

# --- Configuration ---
ROOT_DIR = Path("generated_docs")

def extract_function_arguments(code: str) -> list | None:
    """
    Extracts argument names from a function definition in a C code block.
    Handles both single-line and multi-line function declarations.
    Returns a list of argument names (including pointer and array notations).
    """
    # Remove block comments to simplify parsing
    code = re.sub(r'/\*[\s\S]*?\*/', '', code)
    # Remove line comments
    code = re.sub(r'//.*?$', '', code, flags=re.MULTILINE)
    
    # Find the function declaration - match from function name to closing parenthesis
    # Pattern: return_type function_name(arguments)
    func_match = re.search(r'\b\w+\s*\([^)]*\)', code, re.DOTALL)
    if not func_match:
        return None
    
    func_decl = func_match.group(0)
    
    # Extract the argument list between parentheses
    paren_match = re.search(r'\((.*?)\)', func_decl, re.DOTALL)
    if not paren_match:
        return None
    
    args_string = paren_match.group(1).strip()
    
    # Handle void or empty parameter list
    if not args_string or args_string == 'void':
        return []
    
    # Split by comma, but need to be careful with nested parentheses and function pointers
    arguments = []
    current_arg = ""
    paren_depth = 0
    
    for char in args_string:
        if char == '(':
            paren_depth += 1
            current_arg += char
        elif char == ')':
            paren_depth -= 1
            current_arg += char
        elif char == ',' and paren_depth == 0:
            arguments.append(current_arg.strip())
            current_arg = ""
        else:
            current_arg += char
    
    if current_arg.strip():
        arguments.append(current_arg.strip())
    
    # Extract the variable name from each argument declaration
    arg_names = []
    for arg in arguments:
        arg = arg.strip()
        if not arg:
            continue
        
        # Handle function pointers: return_type (*name)(args)
        func_ptr_match = re.search(r'\(\s*\*\s*(\w+)\s*\)', arg)
        if func_ptr_match:
            arg_names.append('*' + func_ptr_match.group(1))
            continue
        
        # Handle arrays: type name[size] or type name[]
        array_match = re.search(r'(\w+)\s*\[', arg)
        if array_match:
            base_name = array_match.group(1)
            array_part = arg[arg.index('['):]
            arg_names.append(base_name + array_part)
            continue
        
        # Standard case: extract the last identifier, handling pointers
        # Remove array brackets if any for processing
        arg_no_array = re.sub(r'\[.*?\]', '', arg)
        
        # Split by whitespace and operators
        tokens = re.findall(r'\*+|\w+', arg_no_array)
        if not tokens:
            continue
        
        # The variable name is typically the last token
        var_name = tokens[-1]
        
        # Count preceding asterisks
        pointer_prefix = ""
        for i in range(len(tokens) - 2, -1, -1):
            if tokens[i].startswith('*'):
                pointer_prefix = tokens[i] + pointer_prefix
            else:
                break
        
        # Check if there are array brackets in the original
        if '[' in arg:
            array_start = arg.index('[')
            array_part = arg[array_start:]
            arg_names.append(pointer_prefix + var_name + array_part)
        else:
            arg_names.append(pointer_prefix + var_name)
    
    return arg_names if arg_names else None


def main():
    """
    Main function to scan files and update function argument names with detailed skip reporting.
    """
    if not ROOT_DIR.is_dir():
        print(f"Error: Directory '{ROOT_DIR}' not found. Please run this script from the correct location.")
        return

    print(f"Scanning all markdown files in '{ROOT_DIR}' to update function argument names...")

    files_scanned = 0
    files_changed = 0
    # Detailed skip counters
    skipped_no_args = 0
    skipped_mismatch = 0
    skipped_annotated = 0
    skipped_other_errors = 0
    skipped_no_definition = 0
    skipped_non_function_definition = 0
    
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
                if not def_match:
                    skipped_no_definition += 1
                continue

            definition_code = def_match.group(1)

            # Skip if this appears to be a non-function definition (e.g., struct, typedef)
            if definition_code.strip().startswith('struct') or definition_code.strip().startswith('typedef struct'):
                skipped_non_function_definition += 1
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

            # Extract arguments from function definition
            arguments = extract_function_arguments(definition_code)
            
            # Case 1: No arguments found or void function
            if arguments is None:
                print(f"  - Skipping '{md_path.name}': Could not parse function arguments.")
                skipped_no_args += 1
                continue
            
            # If function has no parameters but has parameter lines, it's a mismatch
            if len(arguments) == 0 and len(param_lines) > 0:
                print(f"  - Skipping '{md_path.name}': Function has no parameters but {len(param_lines)} descriptions found.")
                skipped_mismatch += 1
                continue
            
            # If function has parameters but no parameter lines, skip
            if len(arguments) > 0 and len(param_lines) == 0:
                print(f"  - Skipping '{md_path.name}': Function has {len(arguments)} parameters but no descriptions found.")
                skipped_mismatch += 1
                continue
            
            # Case 2 (Mismatch): Argument count does not match parameter line count
            if len(arguments) != len(param_lines):
                print(f"  - Skipping '{md_path.name}': Mismatch! Found {len(arguments)} arguments but {len(param_lines)} descriptions.")
                skipped_mismatch += 1
                continue
            
            # Case 3 (Already Annotated): Check if parameters are already filled in
            is_already_annotated = False
            for line in param_lines:
                if ':' not in line:
                    is_already_annotated = True
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
            
            print(f"  -> Processing '{md_path.name}': Found {len(arguments)} matching and valid descriptions.")
            
            updated_param_lines = []
            for i, arg_name_from_code in enumerate(arguments):
                original_line = param_lines[i]
                right_part = original_line.split(':', 1)[1]
                cleaned_arg_name = arg_name_from_code.replace('/', '')
                new_line = f"- `{cleaned_arg_name}`:{right_part}"
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

    print("\n--- Function Argument Name Update Complete ---")
    print(f"Total files scanned: {files_scanned}")
    print(f"Total files modified: {files_changed}")
    print("\nBreakdown of skipped files:")
    print(f"  - Skipped (non-function definition): {skipped_non_function_definition}")
    print(f"  - Skipped (no definition section found): {skipped_no_definition}")
    print(f"  - Skipped (no arguments or parsing failed): {skipped_no_args}")
    print(f"  - Skipped (argument/parameter count mismatch): {skipped_mismatch}")
    print(f"  - Skipped (already annotated or manually formatted): {skipped_annotated}")
    print(f"  - Skipped (other errors): {skipped_other_errors}")


if __name__ == "__main__":
    main()

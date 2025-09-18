# check_valid_extension_name

## Location
src/backend/commands/extension.c: 266 - 312

## Overview
Validates the format and content of a PostgreSQL extension name to ensure it meets security and parsing requirements.

## Definition
```c
static void check_valid_extension_name(const char *extensionname)
```

## Detailed Description
This static validation function enforces naming rules for PostgreSQL extension names to prevent security vulnerabilities and parsing ambiguities. It performs several checks to ensure extension names are safe to use in file system operations and SQL parsing contexts.

The function validates against several potential issues:
1. **Empty names**: Prevents empty extension names which could cause parsing issues
2. **Double dashes**: Prohibits "--" sequences that would create ambiguity in script filenames (since "--" is used as a separator in extension script naming conventions)  
3. **Leading/trailing dashes**: Prevents names starting or ending with "-" to avoid visual ambiguity and parsing complications
4. **Directory separators**: Blocks any directory separator characters to prevent path traversal attacks ("../" style attacks)

Each validation failure triggers an ERROR with ERRCODE_INVALID_PARAMETER_VALUE, providing specific error messages and details about the validation rule that was violated.

## Parameters / Member Variables
- `extensionname`: The extension name string to validate (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (calculate string length)
  - strstr (search for substring)
  - first_dir_separator (find directory separator characters)
  - ereport (error reporting with detailed messages)

- Called from (representative examples):
  - CreateExtension (during extension creation)
  - get_required_extension (when resolving extension dependencies)
  - pg_extension_update_paths (when checking update path validity)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c compilation unit
- Does not return a value - either passes validation silently or throws an ERROR
- Part of PostgreSQL's security model for extensions, preventing malicious extension names
- The double-dash restriction is specifically related to PostgreSQL's extension script naming convention (extension--version.sql)
- Directory separator checking uses first_dir_separator() which is platform-aware (handles both '/' and '\' on Windows)
- Validation rules are designed to ensure extension names can be safely used as part of filenames in the file system
- All error messages include both the invalid name and a specific explanation of what rule was violated
- Essential for preventing security issues in extension loading and script file resolution
# check_valid_version_name

## Location
[src/backend/commands/extension.c:313-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L313-L359)

## Overview
Validates the format and content of a PostgreSQL extension version name to ensure it meets security and parsing requirements.

## Definition
```c
static void check_valid_version_name(const char *versionname)
```

## Detailed Description
This static validation function enforces naming rules for PostgreSQL extension version names, applying similar security and parsing constraints as check_valid_extension_name but specifically for version identifiers. Extension versions are used in script filenames and update paths, so they must be validated to prevent security vulnerabilities and parsing ambiguities.

The function validates against the same potential issues as extension names:
1. **Empty names**: Prevents empty version names which would be meaningless
2. **Double dashes**: Prohibits "--" sequences that would create ambiguity in extension script filenames (extension--version.sql format)
3. **Leading/trailing dashes**: Prevents names starting or ending with "-" to avoid visual confusion and parsing complications  
4. **Directory separators**: Blocks directory separator characters to prevent path traversal attacks

Each validation failure triggers an ERROR with ERRCODE_INVALID_PARAMETER_VALUE, providing specific error messages tailored to version name context.

## Parameters / Member Variables
- `versionname`: The extension version name string to validate (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (calculate string length)
  - strstr (search for substring)
  - [first_dir_separator](../f/first_dir_separator.md) (find directory separator characters)
  - ereport (error reporting with detailed messages)

- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (during extension creation with specific version)
  - [ExecAlterExtensionStmt](../E/ExecAlterExtensionStmt.md) (during extension version updates/alterations)

## Notes and Other Information
- This is a static function, only accessible within the extension.c compilation unit
- Companion function to check_valid_extension_name, applying the same validation rules to version names
- Does not return a value - either passes validation silently or throws an ERROR
- Critical for security in extension version management and script file resolution
- The validation rules ensure version names can be safely incorporated into extension script filenames
- Double-dash restriction prevents ambiguity in the extension--version.sql filename format
- Directory separator checking prevents malicious version names that could cause path traversal
- Version names are used in extension update paths and must be safe for file system operations
- All error messages are specifically tailored to version name context (vs generic extension name errors)
- Essential component of PostgreSQL's extension security model, preventing malicious version specifications
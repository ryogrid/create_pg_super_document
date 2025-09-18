# tokenize_include_file

## Location
[src/backend/libpq/hba.c:438-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L438-L492)

## Overview
Opens and processes an authentication configuration file that is included from another authentication file, tokenizing its contents and adding the tokens to an existing list.

## Definition


## Detailed Description
This function handles the inclusion of authentication configuration files within PostgreSQL's HBA system. It's used when processing "include", "include_if_exists", or "include_dir" directives in authentication configuration files. The function resolves the included file path relative to the outer file, opens it, and processes its entire contents by calling tokenize_auth_file. All new tokens are allocated in the dedicated tokenize_context memory context. The function provides flexible error handling - it can either require the included file to exist or optionally skip missing files based on the missing_ok parameter.

## Parameters / Member Variables
- : Path of the file that contains the include directive (used for relative path resolution)
- : Path of the file to be included (may be relative or absolute)
- : Pointer to list of token lines where included file's tokens will be added
- : Error reporting level for ereport calls (e.g., ERROR, LOG, WARNING)
- : Current recursion depth for nested includes (prevents infinite recursion)
- : If true, missing files are silently skipped; if false, missing files cause errors
- : Pointer to store error message string if processing fails

## Dependencies
- Functions called/Symbols referenced:
  - : Resolves relative paths to absolute paths
  - : Opens authentication configuration files with error handling
  - : Processes the included file's contents into tokens
  - : Closes the file and cleans up resources
  - : Reports messages at specified logging level
  - : Frees allocated memory for file paths
- Called from (representative examples):
  - : When processing "include", "include_if_exists", and "include_dir" directives

## Notes and Other Information
- Does not return a value; modifies tok_lines list in place
- Supports both required includes (error on missing) and optional includes (skip if missing)
- Handles both relative and absolute file paths correctly
- Part of PostgreSQL's flexible authentication configuration system
- All memory allocations use the tokenize_context for proper cleanup
- Provides comprehensive error reporting with file context information
- Used by include, include_if_exists, and include_dir authentication configuration directives
- Maintains proper file handle management with cleanup on all exit paths
# open_auth_file

## Location
src/backend/libpq/hba.c: 595 - 656

## Overview
Opens authentication configuration files with proper error handling, recursion depth checking, and memory context management for tokenization.

## Definition
```c
FILE *open_auth_file(const char *filename, int elevel, int depth, char **err_msg)
```

## Detailed Description
The open_auth_file function is a specialized file opening routine designed for PostgreSQL's authentication configuration files (pg_hba.conf and pg_ident.conf). It provides several key safety features:

1. **Recursion Protection**: Prevents infinite recursion by checking that the include depth doesn't exceed CONF_FILE_MAX_DEPTH
2. **Memory Context Management**: Creates a dedicated memory context (tokenize_context) when opening the top-level file for tokenization operations
3. **Comprehensive Error Handling**: Returns detailed error messages and preserves errno for caller analysis
4. **Resource Management**: Uses PostgreSQL's AllocateFile() for proper file handle management

The function is designed to work with the include directive functionality in authentication configuration files, allowing nested file inclusion while preventing stack overflow conditions.

## Parameters
- `filename`: Absolute path to the authentication configuration file to open
- `elevel`: Message logging level for error reporting (e.g., ERROR, WARNING)
- `depth`: Current recursion depth for include file processing
- `err_msg`: Output parameter for detailed error message (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile
  - AllocSetContextCreate
  - strerror
  - CONF_FILE_MAX_DEPTH
  - CONF_FILE_START_DEPTH
  - ALLOCSET_START_SMALL_SIZES
- Called from (representative examples):
  - tokenize_include_file
  - tokenize_expand_file
  - load_hba
  - load_ident
  - fill_hba_view
  - fill_ident_view

## Notes and Other Information
- Returns NULL on failure with error details stored in err_msg parameter
- The tokenize_context memory context is only created when depth equals CONF_FILE_START_DEPTH (top-level file)
- Maximum nesting depth is enforced to prevent stack overflow from circular includes
- Preserves errno for detailed error analysis by calling code
- Works in conjunction with free_auth_file() for proper resource cleanup
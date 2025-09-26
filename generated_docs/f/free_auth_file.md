# free_auth_file

## Location
[src/backend/libpq/hba.c:570-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L570-L594)

## Overview
Frees a file handle opened by open_auth_file() and manages the associated tokenization memory context cleanup.

## Definition

```c
void
free_auth_file(FILE *file, int depth)
```
## Detailed Description
The free_auth_file function is responsible for properly closing authentication configuration files that were opened using open_auth_file(). It performs two key operations:

1. Closes the file handle using PostgreSQL's FreeFile() function
2. Conditionally cleans up the global tokenization memory context when processing reaches the top level (depth equals CONF_FILE_START_DEPTH)

This function is part of PostgreSQL's authentication configuration file processing system, ensuring proper resource cleanup and memory management during the parsing of pg_hba.conf and pg_ident.conf files.

## Parameters
- : FILE pointer to the authentication configuration file to be closed
- : Integer representing the current nesting depth of file processing (used to determine when to clean up the tokenization context)

## Dependencies
- Functions called/Symbols referenced:
  - [FreeFile](../F/FreeFile.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - CONF_FILE_START_DEPTH
- Called from (representative examples):
  - [tokenize_include_file](../t/tokenize_include_file.md)
  - [tokenize_expand_file](../t/tokenize_expand_file.md)
  - [load_hba](../l/load_hba.md)
  - [load_ident](../l/load_ident.md)
  - [fill_hba_view](fill_hba_view.md)
  - [fill_ident_view](fill_ident_view.md)

## Notes and Other Information
- The depth parameter is crucial for proper memory management - the tokenization context is only deleted when depth equals CONF_FILE_START_DEPTH, indicating the completion of the entire configuration file processing
- This function works in conjunction with open_auth_file() to provide proper resource management for authentication configuration file processing
- The tokenize_context is a global memory context used during the tokenization process of authentication files
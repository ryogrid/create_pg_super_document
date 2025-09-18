# tokenize_expand_file

## Location
[src/backend/libpq/hba.c:493-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L493-L569)

## Overview
Expands a file referenced by '@' directive within an HBA configuration field into a flat list of tokens that are appended to the existing token list.

## Definition


## Detailed Description
This function handles file expansion within HBA configuration fields when a token beginning with '@' is encountered. Unlike tokenize_include_file which processes entire include directives, this function processes a file referenced within a field and flattens all its tokens into the current field's token list. It opens the referenced file, tokenizes its entire contents, and then extracts all individual tokens from every line and field, appending them to the existing tokens list. This enables constructs like "foo,bar,@filename" to work as expected, where @filename expands to multiple comma-separated values. The function handles recursive expansion, proper memory context management, and comprehensive error propagation.

## Parameters / Member Variables
- : Existing list of AuthToken structures to which new tokens will be appended
- : Path of the file containing the '@' reference (used for relative path resolution)
- : Path of the file to be expanded (may be relative or absolute)
- : Error reporting level for ereport calls (e.g., ERROR, LOG, WARNING)
- : Current recursion depth for nested expansions (prevents infinite recursion)
- : Pointer to store error message string if expansion fails

## Dependencies
- Functions called/Symbols referenced:
  - : Resolves relative file paths to absolute paths
  - : Opens authentication configuration files with error handling
  - : Processes the included file into TokenizedAuthLine structures
  - : Closes file and cleans up resources
  - : Adds tokens to the result list
  - : Manages memory allocation context
  - : Duplicates error message strings
  - : Frees allocated memory
- Called from (representative examples):
  - : When processing '@' file references within fields
  - : During token matching with file expansion

## Dependencies
- Functions called/Symbols referenced:
  - : Resolves relative file paths to absolute paths
  - : Opens authentication configuration files with error handling
  - : Processes the included file into TokenizedAuthLine structures
  - : Closes file and cleans up resources
  - : Adds tokens to the result list
  - : Manages memory allocation context
  - : Duplicates error message strings
  - : Frees allocated memory
- Called from (representative examples):
  - : When processing '@' file references within fields
  - : During token matching with file expansion

## Notes and Other Information
- Returns the modified tokens list with new tokens appended
- Supports recursive expansion if the included file contains '@' references or include directives
- Flattens multi-line, multi-field file contents into a single token list
- Proper error propagation - stops on first error encountered in any line
- Uses tokenize_context memory context for all token allocations
- Enables flexible configuration patterns like comma-separated lists spanning multiple files
- Part of PostgreSQL's authentication configuration expansion system
- Handles complex nested structures by iterating through lines, fields, and tokens
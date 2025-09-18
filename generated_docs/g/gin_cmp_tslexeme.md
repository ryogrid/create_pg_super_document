# gin_cmp_tslexeme

## Location
src/backend/utils/adt/tsginidx.c: 24 - 39

## Overview
A PostgreSQL function that compares two text search lexemes (tokens) for ordering purposes in GIN (Generalized Inverted Index) operations.

## Definition


## Detailed Description
This function serves as a comparison operator for text search lexemes within the GIN indexing framework. It extracts two text arguments, compares them using PostgreSQL's text search comparison logic, and returns an integer result indicating their relative ordering. The function is designed to support GIN index operations that require ordered comparisons of lexeme data, which is essential for efficient text search indexing and retrieval.

The function uses  internally with case-sensitive comparison (false parameter), ensuring consistent lexeme ordering within the index structure.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0):  - First lexeme to compare
  - Second argument (index 1):  - Second lexeme to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text arguments from function call
  -  - Core text search string comparison function
  -  - Extract variable-length data content
  -  - Get variable-length data size excluding header
  -  - Free copied argument data if needed
  -  - Return 32-bit integer result
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- Returns negative value if first lexeme is less than second, positive if greater, zero if equal
- Uses case-sensitive comparison for lexeme ordering
- Properly handles variable-length text data with PostgreSQL's TOAST mechanism
- Memory management includes freeing copied arguments to prevent leaks
- Part of GIN index infrastructure for full-text search capabilities
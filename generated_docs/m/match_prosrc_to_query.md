# match_prosrc_to_query

## Location
src/backend/catalog/pg_proc.c: 1069 - 1126

## Overview
Locates a function body string literal within the original CREATE FUNCTION or DO command text and maps character positions between them.

## Definition
static int match_prosrc_to_query(const char *prosrc, const char *queryText, int cursorpos)

## Detailed Description
This function attempts to find the string literal containing the function body within the original command text. It supports both dollar-quoted literals ($tag$...$tag$) and single-quoted literals ('...'). The function scans through the query text looking for matches and performs position mapping to convert a character index within the function body to the corresponding character index within the original command.

The function uses a simple scanning approach rather than full parsing, which could potentially be fooled in complex scenarios, but works reliably for typical cases. To ensure accuracy, it fails if multiple potential matches are found.

For dollar-quoted literals, position mapping is straightforward since there are no embedded escape characters. For single-quoted literals, it delegates to match_prosrc_to_literal() to handle escape sequences and quote doubling.

## Parameters / Member Variables
- `prosrc`: The function source code string to locate
- `queryText`: The original CREATE FUNCTION or DO command text  
- `cursorpos`: Character position within the prosrc string to map

## Dependencies
- Functions called/Symbols referenced:
  - pg_mbstrlen_with_len
  - match_prosrc_to_literal
- Called from (representative examples):
  - function_parse_error_transpose

## Notes and Other Information
- Returns the mapped character position in the original query, or 0 if unsuccessful
- Handles multibyte characters correctly using pg_mbstrlen_with_len()
- Fails if multiple matches are found to avoid ambiguity
- Uses simple string scanning rather than full SQL parsing for performance
- Supports both $$ and '' literal syntax commonly used for function bodies
- Character positions are counted in characters, not bytes, for proper Unicode handling
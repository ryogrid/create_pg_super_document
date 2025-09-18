# quote_literal_internal

## Location
src/backend/utils/adt/quote.c: 47 - 77

## Overview
A static helper function that performs the core logic for quoting string literals in PostgreSQL, handling escape characters and proper quote formatting for SQL strings.

## Definition


## Detailed Description
The `quote_literal_internal` function is a low-level utility that converts a source string into a properly quoted SQL string literal. It handles the complexities of SQL string escaping, including backslash escaping and quote doubling. The function is designed to work regardless of the `standard_conforming_strings` setting, ensuring consistent behavior across different PostgreSQL configurations. It scans the source string for backslashes to determine if escape string syntax is needed, then processes each character to properly escape quotes and other special characters.

## Parameters / Member Variables
- `dst`: Destination buffer where the quoted string will be written
- `src`: Source string to be quoted
- `len`: Length of the source string

## Dependencies
- Functions called/Symbols referenced:
  - `ESCAPE_STRING_SYNTAX` - Constant or macro for escape string prefix character
  - `SQL_STR_DOUBLE` - Macro to determine if a character should be doubled in SQL strings
- Called from (representative examples):
  - `[quote_literal](quote_literal.md)` - PostgreSQL function for quoting text literals
  - `[quote_literal_cstr](quote_literal_cstr.md)` - Function for quoting C string literals

## Notes and Other Information
- This is a static function, only accessible within the quote.c file
- Designed to be independent of `standard_conforming_strings` setting for maximum compatibility
- Used by external modules like dblink that need consistent string quoting behavior
- Returns the number of characters written to the destination buffer
- Part of PostgreSQL's quote utility functions located in `src/backend/utils/adt/quote.c`
- Critical for SQL injection prevention and proper string literal formatting
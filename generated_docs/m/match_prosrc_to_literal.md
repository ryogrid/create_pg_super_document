# match_prosrc_to_literal

## Location
src/backend/catalog/pg_proc.c: 1127 - 1183

## Overview
Matches function source text against a single-quoted string literal and adjusts cursor positions to account for escape sequences and quote doubling.

## Definition
static bool match_prosrc_to_literal(const char *prosrc, const char *literal, int cursorpos, int *newcursorpos)

## Detailed Description
This function performs character-by-character comparison between function source code and a single-quoted string literal, handling SQL escape sequences (backslashes and doubled quotes) that may appear in the literal but not in the source. It correctly maps character positions from the source text to the literal text, accounting for the additional escape characters.

The function processes the comparison one character at a time (not byte-wise) to ensure correct position calculations for multibyte characters. When it encounters escape sequences in the literal (\\ or ''), it advances past them and adjusts the position mapping accordingly. The function expects the literal to be properly terminated with a single quote that is not doubled.

This is a helper function used by match_prosrc_to_query() when dealing with single-quoted function body literals in CREATE FUNCTION statements.

## Parameters / Member Variables
- `prosrc`: The function source code string to match
- `literal`: Pointer to the literal text (just past the opening quote)
- `cursorpos`: Character position within prosrc to map
- `newcursorpos`: Output parameter for the adjusted cursor position

## Dependencies
- Functions called/Symbols referenced:
  - pg_mblen
- Called from (representative examples):
  - match_prosrc_to_query

## Notes and Other Information
- Returns true if the source matches the literal, false otherwise
- Handles backslash escapes (\\) and doubled quotes ('') in the literal
- Does not handle SQL syntax for literals continued across line boundaries
- Uses character-based rather than byte-based comparison for proper multibyte support
- Expects the literal to end with a single quote (not doubled)
- Always sets *newcursorpos even on failure to suppress compiler warnings
- The function performs position adjustment as it scans, tracking characters before the cursor
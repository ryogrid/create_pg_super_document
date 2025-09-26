# next_insert

## Location
[src/interfaces/ecpg/ecpglib/execute.c:111-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L111-L147)

## Overview
A static parsing function that locates the next parameter placeholder position in an SQL statement text, handling both PostgreSQL-style ($n) and question mark (?) placeholders.

## Definition
```c
static int next_insert(char *text, int pos, bool questionmarks, bool std_strings)
```

## Detailed Description
The `next_insert` function scans SQL statement text starting from a given position to find the next parameter placeholder. It handles two types of placeholders: PostgreSQL-style numbered parameters ($1, $2, etc.) and question mark placeholders (?). The function carefully navigates string literals to avoid matching placeholders that appear inside quoted strings, and handles escape sequences appropriately based on the `std_strings` setting. It distinguishes between parameter placeholders and PostgreSQL dollar-quoted strings by checking the character pattern following the dollar sign.

## Parameters / Member Variables
- `text`: The SQL statement text to search through
- `pos`: Starting position in the text for the search
- `questionmarks`: Boolean flag indicating whether to recognize '?' as placeholders
- `std_strings`: Boolean flag indicating whether standard conforming strings are used (affects escape handling)

## Dependencies
- Functions called/Symbols referenced:
  - Uses standard C library functions (isdigit, isalpha, isascii)
- Called from (representative examples):
  - [ecpg_build_params](../e/ecpg_build_params.md) (multiple locations)

## Notes and Other Information
- Returns the position of the next placeholder, or -1 if none found
- Properly handles string literal parsing to avoid false matches inside quotes
- Distinguishes between $n parameter placeholders and $tag$ dollar-quoted strings
- Handles escape sequences differently based on standard_conforming_strings setting
- Essential for parameter substitution in ECPG prepared statement processing
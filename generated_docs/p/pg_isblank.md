# pg_isblank

## Location
src/backend/libpq/hba.c: 144 - 184

## Overview
A portable implementation of the isblank() function that tests whether a character is a whitespace character (space, tab, or carriage return).

## Definition


## Detailed Description
The pg_isblank function provides a custom implementation of the isblank() function from the ISO C99 specification. Since isblank() is not very portable across different systems, PostgreSQL provides its own version to ensure consistent behavior. The function checks if the given character is one of the common whitespace characters: space (' '), tab ('\t'), or carriage return ('\r').

Unlike the standard isblank() which typically only checks for space and tab, this PostgreSQL version also includes carriage return ('\r') as a blank character, which is useful for parsing configuration files and handling different line ending conventions.

## Parameters / Member Variables
- : The character to test for being a blank/whitespace character

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic character comparisons)
- Called from (representative examples):
  - interpret_ident_response (in src/backend/libpq/auth.c)
  - next_token (in src/backend/libpq/hba.c)

## Notes and Other Information
- This function is part of PostgreSQL's HBA (Host-Based Authentication) parsing infrastructure
- The inclusion of carriage return ('\r') as a blank character helps with cross-platform compatibility when parsing configuration files that may have different line endings
- Returns true if the character is space, tab, or carriage return; false otherwise
- Used primarily in authentication and configuration file parsing contexts
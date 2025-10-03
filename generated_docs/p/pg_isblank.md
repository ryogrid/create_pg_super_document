# pg_isblank

## Location
[src/backend/libpq/hba.c:144-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L144-L184)

## Overview
A portable implementation of the isblank() function that tests whether a character is a whitespace character (space, tab, or carriage return).

## Definition

```c
bool
pg_isblank(const char c)
```
## Detailed Description
The pg_isblank function provides a custom implementation of the isblank() function from the ISO C99 specification. Since isblank() is not very portable across different systems, PostgreSQL provides its own version to ensure consistent behavior. The function checks if the given character is one of the common whitespace characters: space (' '), tab ('\t'), or carriage return ('\r').

Unlike the standard isblank() which typically only checks for space and tab, this PostgreSQL version also includes carriage return ('\r') as a blank character, which is useful for parsing configuration files and handling different line ending conventions.

## Parameters / Member Variables
- `c`: The character to test for being a blank/whitespace character
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic character comparisons)
- Called from (representative examples):
  - [interpret_ident_response](../i/interpret_ident_response.md) (in src/backend/libpq/auth.c)
  - [next_token](../n/next_token.md) (in src/backend/libpq/hba.c)

## Notes and Other Information
- This function is part of PostgreSQL's HBA (Host-Based Authentication) parsing infrastructure
- The inclusion of carriage return ('\r') as a blank character helps with cross-platform compatibility when parsing configuration files that may have different line endings
- Returns true if the character is space, tab, or carriage return; false otherwise
- Used primarily in authentication and configuration file parsing contexts

## Simplified Source

```c
// Simplified version of pg_isblank
bool pg_isblank(const char c) {
    // Check if character is a whitespace character
    // Includes space, tab, and carriage return for cross-platform compatibility
    return c == ' ' || c == '\t' || c == '\r';
}
```

Key simplifications made:
- Function is already very simple - no simplifications needed
- Added descriptive comments explaining the purpose and rationale
- The original logic is preserved as it's already minimal and clear
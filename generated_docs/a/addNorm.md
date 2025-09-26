# addNorm

## Location
[src/backend/tsearch/spell.c:2524-2539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2524-L2539)

## Overview
Adds a normalized word lexeme to the result array during PostgreSQL's text search normalization process, managing the dynamic allocation and population of TSLexeme structures.

## Definition
```c
static void addNorm(TSLexeme **lres, TSLexeme **lcur, char *word, int flags, uint16 NVariant)
```

## Detailed Description
addNorm is a utility function that appends normalized word results to a dynamically managed array of TSLexeme structures. It handles the initial allocation of the result array when needed and ensures proper bounds checking to prevent buffer overflow. The function maintains both the start of the result array and the current insertion position, automatically null-terminating the array after each addition. This function is essential for building the final output of the text search normalization process.

## Parameters / Member Variables
- `lres`: Double pointer to the start of the TSLexeme result array
- `lcur`: Double pointer to the current position in the result array
- `word`: Normalized word string to be added
- `flags`: Flags indicating properties of the lexeme
- `NVariant`: Variant number for this lexeme

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation for TSLexeme array)
  - TSLexeme (structure type for text search lexemes)
  - MAX_NORM (maximum number of normalized forms)
- Called from (representative examples):
  - [NINormalizeWord](../N/NINormalizeWord.md) (at src/backend/tsearch/spell.c:2555, 2582, 2585)

## Notes and Other Information
- Allocates space for MAX_NORM TSLexeme structures on first call
- Performs bounds checking to prevent array overflow
- Automatically null-terminates the lexeme array after each addition
- Used specifically in text search normalization to collect all possible word forms
- Part of PostgreSQL's text search infrastructure for processing dictionary results
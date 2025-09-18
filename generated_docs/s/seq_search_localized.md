# seq_search_localized

## Location
src/backend/utils/adt/formatting.c: 2635 - 2718

## Overview
A static function that performs a case-insensitive sequential search through an array of strings (potentially non-English) using locale-aware case folding rules, designed to handle international text properly.

## Definition
```c
static int seq_search_localized(const char *name, char **array, int *len, Oid collid)
```

## Detailed Description
This function provides a more general case-insensitive string search compared to `seq_search_ascii()`. It can handle non-English words and uses locale-specific case folding rules determined by the collation ID. The function employs a two-phase approach: first attempting an exact match for performance, then falling back to case-insensitive comparison using double case-folding (upper then lower) to ensure reliable matching even in languages where case conversions are not injective.

The search algorithm:
1. First performs a quick exact match pass for performance optimization
2. If no exact match, applies double case-folding (upper then lower) to the input name
3. For each array element, applies the same double case-folding transformation
4. Compares the case-folded strings using `strncmp()`
5. Returns the array index of the first match found

## Parameters / Member Variables
- `name`: Input string to search for matches against
- `array`: Array of string pointers to search through (not declared const due to pg_locale.c compatibility)
- `len`: Output parameter - set to the length of the matched array element, or 0 for no match
- `collid`: Collation ID that determines the case-folding rules to use

## Dependencies
- Functions called/Symbols referenced:
  - unconstify (to cast away const qualifier)
  - [str_toupper](str_toupper.md) (for locale-aware upper case conversion)
  - [str_tolower](str_tolower.md) (for locale-aware lower case conversion)
  - [pfree](../p/pfree.md) (for memory cleanup)
  - strlen, strncmp (standard C library functions)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1065)
  - [from_char_seq_search](../f/from_char_seq_search.md) (formatting.c:2728)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- More expensive than `seq_search_ascii()` due to locale-aware case processing
- Uses double case-folding (upper then lower) to handle languages with non-injective case conversions
- Includes performance optimization with initial exact match pass
- Properly manages memory allocation/deallocation for case-folded strings
- Used primarily for international date/time formatting in PostgreSQL
- Returns -1 and sets *len to 0 when no match is found or input is empty
- The collation parameter allows for proper handling of different linguistic rules
# suff_search

## Location
[src/backend/utils/adt/formatting.c:1123-1138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1123-L1138)

## Overview
Searches through an array of KeySuffix structures to find a suffix that matches both the input string and the specified type.

## Definition
```c
static const KeySuffix *
suff_search(const char *str, const KeySuffix *suf, int type)
```

## Detailed Description
This function performs a linear search through an array of KeySuffix structures to find a suffix that matches both the provided string and type criteria. It iterates through the suffix array, checking each entry for type compatibility before performing string comparison. The function is designed for format parsing where suffixes need to be matched against specific formatting contexts.

The search algorithm:
1. Iterates through the KeySuffix array until a NULL name is encountered
2. Skips entries that don't match the specified type
3. Performs string comparison using strncmp for matching entries
4. Returns the first matching KeySuffix or NULL if no match is found

## Parameters / Member Variables
- `str`: Input string to match against suffix names
- `suf`: Array of KeySuffix structures to search through
- `type`: Type identifier to filter suffixes by category

## Dependencies
- Functions called/Symbols referenced:
  - KeySuffix (struct type)
  - strncmp (standard C library function)
- Called from (representative examples):
  - DCH_ZONED
  - parse_format (multiple locations)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Used in format parsing to identify appropriate suffixes for formatting tokens
- The function assumes the KeySuffix array is NULL-terminated (name field)
- Type filtering allows the same suffix name to be used in different contexts
- Returns NULL if no matching suffix is found
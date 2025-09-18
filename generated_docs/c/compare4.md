# compare4

## Location
[src/backend/utils/mb/conv.c:339-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L339-L352)

## Overview
A static comparison function used by bsearch() for binary search operations when converting local encoding codes to combined UTF-8 character sequences.

## Definition
```c
static int compare4(const void *p1, const void *p2)
```

## Detailed Description
The `compare4` function serves as a comparison routine specifically designed for the bsearch() standard library function. It is used in the context of local code to UTF-8 conversion, particularly when dealing with local character codes that need to be converted to combined UTF-8 sequences. The function compares a single uint32 search key against the `code` field in a `pg_local_to_utf_combined` structure entry.

This function performs a simple numerical comparison between the search key (local character code) and the stored local code in the conversion table entry. It enables efficient binary search through sorted arrays of local-to-UTF-8 character mappings.

## Parameters / Member Variables
- `p1`: Pointer to the search key, interpreted as a uint32 value representing a local character code
- `p2`: Pointer to a `pg_local_to_utf_combined` structure entry containing the code field for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [pg_local_to_utf_combined](../p/pg_local_to_utf_combined.md) (structure type accessed via casting)
- Called from (representative examples):
  - [LocalToUtf](../L/LocalToUtf.md) (used as comparison function in bsearch() call)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the conv.c translation unit
- The function implements a three-way comparison returning -1, 0, or 1 as required by bsearch()
- Unlike compare3, this function performs a single-value comparison since it deals with simple local codes rather than combined UTF-8 sequences
- Part of PostgreSQLs multibyte character encoding conversion subsystem
- The counterpart to compare3, handling the reverse direction of character encoding conversion
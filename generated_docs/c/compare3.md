# compare3

## Location
src/backend/utils/mb/conv.c: 320 - 338

## Overview
A static comparison function used by bsearch() for binary search operations when converting combined UTF-8 character sequences to local encoding codes.

## Definition
```c
static int compare3(const void *p1, const void *p2)
```

## Detailed Description
The `compare3` function serves as a comparison routine specifically designed for the bsearch() standard library function. It is used in the context of UTF-8 to local code conversion, particularly when dealing with combined UTF-8 character sequences that require multi-step conversion. The function compares two UTF-8 code points (utf1 and utf2) from the search key against the corresponding fields in a `pg_utf_to_local_combined` structure entry.

The comparison follows a lexicographic ordering: first by the primary UTF-8 code (utf1), and if they are equal, then by the secondary UTF-8 code (utf2). This allows for efficient binary search through sorted arrays of combined UTF-8 to local character mappings.

## Parameters / Member Variables
- `p1`: Pointer to the search key, interpreted as two consecutive uint32 values representing UTF-8 code points
- `p2`: Pointer to a `pg_utf_to_local_combined` structure entry containing utf1 and utf2 fields for comparison

## Dependencies
- Functions called/Symbols referenced:
  - pg_utf_to_local_combined (structure type accessed via casting)
- Called from (representative examples):
  - [UtfToLocal](../U/UtfToLocal.md) (used as comparison function in bsearch() call)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the conv.c translation unit
- The function implements a three-way comparison returning -1, 0, or 1 as required by bsearch()
- The comparison logic handles two uint32 values as a compound key, enabling efficient lookup of combined UTF-8 character sequences
- Part of PostgreSQLs multibyte character encoding conversion subsystem
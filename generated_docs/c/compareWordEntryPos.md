# compareWordEntryPos

## Location
[src/backend/utils/adt/tsvector.c:36-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L36-L51)

## Overview
A comparison function used for sorting WordEntryPos values in PostgreSQL's text search functionality.

## Definition


## Detailed Description
This function serves as a comparator for qsort operations on arrays of WordEntryPos structures. It extracts position information from two WordEntryPos values and compares them using PostgreSQL's standard 32-bit signed integer comparison function. The function is essential for maintaining sorted order of word positions within tsvector data structures, which is crucial for efficient text search operations.

## Parameters / Member Variables
- : Pointer to the first WordEntryPos value to compare (cast from const void*)
- : Pointer to the second WordEntryPos value to compare (cast from const void*)

## Dependencies
- Functions called/Symbols referenced:
  - WEP_GETPOS (macro to extract position from WordEntryPos)
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL's 32-bit signed integer comparison function)
  - WordEntryPos (structure type)
- Called from (representative examples):
  - [uniquePos](../u/uniquePos.md) (for sorting positions before removing duplicates)
  - [checkcondition_str](checkcondition_str.md) (in tsvector operations)

## Notes and Other Information
- Returns negative, zero, or positive value if the first position is less than, equal to, or greater than the second position respectively
- Follows the standard qsort comparator function signature
- Used specifically in text search vector processing where maintaining sorted word positions is critical for performance
- Part of PostgreSQL's full-text search infrastructure
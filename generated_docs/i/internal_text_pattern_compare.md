# internal_text_pattern_compare

## Location
[src/backend/utils/adt/varlena.c:2797-2818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2797-L2818)

## Overview
Static function that performs character-by-character comparison of two text datums, specifically designed to support LIKE clause indexing and pattern matching operations.

## Definition

```c
static int
internal_text_pattern_compare(text *arg1, text *arg2)
```
## Detailed Description
The  function is a core utility function for PostgreSQL's text pattern comparison operations. It performs byte-by-byte comparison of text values using memcmp(), which enables character-by-character ordering that is essential for building indexes suitable for LIKE clauses and pattern matching operations.

The function first compares the overlapping portions of both strings using memcmp(), and if they are identical, it then compares the lengths to determine ordering. This approach ensures consistent lexicographic ordering that is compatible with regular text comparison operators when using "C" collation.

## Parameters / Member Variables
- `*arg1`: Pointer to the first text datum to compare
- `*arg2`: Pointer to the second text datum to compare
## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to get variable-length data size excluding header
  -  - Macro to get pointer to variable-length data content
  -  - Macro to get minimum of two values
  -  - Standard C library function for memory comparison
- Called from (representative examples):
  -  - Less than comparison for pattern operations
  -  - Less than or equal comparison for pattern operations
  -  - Greater than or equal comparison for pattern operations
  -  - Greater than comparison for pattern operations
  -  - B-tree comparison function for pattern operations

## Notes and Other Information
- This function is declared static, making it internal to the varlena.c translation unit
- The function is specifically designed to be compatible with regular texteq/textne comparison operators and support functions 1 and 2 with "C" collation
- Returns negative value if arg1 < arg2, zero if arg1 == arg2, positive value if arg1 > arg2
- The comparison is performed at the byte level, making it suitable for building indexes that can efficiently support LIKE pattern matching
- Located in  at lines 2797-2818
- Part of PostgreSQL's text pattern matching infrastructure introduced to optimize LIKE clause operations
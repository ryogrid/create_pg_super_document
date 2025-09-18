# byteacmp

## Location
src/backend/utils/adt/varlena.c: 3938 - 3959

## Overview
The byteacmp function implements a three-way comparison function for bytea (binary string) data types, returning -1, 0, or 1 to indicate less-than, equal, or greater-than relationships.

## Definition
```c
Datum byteacmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a lexicographic comparison between two bytea values and returns an integer indicating their relative ordering. It first compares the binary data using memcmp() up to the minimum length of both arguments. If the overlapping portions are identical, it then compares the lengths to determine the final result. This function is the foundation for all bytea comparison operators and is commonly used in sorting and indexing operations.

The comparison logic follows standard lexicographic ordering:
- If memcmp() returns non-zero, that result is returned directly
- If memcmp() returns zero (equal prefixes), the shorter string is considered less than the longer one
- If both content and length are identical, zero is returned

## Parameters / Member Variables
- `arg1`: First bytea value to compare
- `arg2`: Second bytea value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (argument extraction)
  - VARSIZE_ANY_EXHDR (get data length)
  - VARDATA_ANY (get data pointer)
  - memcmp (binary comparison)
  - Min (minimum of two values)
  - PG_FREE_IF_COPY (memory cleanup)
  - PG_RETURN_INT32 (return integer result)
- Called from (representative examples):
  - B-tree comparison operations
  - Sorting algorithms
  - Other bytea comparison operators

## Notes and Other Information
- This is the canonical comparison function for bytea types in PostgreSQL
- Returns standard three-way comparison result: -1 (less), 0 (equal), 1 (greater)
- Used internally by other comparison operators like <, <=, >=, >
- Efficiently handles variable-length binary data with proper varlena management
- The comparison is case-sensitive as it operates on raw binary data
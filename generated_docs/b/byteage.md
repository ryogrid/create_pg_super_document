# byteage

## Location
[src/backend/utils/adt/varlena.c:3918-3937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3918-L3937)

## Overview
The byteage function implements the greater-than-or-equal-to (>=) comparison operator for bytea (binary string) data types in PostgreSQL.

## Definition

```c
Datum
byteage(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares two bytea values and returns true if the first argument is greater than or equal to the second argument. The comparison is performed lexicographically using memcmp() on the binary data. For equal-length prefixes, the longer string is considered greater. If both strings are identical in content and length, they are considered equal.

The function extracts the actual data length excluding the varlena header using VARSIZE_ANY_EXHDR(), then performs a byte-by-byte comparison up to the minimum length of both arguments. The result is true if either:
1. The first bytea is lexicographically greater than the second
2. The byteas are equal in their overlapping portion AND the first is longer or equal in length

## Parameters / Member Variables
- : First bytea value to compare (left operand of >= operator)
- : Second bytea value to compare (right operand of >= operator)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (argument extraction)
  - VARSIZE_ANY_EXHDR (get data length)
  - VARDATA_ANY (get data pointer)
  - memcmp (binary comparison)
  - Min (minimum of two values)
  - PG_FREE_IF_COPY (memory cleanup)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - SQL >= operator for bytea types
  - B-tree comparison operations

## Notes and Other Information
- This function is part of PostgreSQL's bytea comparison operator family
- Uses efficient memcmp() for binary data comparison
- Properly handles varlena header management and memory cleanup
- Returns true for both greater-than and equal-to cases, implementing the >= semantic
- The comparison is case-sensitive as it operates on raw binary data

## Simplified Source

```c
Datum byteage(PG_FUNCTION_ARGS) {
    // Get the two bytea arguments
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    bytea *arg2 = PG_GETARG_BYTEA_PP(1);

    // Get lengths excluding headers
    int len1 = VARSIZE_ANY_EXHDR(arg1);
    int len2 = VARSIZE_ANY_EXHDR(arg2);

    // Compare bytes up to the length of shorter string
    int cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    // Clean up memory if needed
    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    // Return true if arg1 >= arg2 lexicographically
    // Either bytes differ (cmp > 0) or bytes same but arg1 not shorter
    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}
```
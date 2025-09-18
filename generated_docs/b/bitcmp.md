# bitcmp

## Location
src/backend/utils/adt/varbit.c: 949 - 967

## Overview
Implements a three-way comparison function for PostgreSQL bit string data types, returning an integer indicating the lexicographical ordering relationship between two bit strings.

## Definition
```c
Datum bitcmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bitcmp` function is a PostgreSQL function that provides a three-way comparison for bit strings, similar to the standard C library's `strcmp` function. It takes two VarBit (variable-length bit string) arguments and returns an integer result: negative if the first argument is lexicographically smaller, zero if they are equal, or positive if the first argument is lexicographically greater. This function directly exposes the result of the internal `bit_cmp` helper function and is typically used for sorting operations and btree index support.

## Parameters / Member Variables
- `arg1`: The first VarBit argument for comparison
- `arg2`: The second VarBit argument for comparison
- `result`: Integer variable storing the comparison result (<0, 0, or >0)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_VARBIT_P` - macro to extract VarBit arguments from function call
  - [bit_cmp](bit_cmp.md) - internal comparison function that returns <0, 0, or >0
  - `PG_FREE_IF_COPY` - macro to free copied arguments if necessary
  - `PG_RETURN_INT32` - macro to return int32 result
- Called from (representative examples):
  - No direct references found (likely called via SQL system for sorting/indexing)

## Notes and Other Information
- This function is essential for btree index support on bit string columns
- The comparison is lexicographical, considering all bits including trailing zeros
- Returns the same values as the internal bit_cmp function: <0 for less than, 0 for equal, >0 for greater than
- Memory management is handled through PG_FREE_IF_COPY to prevent leaks in btree operations
- Located in src/backend/utils/adt/varbit.c:949-967
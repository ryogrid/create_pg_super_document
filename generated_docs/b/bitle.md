# bitle

## Location
src/backend/utils/adt/varbit.c: 904 - 918

## Overview
Implements the "less than or equal to" comparison operator for PostgreSQL bit string data types, returning true if the first bit string is lexicographically smaller than or equal to the second.

## Definition
```c
Datum bitle(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bitle` function is a PostgreSQL function that implements the "<=" operator for bit string comparisons. It takes two VarBit (variable-length bit string) arguments and returns a boolean result indicating whether the first argument is lexicographically less than or equal to the second. The function uses the internal `bit_cmp` helper function to perform the actual comparison and returns true if the comparison result is less than or equal to zero (<= 0).

## Parameters / Member Variables
- `arg1`: The first VarBit argument (left operand of the <= operator)
- `arg2`: The second VarBit argument (right operand of the <= operator)
- `result`: Boolean variable storing the comparison result

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_VARBIT_P` - macro to extract VarBit arguments from function call
  - `[bit_cmp](bit_cmp.md)` - internal comparison function that returns <0, 0, or >0
  - `PG_FREE_IF_COPY` - macro to free copied arguments if necessary
  - `PG_RETURN_BOOL` - macro to return boolean result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's bit string comparison operator family
- The comparison is lexicographical, considering all bits including trailing zeros
- Memory management is handled through PG_FREE_IF_COPY to prevent leaks in btree operations
- Located in src/backend/utils/adt/varbit.c:904-918
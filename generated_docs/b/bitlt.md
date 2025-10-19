# bitlt

## Location
[src/backend/utils/adt/varbit.c:889-903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L889-L903)

## Overview
Implements the "less than" comparison operator for PostgreSQL bit string data types, returning true if the first bit string is lexicographically smaller than the second.

## Definition

```c
Datum
bitlt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL function that implements the "<" operator for bit string comparisons. It takes two VarBit (variable-length bit string) arguments and returns a boolean result indicating whether the first argument is lexicographically less than the second. The function uses the internal  helper function to perform the actual comparison and returns true if the comparison result is negative (< 0).

## Parameters / Member Variables
- : The first VarBit argument (left operand of the < operator)
- : The second VarBit argument (right operand of the < operator)
- : Boolean variable storing the comparison result

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract VarBit arguments from function call
  -  - internal comparison function that returns <0, 0, or >0
  -  - macro to free copied arguments if necessary
  -  - macro to return boolean result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's bit string comparison operator family
- The comparison is lexicographical, considering all bits including trailing zeros
- Memory management is handled through PG_FREE_IF_COPY to prevent leaks in btree operations
- Located in src/backend/utils/adt/varbit.c:889-903

## Simplified Source

```c
Datum bitlt(PG_FUNCTION_ARGS) {
    VarBit *arg1 = PG_GETARG_VARBIT_P(0);
    VarBit *arg2 = PG_GETARG_VARBIT_P(1);

    // Check if first argument is less than second
    bool result = (bit_cmp(arg1, arg2) < 0);

    // Clean up memory for toasted values
    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}
```
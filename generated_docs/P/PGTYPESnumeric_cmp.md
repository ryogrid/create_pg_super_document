# PGTYPESnumeric_cmp

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1281-1308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1281-L1308)

## Overview
Compares two numeric values and returns their relative ordering (-1, 0, or 1) in PostgreSQL's ECPG pgtypes library.

## Definition
```c
int PGTYPESnumeric_cmp(numeric *var1, numeric *var2)
```

## Detailed Description
This function performs comparison between two numeric variables by analyzing their signs and delegating to the absolute value comparison function cmp_abs. The comparison logic handles four cases: both positive (direct comparison), both negative (inverted parameter order), mixed signs (trivial comparison based on sign), and error cases. For same-sign comparisons, it uses the cmp_abs helper function. For negative numbers, it inverts the parameter order to cmp_abs to achieve the correct ordering since -5 < -3 but |-5| > |-3|.

## Parameters / Member Variables
- `var1`: Pointer to the first numeric variable to compare
- `var2`: Pointer to the second numeric variable to compare

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_abs](../c/cmp_abs.md) (compares absolute values of numeric variables)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - PGTYPES_NUM_BAD_NUMERIC (error constant)
  - [numeric](../n/numeric.md) (numeric type)
- Called from (representative examples):
  - [deccmp](../d/deccmp.md) (in ECPG Informix compatibility layer)
  - [main](../m/main.md) (in various pgtypes test programs)

## Notes and Other Information
- Returns -1 if var1 < var2, 0 if var1 == var2, 1 if var1 > var2
- Returns INT_MAX on error and sets errno to PGTYPES_NUM_BAD_NUMERIC
- Uses sign-based logic for efficient comparison
- Critical component for sorting and conditional operations in client applications
- Part of the ECPG pgtypes library providing PostgreSQL-compatible numeric operations
- Handles all edge cases including zero values and sign combinations

## Simplified Source

```c
int
PGTYPESnumeric_cmp(numeric *var1, numeric *var2)
{
    // Both positive: direct comparison
    if (var1->sign == NUMERIC_POS && var2->sign == NUMERIC_POS)
        return cmp_abs(var1, var2);

    // Both negative: invert parameter order for correct result
    if (var1->sign == NUMERIC_NEG && var2->sign == NUMERIC_NEG)
        return cmp_abs(var2, var1);

    // Mixed signs: positive > negative
    if (var1->sign == NUMERIC_POS && var2->sign == NUMERIC_NEG)
        return 1;
    if (var1->sign == NUMERIC_NEG && var2->sign == NUMERIC_POS)
        return -1;

    // Error case: invalid sign combination
    errno = PGTYPES_NUM_BAD_NUMERIC;
    return INT_MAX;
}
```
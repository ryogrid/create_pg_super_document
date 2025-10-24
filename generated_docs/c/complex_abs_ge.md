# complex_abs_ge

## Location
[src/tutorial/complex.c:181-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L181-L191)

## Overview
A PostgreSQL function that implements the "greater than or equal to" comparison operator for the absolute values (magnitudes) of two complex numbers.

## Definition

```c
Datum
complex_abs_ge(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL-callable wrapper that implements the ">=" operator for comparing the magnitudes of complex numbers. It extracts two Complex pointers from the function arguments, delegates the actual comparison to the internal `complex_abs_cmp_internal` function, and returns true if the first complex number has a magnitude greater than or equal to the second. This function is part of the B-tree index operator class for complex numbers, enabling PostgreSQL to perform ordered operations and indexing based on complex number magnitudes.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access function arguments:
  - Argument 0: Pointer to the first Complex number (a)
  - Argument 1: Pointer to the second Complex number (b)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract Complex pointers from arguments)
  - [complex_abs_cmp_internal](complex_abs_cmp_internal.md) (for the actual magnitude comparison)
  - PG_RETURN_BOOL (to return boolean result)
  - [Complex](../C/Complex.md) (struct type for complex numbers)
- Called from (representative examples):
  - PostgreSQL query execution engine when using `>=` operator on complex values
  - B-tree index operations for complex number ordering

## Notes and Other Information
- This is a PostgreSQL-callable function using the fmgr interface
- Returns true if |a| >= |b|, false otherwise
- Part of the B-tree index operator class implementation for complex numbers
- Uses the centralized comparison logic in `complex_abs_cmp_internal` to ensure consistency
- Located in src/tutorial/complex.c:181-191
- Requires PG_FUNCTION_INFO_V1 declaration for PostgreSQL function registration

## Simplified Source

```c
Datum complex_abs_ge(PG_FUNCTION_ARGS) {
    // Extract the two complex numbers from function arguments
    Complex *a = (Complex *) PG_GETARG_POINTER(0);
    Complex *b = (Complex *) PG_GETARG_POINTER(1);

    // Compare magnitudes and return true if first >= second
    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) >= 0);
}
```
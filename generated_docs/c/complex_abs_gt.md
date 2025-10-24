# complex_abs_gt

## Location
[src/tutorial/complex.c:192-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L192-L202)

## Overview
A PostgreSQL operator function that compares the absolute magnitudes of two complex numbers and returns true if the first is greater than the second.

## Definition

```c
Datum
complex_abs_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that implements the "greater than" comparison operator for complex numbers based on their absolute magnitudes (moduli). It serves as part of the B-tree operator class for complex numbers, enabling indexing and ordering operations on complex data types.

The function extracts two Complex pointers from the PostgreSQL function arguments, compares their absolute magnitudes using the internal comparison function, and returns a boolean result indicating whether the first complex number has a greater absolute magnitude than the second.

This function is designed to work within PostgreSQL's operator framework and follows the standard PostgreSQL function calling conventions using the PG_FUNCTION_ARGS macro and PG_RETURN_BOOL for the return value.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Pointer to the first Complex number (a)
  - Second argument: Pointer to the second Complex number (b)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts pointer arguments from PostgreSQL function call
  - : Internal three-way comparison function for complex magnitudes
  - : Returns boolean result to PostgreSQL
  - : Complex number data type structure
- Called from (representative examples):
  - : Greater-than-or-equal comparison function

## Notes and Other Information
- This function is part of a B-tree operator class implementation for complex numbers
- The comparison is based on the absolute magnitude (modulus) of complex numbers, calculated as sqrt(x² + y²)
- All comparison operators in this class use the same internal comparison function () to ensure consistency
- The function follows PostgreSQL's V1 calling convention
- Returns true if |a| > |b|, false otherwise
- Located in the tutorial code demonstrating how to implement custom data types in PostgreSQL

## Simplified Source

```c
Datum complex_abs_gt(PG_FUNCTION_ARGS) {
    // Extract the two complex numbers from function arguments
    Complex *a = (Complex *) PG_GETARG_POINTER(0);
    Complex *b = (Complex *) PG_GETARG_POINTER(1);

    // Compare magnitudes and return true if first > second
    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) > 0);
}
```
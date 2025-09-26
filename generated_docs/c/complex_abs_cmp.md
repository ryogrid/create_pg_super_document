# complex_abs_cmp

## Location
src/tutorial/complex.c: 203 - 209

## Overview
A PostgreSQL operator function that performs a three-way comparison of the absolute magnitudes of two complex numbers, returning -1, 0, or 1 based on their relative ordering.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL-callable function that implements the comparison support function for complex numbers based on their absolute magnitudes (moduli). This function serves as the foundation for B-tree indexing operations on complex data types by providing a three-way comparison result.

The function extracts two Complex pointers from the PostgreSQL function arguments and delegates the actual comparison logic to the internal  function. It returns an integer result following the standard comparison convention: negative if the first argument is smaller, zero if they are equal, and positive if the first argument is larger.

This function is essential for PostgreSQL's B-tree operator class implementation, as it provides the core ordering logic that all other comparison operators rely upon, ensuring consistency across the entire operator family.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Pointer to the first Complex number (a)
  - Second argument: Pointer to the second Complex number (b)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts pointer arguments from PostgreSQL function call
  - : Internal three-way comparison function for complex magnitudes
  - : Returns integer result to PostgreSQL
  - : Complex number data type structure
- Called from (representative examples):
  - : Referenced in PG_FUNCTION_INFO_V1 declaration

## Notes and Other Information
- This function is the core support function for the B-tree operator class for complex numbers
- The comparison is based on the absolute magnitude (modulus) of complex numbers, calculated as sqrt(x² + y²)
- Returns -1 if |a| < |b|, 0 if |a| = |b|, and 1 if |a| > |b|
- All other comparison operators in the class use this function internally to ensure consistent ordering
- The function follows PostgreSQL's V1 calling convention
- Located in the tutorial code demonstrating how to implement custom data types with proper B-tree support in PostgreSQL
- Critical for enabling sorting, indexing, and range queries on complex number data
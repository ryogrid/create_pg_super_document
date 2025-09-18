# non_negative

## Location
[src/backend/access/gist/gistproc.c:339-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L339-L350)

## Overview
A static inline utility function that ensures a floating-point value is non-negative by replacing negative values or NaN with zero.

## Definition


## Detailed Description
The  function is a simple utility that sanitizes floating-point values to ensure they are non-negative. It performs a straightforward check: if the input value is greater than or equal to 0.0, it returns the value unchanged; otherwise, it returns 0.0. This function is particularly useful in geometric calculations where negative distances or dimensions would be meaningless, such as in GiST (Generalized Search Tree) operations for spatial data types.

The function handles both negative values and NaN (Not a Number) values by replacing them with 0.0, as the comparison  will be false for both negative numbers and NaN values in IEEE 754 floating-point arithmetic.

## Parameters / Member Variables
- : The input floating-point value to be checked and potentially sanitized

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic arithmetic comparison)
- Called from (representative examples):
  - [g_box_consider_split](../g/g_box_consider_split.md) (called twice at lines 436 and 438)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the same compilation unit and will likely be inlined by the compiler for performance
- The function is located in the GiST procedural code (gistproc.c), indicating its use in spatial indexing operations
- The IEEE 754 floating-point standard ensures that NaN comparisons with any value (including itself) return false, making this function effective for handling both negative values and NaN cases
- This utility is essential for maintaining data integrity in geometric calculations where non-negative values are required
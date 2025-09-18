# float4_eq

## Location
src/include/utils/float.h: 262 - 267

## Overview
Performs NaN-aware equality comparison between two single-precision floating-point numbers (float4), treating all NaN values as equal to each other.

## Definition


## Detailed Description
The  function provides a specialized equality comparison for  (single-precision floating-point) values that handles NaN (Not a Number) values in a database-friendly manner. Unlike standard floating-point equality comparison where NaN != NaN, this function considers all NaN values to be equal to each other and different from any non-NaN value.

This behavior is essential for database operations where consistent sorting and comparison behavior is required. The function follows PostgreSQL's design philosophy that all NaN values should be considered equal and larger than any non-NaN value, providing deterministic behavior for indexing, sorting, and comparison operations.

## Parameters / Member Variables
- : The first single-precision floating-point value to compare
- : The second single-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function to check for NaN (Not a Number)
  - : Type alias for single-precision floating-point (float)
- Called from (representative examples):
  - : Main SQL-callable equality function in src/backend/utils/adt/float.c:824

## Notes and Other Information
- This is an inline function for performance, defined in src/include/utils/float.h:262-267
- Part of PostgreSQL's NaN-aware comparison routines with consistent sort order design
- The function returns true if both values are NaN, or if both are non-NaN and equal
- Returns false if one value is NaN and the other is not, or if both are non-NaN but not equal
- This behavior differs from IEEE 754 standard where NaN != NaN always returns true
- Essential for database operations requiring consistent and predictable comparison behavior
- Used as the foundation for SQL equality operations on the  data type (float4)
- The design ensures that NaN values can be properly indexed and sorted in database operations
- Follows PostgreSQL's convention that all NaNs are considered equal and greater than any finite value
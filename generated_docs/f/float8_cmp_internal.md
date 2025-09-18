# float8_cmp_internal

## Location
src/backend/utils/adt/float.c: 903 - 912

## Overview
Internal comparison function that performs three-way comparison between two double-precision floating-point numbers (float8).

## Definition


## Detailed Description
This function provides the core comparison logic for double-precision floating-point numbers in PostgreSQL. It implements a three-way comparison that returns -1 if the first argument is less than the second, 0 if they are equal, and 1 if the first argument is greater than the second. The function uses the existing  and  functions to determine the relationship between the two values, ensuring consistent handling of special floating-point cases like NaN values. This internal function serves as a building block for various comparison operations and is used by B-tree comparison functions, sorting operations, and other components that need to establish ordering relationships between float8 values.

## Parameters / Member Variables
- : First double-precision floating-point value to compare
- : Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Function to test if first float8 is greater than second
  - : Function to test if first float8 is less than second
  - : Double-precision floating-point data type

- Called from (representative examples):
  - : B-tree comparison function for float8
  - : Fast comparison function for float8 sorting
  - : Mixed float4/float8 comparison function
  - : Mixed float8/float4 comparison function
  - : GiST index support function for intervals
  - : GiST index support function for intervals
  - : GiST index support function
  - : GiST search heap comparison

## Notes and Other Information
- Returns standard three-way comparison result: -1, 0, or 1
- Handles special floating-point cases through the underlying  and  functions
- Located in 
- Part of the float8 comparison operations suite as indicated by the comment
- Used extensively throughout PostgreSQL's indexing and sorting infrastructure
- Provides consistent comparison semantics for double-precision floating-point values
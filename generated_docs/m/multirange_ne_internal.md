# multirange_ne_internal

## Location
src/backend/utils/adt/multirangetypes.c: 1914 - 1922

## Overview
Implements the internal inequality comparison logic for multirange types by negating the result of the equality comparison.

## Definition


## Detailed Description
This function provides the core inequality comparison logic for multirange types. It takes a simple and efficient approach by delegating to the existing equality comparison function  and negating the result. This ensures consistent behavior between equality and inequality operations while avoiding code duplication. The function is designed as an internal utility that can be called by both SQL-facing functions and other internal PostgreSQL operations.

The logic is straightforward: two multiranges are considered not equal if they differ in any way - different number of ranges, different range bounds, or different ordering of ranges.

## Parameters / Member Variables
- : Type cache entry containing metadata and comparison functions for the underlying range type
- : First multirange value to compare (const to prevent modification)  
- : Second multirange value to compare (const to prevent modification)

## Dependencies
- Functions called/Symbols referenced:
  - : Core equality comparison function that this negates
  - : Multirange data structure type
- Called from (representative examples):
  - : SQL-callable inequality operator wrapper
  - : Macro that may use this for comparisons

## Notes and Other Information
- This is an internal function not directly callable from SQL
- Follows the PostgreSQL convention of having separate internal and external function versions
- Uses logical negation of equality rather than implementing independent inequality logic
- The function maintains const correctness by not modifying input parameters
- Extremely lightweight implementation that leverages existing equality comparison infrastructure
- Located in 
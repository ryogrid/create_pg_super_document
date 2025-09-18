# range_cmp

## Location
src/backend/utils/adt/rangetypes.c: 1249 - 1294

## Overview
B-tree comparator function that provides total ordering for range types, enabling range values to be sorted, indexed, and used in ordered operations.

## Definition


## Detailed Description
The  function implements the primary comparison logic for PostgreSQL range types, providing the foundation for B-tree indexing and ordering operations. It establishes a total ordering among range values by comparing them lexicographically: first by lower bounds, then by upper bounds if the lower bounds are equal.

The function handles several special cases in its comparison logic: empty ranges are considered to sort before all non-empty ranges, and when comparing two empty ranges, they are considered equal. For non-empty ranges, the comparison is performed using type-specific comparison functions for the range's element type.

The function includes a stack depth check to prevent stack overflow when dealing with nested range types (ranges whose element type is itself a range type). It also includes proper memory management by freeing copied range values when they are no longer needed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): First range value for comparison ()
  - Second argument (index 1): Second range value for comparison ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts range arguments from function call
  - : Prevents stack overflow in recursive comparisons
  - : Gets the OID of a range type for type validation
  - : Retrieves type cache information for element type operations
  - : Extracts boundary and empty flag information from ranges
  - : Compares individual range boundaries using element type comparison
  - : Releases memory for copied range arguments
  - : Returns the comparison result as a 32-bit integer
  - : Structure representing range boundary information
- Called from:
  - : Less-than comparison function
  - : Less-than-or-equal comparison function  
  - : Greater-than-or-equal comparison function
  - : Greater-than comparison function
  - B-tree indexing operations on range columns
  - ORDER BY clauses involving range values

## Notes and Other Information
- Returns negative, zero, or positive integer indicating r1 < r2, r1 = r2, or r1 > r2 respectively
- Empty ranges sort before all non-empty ranges in the ordering
- Comparison is lexicographic: lower bounds compared first, then upper bounds if lower bounds are equal
- Includes type checking to ensure both ranges are of the same range type
- Handles recursive range types safely with stack depth checking
- Essential for B-tree indexing support and range ordering operations
- Located in 
- Forms the basis for all range comparison operators and sorting functionality
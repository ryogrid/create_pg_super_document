# multirange_gt

## Location
src/backend/utils/adt/multirangetypes.c: 2664 - 2674

## Overview
A PostgreSQL function that implements the greater-than operator (>) for multirange types, determining if one multirange is strictly greater than another according to PostgreSQL's multirange ordering semantics.

## Definition


## Detailed Description
The  function is a comparison operator that implements the ">" (greater than) operation for multirange data types in PostgreSQL. It complements the other comparison operators in the multirange operator family and is essential for B-tree index support and query processing involving multirange comparisons.

The function operates by calling  to perform the detailed comparison between two multirange values and returns true only when the first multirange is strictly greater than the second. This means the comparison result must be positive (> 0), indicating a clear ordering preference for the first operand.

The comparison follows PostgreSQL's established multirange ordering rules:
- Lexicographic comparison of constituent ranges within each multirange
- Shorter multiranges are treated as "smaller" due to implicit empty range padding
- Empty ranges always compare as less than any non-empty range
- Range bounds are compared using the underlying range type's comparison semantics

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to function arguments through the fcinfo structure
  - First argument (index 0): The left multirange operand
  - Second argument (index 1): The right multirange operand

## Dependencies
- Functions called/Symbols referenced:
  - : Core comparison function that returns -1, 0, or 1 for less than, equal, or greater than relationships
  - : PostgreSQL macro for returning boolean values from functions

- Called from (representative examples):
  - SQL queries using > operator on multirange columns
  - B-tree index operations during inequality searches
  - Query planner optimization for multirange predicate evaluation

## Notes and Other Information
- This function is part of PostgreSQL's comprehensive multirange type system for handling collections of ranges
- The function follows PostgreSQL's standard comparison operator pattern where > is implemented as (cmp > 0)
- Unlike >= and <=, this operator requires strict inequality, excluding cases where multiranges are equal
- Performance scales with the complexity and size of the multiranges being compared
- The function is registered as part of the multirange operator class for complete B-tree indexing support
- Type safety and validation are ensured through the multirange_cmp function
- Essential for implementing full SQL comparison semantics and enabling efficient query execution on multirange data
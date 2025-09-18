# multirange_le

## Location
[src/backend/utils/adt/multirangetypes.c:2648-2655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2648-L2655)

## Overview
A PostgreSQL function that implements the less-than-or-equal-to operator (<=) for multirange types, determining if one multirange is less than or equal to another according to PostgreSQL's multirange ordering semantics.

## Definition


## Detailed Description
The  function is a comparison operator that implements the "<=" (less than or equal to) operation for multirange data types in PostgreSQL. It serves as part of the B-tree operator class for multiranges, enabling them to be used in indexes, sorting operations, and comparison queries.

The function works by delegating the actual comparison logic to , which performs a comprehensive comparison of two multirange values. The function returns true if the first multirange is less than or equal to the second multirange based on PostgreSQL's multirange ordering rules.

The comparison follows these principles:
- Multiranges are compared element by element, starting from the first range
- If one multirange is shorter, it's treated as having empty ranges at the end
- Empty ranges compare as less than any non-empty range
- Individual ranges within multiranges are compared by their bounds

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to function arguments through the fcinfo structure
  - First argument (index 0): The left multirange operand
  - Second argument (index 1): The right multirange operand

## Dependencies
- Functions called/Symbols referenced:
  - : Core comparison function that returns -1, 0, or 1 for less than, equal, or greater than relationships
  - : PostgreSQL macro for returning boolean values from functions

- Called from (representative examples):
  - SQL queries using <= operator on multirange columns
  - B-tree index operations during sorting and searching
  - [Query](../Q/Query.md) planner when evaluating multirange comparisons

## Notes and Other Information
- This function is part of PostgreSQL's multirange type infrastructure introduced for handling collections of ranges
- The function follows PostgreSQL's standard comparison operator pattern where <= is implemented as (cmp <= 0)
- Performance is dependent on the complexity of the multiranges being compared, as each constituent range must be examined
- The function is registered as part of the multirange operator class for B-tree indexing support
- Type safety is ensured by the multirange_cmp function, which validates that both operands are of the same multirange type
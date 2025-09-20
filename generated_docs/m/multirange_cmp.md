# multirange_cmp

## Location
[src/backend/utils/adt/multirangetypes.c:2575-2639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2575-L2639)

## Overview
Provides btree comparison functionality for multirange types by comparing multiranges lexicographically range by range.

## Definition

```c
Datum
multirange_cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the btree comparator for multirange types, enabling multiranges to be used in btree indexes and sorted operations. The function performs a lexicographic comparison by comparing corresponding ranges within the multiranges from left to right.

The comparison algorithm works as follows:
1. Validates that both multiranges are of the same type (different types should be prevented by ANYMULTIRANGE matching rules)
2. Iterates through corresponding ranges in both multiranges
3. For each pair of ranges, compares first the lower bounds, then the upper bounds using 
4. If one multirange is shorter than the other, the shorter one is considered to come before the longer one (similar to string comparison where 'aaa' < 'aaaaaa')
5. Returns -1, 0, or 1 indicating whether the first multirange is less than, equal to, or greater than the second

This function serves as the foundation for all multirange comparison operators and enables multiranges to participate in btree-based operations like sorting and indexing.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0:  - the first multirange for comparison
  - Argument 1:  - the second multirange for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  - extract multirange arguments
  -  - get OID of multirange type for type checking
  -  - get type cache for multirange operations
  -  - extract bounds from specific ranges within multiranges
  -  - [compare](../c/compare.md) range bounds
  -  - free copied arguments if necessary
  -  - macro to find maximum of two values
- Called from (representative examples):
  -  - less than operator
  -  - less than or equal operator
  -  - greater than or equal operator
  -  - greater than operator
  - Btree index operations for multirange types

## Notes and Other Information
- Returns an int32 result (-1, 0, 1) wrapped as a Datum using 
- Includes type validation to ensure both multiranges are of the same type
- Uses lexicographic comparison similar to string comparison algorithms
- Handles multiranges of different lengths by treating missing ranges as empty (which compare as less than any non-empty range)
- Essential for btree indexing support of multirange types
- Part of PostgreSQL's range and multirange type system for advanced range operations
- File location: 
# multirange_overright_range

## Location
[src/backend/utils/adt/multirangetypes.c:2191-2214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2191-L2214)

## Overview
Tests whether a multirange is positioned to the right of or overlapping with a range, specifically checking if the multirange's leftmost bound is greater than or equal to the range's leftmost bound.

## Definition


## Detailed Description
The  function implements the "overright" or "not left of" operator (&>) for checking the positional relationship between a multirange and a range. It returns true if the multirange is positioned to the right of or overlapping with the range, which is determined by comparing their leftmost bounds.

The function performs a bound comparison to determine if the multirange's leftmost lower bound is greater than or equal to the range's lower bound. This operation is fundamental to range-based spatial queries and indexing operations in PostgreSQL.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0:  - The multirange to test
  - Argument 1:  - The range to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract multirange from function arguments
  -  - Extract range from function arguments  
  -  - Check if multirange is empty
  -  - Check if range is empty
  -  - Get type cache for range type
  -  - Get OID of multirange type
  -  - Extract bounds from multirange
  -  - Extract bounds from range
  -  - Compare range bounds
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- Returns false immediately if either the multirange or range is empty
- Uses the first range in the multirange for comparison (index 0)
- The comparison result >= 0 indicates the multirange is "overright" of the range
- This function supports the &> operator in SQL queries for multirange types
- Located in src/backend/utils/adt/multirangetypes.c:2191-2214
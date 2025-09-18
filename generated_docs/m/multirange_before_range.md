# multirange_before_range

## Location
[src/backend/utils/adt/multirangetypes.c:2340-2351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2340-L2351)

## Overview
Determines if a multirange is strictly positioned before (to the left of) a range, implemented by checking if the range is strictly after the multirange.

## Definition


## Detailed Description
This PostgreSQL function implements the "strictly left of" operator (<<) between a multirange and a range. Interestingly, it achieves this by calling  with swapped argument semantics - if a multirange is before a range, then equivalently the range is after the multirange. This demonstrates an elegant implementation technique where the symmetric "after" relationship is used to implement the "before" relationship.

The function extracts the multirange and range arguments from the function call info, retrieves the appropriate type cache entry, and delegates to the internal "after" logic. The internal function compares the range's lower bound against the multirange's upper bound (from its last range element) to determine strict ordering.

## Parameters / Member Variables
- : PostgreSQL function call information containing the multirange and range arguments
  - Argument 0:  - The multirange to compare
  - Argument 1:  - The range to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract multirange argument from function call
  -  - Extract range argument from function call
  -  - Get type cache entry for multirange operations
  -  - Get OID of multirange type
  -  - Internal implementation using symmetric "after" logic
- Called from (representative examples):
  - SQL queries using the << operator between multirange and range types

## Notes and Other Information
- This function implements the << (strictly left of) operator for multirange-range comparisons
- Uses a clever implementation approach: "multirange before range" is equivalent to "range after multirange"
- Returns false for empty multiranges or ranges, following PostgreSQL's convention for spatial operations
- The internal logic compares the range's lower bound with the multirange's rightmost (last) range's upper bound
- Part of PostgreSQL's multirange type system that provides comprehensive spatial relationship operators
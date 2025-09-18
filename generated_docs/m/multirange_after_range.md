# multirange_after_range

## Location
[src/backend/utils/adt/multirangetypes.c:2377-2388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2377-L2388)

## Overview
Determines if a multirange is strictly positioned after (to the right of) a range, implemented by checking if the range is strictly before the multirange.

## Definition


## Detailed Description
This PostgreSQL function implements the "strictly right of" operator (>>) between a multirange and a range. Like its counterpart , it uses a clever symmetric implementation approach. Instead of implementing dedicated "after" logic, it calls  with swapped argument semantics - if a multirange is after a range, then equivalently the range is before the multirange.

The function extracts the multirange and range arguments from the function call info, retrieves the appropriate type cache entry, and delegates to the internal "before" logic. This demonstrates efficient code reuse where symmetric spatial relationships are implemented using a single underlying comparison function, reducing code duplication and potential inconsistencies.

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
  -  - Internal implementation using symmetric "before" logic
- Called from (representative examples):
  - SQL queries using the >> operator between multirange and range types

## Notes and Other Information
- This function implements the >> (strictly right of) operator for multirange-range comparisons
- Uses symmetric implementation: "multirange after range" is equivalent to "range before multirange"
- Returns false for empty multiranges or ranges, following PostgreSQL's convention for spatial operations
- Complements  function, together providing bidirectional spatial comparison operators
- The internal logic compares the multirange's leftmost lower bound with the range's upper bound
- Part of PostgreSQL's multirange type system that provides comprehensive spatial relationship operators
- Demonstrates elegant code design through symmetrical operator implementation
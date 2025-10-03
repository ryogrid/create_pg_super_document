# multirange_elem_bsearch_comparison

## Location
[src/backend/utils/adt/multirangetypes.c:1674-1706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1674-L1706)

## Overview
A static comparison function used for binary search to determine if a given element is contained within any range of a multirange by comparing it against range bounds.

## Definition

```c
static int
multirange_elem_bsearch_comparison(TypeCacheEntry *typcache,
								   RangeBound *lower, RangeBound *upper,
								   void *key, bool *match)
```
## Detailed Description
This function serves as a comparison callback for binary search operations when checking if an element is contained within any range of a multirange. It compares a given key element against the lower and upper bounds of a range to determine the element's position relative to that range. The function returns -1 if the key is before the range, 1 if after the range, and 0 if the key is contained within the range (setting *match to true).

The function handles both finite and infinite bounds, using the appropriate comparison function from the type cache. For finite bounds, it performs collation-aware comparisons and respects the inclusiveness/exclusiveness of the bounds.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison function information and collation settings for the range element type
- `*lower`: Lower bound of the range being compared against
- `*upper`: Upper bound of the range being compared against
- `*key`: Pointer to the key element value being searched for (cast to Datum)
- `*match`: Output parameter set to true if the key is found to be contained within the range bounds
## Dependencies
- Functions called/Symbols referenced:
  - RangeBound (struct type)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (function call interface)
  - [DatumGetInt32](../D/DatumGetInt32.md) (datum conversion utility)
- Called from (representative examples):
  - [multirange_contains_elem_internal](multirange_contains_elem_internal.md)

## Notes and Other Information
This is a static helper function specifically designed for use with binary search algorithms. It follows the standard comparison function contract returning negative, zero, or positive values. The function properly handles infinite bounds and respects the inclusiveness flags of range boundaries. The comparison operations are collation-aware, using the collation information stored in the type cache.
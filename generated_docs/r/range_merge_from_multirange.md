# range_merge_from_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2675-2712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2675-L2712)

## Overview
A PostgreSQL function that converts a multirange into a single range by finding the smallest range that encompasses all individual ranges within the multirange.

## Definition

```c
typedef struct
	{
		MultirangeType *mr;
		TypeCacheEntry *typcache;
		int			index;
	} multirange_unnest_fctx;
```
## Detailed Description
The  function performs a crucial operation in PostgreSQL's multirange system by creating a single range that spans from the lowest bound of the first range to the highest bound of the last range in a multirange. This effectively creates the "convex hull" of all ranges in the multirange.

The function handles three distinct cases:
1. **Empty multirange**: Returns an empty range of the appropriate type
2. **Single range**: Returns that single range directly (optimization)
3. **Multiple ranges**: Creates a new range spanning from the lower bound of the first range to the upper bound of the last range

The resulting range may include values that were not present in the original multirange if there were gaps between the constituent ranges. For example, a multirange containing [1,3) and [5,7) would merge to [1,7), which includes the gap [3,5).

This function is particularly useful for operations that need to work with the overall span of a multirange rather than its individual components.

## Parameters
- `fcinfo`: Standard PostgreSQL function calling convention
  - First argument (index 0): The multirange to be merged into a single range

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract multirange argument from function call
  - : Gets the OID of the multirange type
  - : Retrieves type cache information for the multirange
  - : Checks if the multirange contains no ranges
  - : Creates an empty range of the specified type
  - : Extracts a specific range from the multirange
  - : Extracts the bounds of a specific range within the multirange
  - : Creates a new range from the specified bounds
  - : Macro to return a range value from the function

- Called from (representative examples):
  - SQL functions that need to convert multiranges to ranges
  - [Query](../Q/Query.md) operations requiring the overall span of a multirange
  - [Range](../R/Range.md) algebra operations

## Notes and Other Information
- The function preserves the underlying range type of the multirange's constituent ranges
- The resulting range may be larger than the union of the original ranges if there were gaps
- Performance is O(1) for empty and single-range multiranges, O(1) for multi-range cases since it only examines the first and last ranges
- The function assumes that ranges within the multirange are stored in sorted order (a multirange invariant)
- Type safety is maintained through the type cache system
- This is a lossy operation - information about gaps between ranges is lost in the conversion
- The function is essential for interoperability between multirange and range types in PostgreSQL

## Simplified Source

```c
Datum
range_merge_from_multirange(PG_FUNCTION_ARGS)
{
    // Extract multirange argument and get type information
    MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(0);
    Oid multirange_type_oid = MultirangeTypeGetOid(multirange);
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, multirange_type_oid);

    RangeType *result;

    // Handle empty multirange
    if (MultirangeIsEmpty(multirange))
    {
        result = make_empty_range(typcache->rngtype);
    }
    // Optimization: single range case
    else if (multirange->rangeCount == 1)
    {
        result = multirange_get_range(typcache->rngtype, multirange, 0);
    }
    // Multiple ranges: merge from first lower bound to last upper bound
    else
    {
        RangeBound first_lower, first_upper, last_lower, last_upper;

        // Get bounds of first and last ranges
        multirange_get_bounds(typcache->rngtype, multirange, 0, &first_lower, &first_upper);
        multirange_get_bounds(typcache->rngtype, multirange, multirange->rangeCount - 1, &last_lower, &last_upper);

        // Create spanning range from first lower to last upper bound
        result = make_range(typcache->rngtype, &first_lower, &last_upper, false, NULL);
    }

    PG_RETURN_RANGE_P(result);
}
```
# make_range

## Location
[src/backend/utils/adt/rangetypes.c:1952-2015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1952-L2015)

## Overview
Creates and returns a properly serialized and canonicalized RangeType from the provided bounds, handling both empty and non-empty ranges.

## Definition

```c
struct Node *escontext)
{
	RangeType  *range;

	range = range_serialize(typcache, lower, upper, empty, escontext);

	if (SOFT_ERROR_OCCURRED(escontext))
		return NULL;

	/* no need to call canonical on empty ranges ... */
	if (OidIsValid(typcache->rng_canonical_finfo.fn_oid) &&
		!RangeIsEmpty(range))
	{
		/* Do this the hard way so that we can pass escontext */
		LOCAL_FCINFO(fcinfo, 1);
		Datum		result;

		InitFunctionCallInfoData(*fcinfo, &typcache->rng_canonical_finfo, 1,
								 InvalidOid, escontext, NULL);

		fcinfo->args[0].value = RangeTypePGetDatum(range);
		fcinfo->args[0].isnull = false;

		result = FunctionCallInvoke(fcinfo);

		if (SOFT_ERROR_OCCURRED(escontext))
			return NULL;

		/* Should not get a null result if there was no error */
		if (fcinfo->isnull)
			elog(ERROR, "function %u returned NULL",
				 typcache->rng_canonical_finfo.fn_oid);

		range = DatumGetRangeTypeP(result);
	}

	return range;
}

/*
 * Compare two range boundary points, returning <0, 0, or >0 according to
 * whether b1 is less than, equal to, or greater than b2.
 *
 * The boundaries can be any combination of upper and lower;
```
## Detailed Description
The  function is a high-level constructor for RangeType objects that performs both serialization and canonicalization (when applicable). It serves as the primary entry point for most callers who need to create range objects. The function first serializes the range using , then applies the canonical function if one is defined for the range type and the range is not empty. This ensures that equivalent ranges have identical internal representations, which is crucial for proper comparison and indexing operations.

## Parameters / Member Variables
- : Type cache entry containing metadata about the range type, including canonicalization function information
- : Pointer to the lower bound of the range
- : Pointer to the upper bound of the range  
- : Boolean flag indicating whether to create an empty range
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [range_serialize](../r/range_serialize.md)
  - SOFT_ERROR_OCCURRED
  - RangeIsEmpty
  - LOCAL_FCINFO
  - InitFunctionCallInfoData
  - RangeTypePGetDatum
  - FunctionCallInvoke
  - DatumGetRangeTypeP
- Called from (representative examples):
  - [range_in](../r/range_in.md)
  - [range_recv](../r/range_recv.md)
  - [range_constructor2](../r/range_constructor2.md)
  - [range_constructor3](../r/range_constructor3.md)
  - [range_union_internal](../r/range_union_internal.md)
  - [range_intersect_internal](../r/range_intersect_internal.md)
  - [make_empty_range](make_empty_range.md)

## Notes and Other Information
- This function handles soft errors through the escontext parameter, returning NULL when errors occur
- Canonicalization is only applied to non-empty ranges when a canonical function is available
- The function is the recommended way to create RangeType objects as it ensures proper serialization and canonicalization
- Empty ranges bypass canonicalization since they have a standardized representation regardless of bounds
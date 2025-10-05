# multirange_get_bounds

## Location
[src/backend/utils/adt/multirangetypes.c:744-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L744-L801)

## Overview
This function efficiently extracts the lower and upper bounds from the i-th range within a multirange, providing a performance-optimized alternative to deserializing a complete range object.

## Definition

```c
struct union range from the multirange.
 */
RangeType *
multirange_get_union_range(TypeCacheEntry *rangetyp,
						   const MultirangeType *mr)
{
	RangeBound	lower,
				upper,
				tmp;

	if (MultirangeIsEmpty(mr))
		return make_empty_range(rangetyp);

	multirange_get_bounds(rangetyp, mr, 0, &lower, &tmp);
	multirange_get_bounds(rangetyp, mr, mr->rangeCount - 1, &tmp, &upper);

	return make_range(rangetyp, &lower, &upper, false, NULL);
}


/*
 * multirange_deserialize: deconstruct a multirange value
 *
 * NB: the given multirange object must be fully detoasted;
```
## Detailed Description
The function provides a streamlined way to access range bounds without the overhead of constructing a complete RangeType structure. It directly extracts bounds data from the multirange's compressed format, handling type-specific considerations like alignment and value fetching. The function properly handles both finite and infinite bounds, as well as inclusive/exclusive boundaries.

This is an optimized shortcut that performs the equivalent of calling multirange_get_range() followed by range_deserialize(), but with significantly fewer operations and memory allocations. The function ensures that empty ranges are never encountered (as multiranges cannot contain empty ranges) and correctly interprets the range flags to populate the RangeBound structures.

## Parameters / Member Variables
- `tmp`: TypeCacheEntry containing metadata about the range element type
- `make_empty_range(rangetyp)`: Pointer to the source MultirangeType structure
- `&tmp)`: Zero-based index of the range to extract bounds from
- `&upper)`: Output parameter for the lower bound information
- `NULL)`: Output parameter for the upper bound information

## Dependencies
- Functions called/Symbols referenced:
  - [multirange_get_bounds_offset](multirange_get_bounds_offset.md)
  - MultirangeGetFlagsPtr
  - MultirangeGetBoundariesPtr
  - RANGE_EMPTY
  - RANGE_HAS_LBOUND
  - RANGE_HAS_UBOUND
  - [fetch_att](../f/fetch_att.md)
  - att_addlength_pointer
  - att_align_pointer
  - RANGE_LB_INF, RANGE_LB_INC
  - RANGE_UB_INF, RANGE_UB_INC
- Called from (representative examples):
  - [multirange_get_union_range](multirange_get_union_range.md)
  - [multirange_bsearch_match](multirange_bsearch_match.md)
  - [multirange_lower](multirange_lower.md)
  - [multirange_upper](multirange_upper.md)
  - [multirange_overlaps_multirange_internal](multirange_overlaps_multirange_internal.md)
  - [range_contains_multirange_internal](../r/range_contains_multirange_internal.md)

## Notes and Other Information
- The function validates the index with an Assert to ensure it's within bounds
- Includes an assertion that multiranges cannot contain empty ranges
- Efficiently handles type alignment without unnecessary alignment operations for lower bounds
- Sets all RangeBound fields including val, infinite, inclusive, and lower flags
- Used extensively throughout multirange comparison and operation functions
- Provides significant performance benefits over full range deserialization when only bounds are needed

## Simplified Source

```c
void
multirange_get_bounds(TypeCacheEntry *rangetyp,
                     const MultirangeType *multirange,
                     uint32 i, RangeBound *lower, RangeBound *upper)
{
    Assert(i < multirange->rangeCount);

    // Get type information for element access
    int16 typlen = rangetyp->rngelemtype->typlen;
    char typalign = rangetyp->rngelemtype->typalign;
    bool typbyval = rangetyp->rngelemtype->typbyval;

    // Get offset to the i-th range's bounds data
    uint32 offset = multirange_get_bounds_offset(multirange, i);
    uint8 flags = MultirangeGetFlagsPtr(multirange)[i];
    Pointer ptr = MultirangeGetBoundariesPtr(multirange, typalign) + offset;

    // Multiranges cannot contain empty ranges
    Assert((flags & RANGE_EMPTY) == 0);

    // Extract lower bound if present
    Datum lbound;
    if (RANGE_HAS_LBOUND(flags)) {
        lbound = fetch_att(ptr, typbyval, typlen);
        ptr = (Pointer) att_addlength_pointer(ptr, typlen, ptr);
    } else {
        lbound = (Datum) 0;
    }

    // Extract upper bound if present
    Datum ubound;
    if (RANGE_HAS_UBOUND(flags)) {
        ptr = (Pointer) att_align_pointer(ptr, typalign, typlen, ptr);
        ubound = fetch_att(ptr, typbyval, typlen);
    } else {
        ubound = (Datum) 0;
    }

    // Set lower bound structure
    lower->val = lbound;
    lower->infinite = (flags & RANGE_LB_INF) != 0;
    lower->inclusive = (flags & RANGE_LB_INC) != 0;
    lower->lower = true;

    // Set upper bound structure
    upper->val = ubound;
    upper->infinite = (flags & RANGE_UB_INF) != 0;
    upper->inclusive = (flags & RANGE_UB_INC) != 0;
    upper->lower = false;
}
```
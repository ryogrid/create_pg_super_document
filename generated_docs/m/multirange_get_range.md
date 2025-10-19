# multirange_get_range

## Location
[src/backend/utils/adt/multirangetypes.c:695-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L695-L743)

## Overview
This function extracts the i-th range from a multirange by constructing a complete RangeType structure with proper bounds data, flags, and memory layout.

## Definition

```c
RangeType *
multirange_get_range(TypeCacheEntry *rangetyp,
					 const MultirangeType *multirange, int i)
```
## Detailed Description
The function reconstructs an individual range from the compressed multirange format. It first calculates the bounds offset using multirange_get_bounds_offset, then extracts the range flags and boundary values. The function carefully handles the variable-length nature of range bounds by walking through the data structure to determine the exact size needed, considering data alignment requirements for the element type.

The resulting RangeType is allocated as a complete, standalone range object that can be used independently of the original multirange. The function handles both lower and upper bounds according to the range flags, properly aligning data and calculating the total memory required.

## Parameters / Member Variables
- `*rangetyp`: TypeCacheEntry containing type information for the range element type
- `*multirange`: Pointer to the source MultirangeType structure
- `i`: Zero-based index of the range to extract
## Dependencies
- Functions called/Symbols referenced:
  - [multirange_get_bounds_offset](multirange_get_bounds_offset.md)
  - MultirangeGetFlagsPtr
  - MultirangeGetBoundariesPtr
  - RANGE_HAS_LBOUND
  - RANGE_HAS_UBOUND
  - att_addlength_pointer
  - att_align_pointer
  - [palloc0](../p/palloc0.md)
  - SET_VARSIZE
  - memcpy
- Called from (representative examples):
  - [multirange_deserialize](multirange_deserialize.md)
  - [range_merge_from_multirange](../r/range_merge_from_multirange.md)
  - [multirange_unnest_fctx](multirange_unnest_fctx.md)
  - PG_RETURN_MULTIRANGE_P

## Notes and Other Information
- The function validates the index with an Assert to ensure it's within bounds
- Memory alignment is handled according to the element type's alignment requirements
- The resulting RangeType includes both the range data and the flags byte
- Uses palloc0 to ensure the allocated memory is zero-initialized
- The function properly reconstructs the variable-length range structure from the compressed multirange format

## Simplified Source

```c
RangeType *
multirange_get_range(TypeCacheEntry *rangetyp,
                     const MultirangeType *multirange, int i)
{
    // Get the offset and flags for the i-th range
    uint32 offset = multirange_get_bounds_offset(multirange, i);
    uint8 flags = MultirangeGetFlagsPtr(multirange)[i];
    Pointer begin = MultirangeGetBoundariesPtr(multirange, typalign) + offset;
    Pointer ptr = begin;

    // Calculate size by walking through bound values
    if (RANGE_HAS_LBOUND(flags))
        ptr = att_addlength_pointer(ptr, typlen, ptr);
    if (RANGE_HAS_UBOUND(flags)) {
        ptr = att_align_pointer(ptr, typalign, typlen, ptr);
        ptr = att_addlength_pointer(ptr, typlen, ptr);
    }

    // Allocate and construct the range object
    uint32 len = (ptr - begin) + sizeof(RangeType) + sizeof(uint8);
    RangeType *range = palloc0(len);
    SET_VARSIZE(range, len);
    range->rangetypid = rangetyp->type_id;

    // Copy bounds data and flags
    memcpy(range + 1, begin, ptr - begin);
    *((uint8 *)(range + 1) + (ptr - begin)) = flags;

    return range;
}
```
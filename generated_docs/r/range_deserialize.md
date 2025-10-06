# range_deserialize

## Location
[src/backend/utils/adt/rangetypes.c:1856-1922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1856-L1922)

## Overview
Deconstructs a serialized range value into its component bounds, flags, and empty status for processing by range functions.

## Definition

```c
void
range_deserialize(TypeCacheEntry *typcache, const RangeType *range,
				  RangeBound *lower, RangeBound *upper, bool *empty)
```
## Detailed Description
This function extracts the internal components of a serialized RangeType object, parsing the binary format to reconstruct the lower bound, upper bound, and empty flag. It reads the flags byte from the end of the range object, then uses type information to properly deserialize the bound values based on their storage characteristics. The function handles both fixed-length and variable-length element types, properly aligning data according to the element type's requirements. For pass-by-reference element types, the returned datums point directly into the original range object's memory.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing metadata about the range type and its element type
- `*range`: Serialized range object to deserialize (must be fully detoasted)
- `*lower`: Output parameter for lower bound information (value, inclusivity, infinity flags)
- `*upper`: Output parameter for upper bound information (value, inclusivity, infinity flags)
- `*empty`: Output parameter indicating whether the range is empty
## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid
  - VARSIZE
  - RANGE_HAS_LBOUND, RANGE_HAS_UBOUND
  - [fetch_att](../f/fetch_att.md)
  - att_addlength_pointer
  - att_align_pointer
  - RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_INC, RANGE_UB_INF, RANGE_UB_INC
- Called from (representative examples):
  - [range_out](range_out.md)
  - [range_send](range_send.md)
  - [range_lower](range_lower.md)
  - [range_upper](range_upper.md)
  - [range_eq_internal](range_eq_internal.md)
  - [range_overlaps_internal](range_overlaps_internal.md)
  - [range_cmp](range_cmp.md)
  - [hash_range](../h/hash_range.md)

## Notes and Other Information
- Requires the input range to be fully detoasted (no short varlena headers)
- For pass-by-reference types, returned datums are pointers into the original range object
- The flags byte is stored at the last byte of the range object
- Properly handles data alignment requirements for different element types
- Widely used throughout the range type system for accessing range components
- Critical function for all range comparison, operation, and I/O functions

## Simplified Source

```c
void
range_deserialize(TypeCacheEntry *typcache, const RangeType *range,
                  RangeBound *lower, RangeBound *upper, bool *empty)
{
    // Extract the flags byte from the last byte of the range object
    char flags = *((const char *) range + VARSIZE(range) - 1);

    // Get element type information for deserialization
    int16 typlen = typcache->rngelemtype->typlen;
    bool typbyval = typcache->rngelemtype->typbyval;
    char typalign = typcache->rngelemtype->typalign;

    // Start reading data after the range type OID
    Pointer ptr = (Pointer) (range + 1);

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

    // Set output parameters
    *empty = (flags & RANGE_EMPTY) != 0;

    // Fill in lower bound structure
    lower->val = lbound;
    lower->infinite = (flags & RANGE_LB_INF) != 0;
    lower->inclusive = (flags & RANGE_LB_INC) != 0;
    lower->lower = true;

    // Fill in upper bound structure
    upper->val = ubound;
    upper->infinite = (flags & RANGE_UB_INF) != 0;
    upper->inclusive = (flags & RANGE_UB_INC) != 0;
    upper->lower = false;
}
```
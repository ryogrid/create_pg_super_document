# EA_get_flat_size

## Location
[src/backend/utils/adt/array_expanded.c:233-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L233-L292)

## Overview
A method function that calculates the size in bytes required to flatten an expanded array into its standard serialized array representation.

## Definition

```c
static Size
EA_get_flat_size(ExpandedObjectHeader *eohptr)
```
## Detailed Description
This function is part of the ExpandedObjectMethods interface for expanded arrays, used to determine how much memory space will be needed to create a flattened (standard PostgreSQL array) representation of an expanded array. It employs multiple optimization strategies to avoid expensive recalculation.

The function first checks if a flattened representation already exists and returns its size directly. If not, it checks for a cached size value. As a last resort, it calculates the size by examining the deconstructed representation (dvalues/dnulls arrays), computing space needed for each non-null element including proper alignment, and adding the appropriate array overhead.

The calculated size is cached in the expanded array header for future calls, making subsequent size queries very efficient.

## Parameters / Member Variables
- : Pointer to the ExpandedObjectHeader (cast to ExpandedArrayHeader internally)

## Dependencies
- Functions called/Symbols referenced:
  - ARR_SIZE
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - ereport, errcode, errmsg (for error handling)
- Called from (representative examples):
  - Used through EA_methods function pointer table in ExpandedObjectMethods interface

## Notes and Other Information
- This is a static function that implements the get_flat_size method in the EA_methods table
- Includes overflow checking to prevent allocation requests exceeding MaxAllocSize
- The function accounts for null bitmap overhead even if no nulls are currently present, as the flattened array will include a null bitmap if dnulls exists
- Proper alignment calculation ensures the resulting size estimate matches actual memory layout requirements
- Caches the calculated size in eah->flat_size to avoid recalculation on subsequent calls
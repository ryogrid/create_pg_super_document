# write_multirange_data

## Location
src/backend/utils/adt/multirangetypes.c: 596 - 645

## Overview
Serializes an array of ranges into a pre-allocated MultirangeType structure, writing the optimized binary representation with items, flags, and boundary data.

## Definition


## Detailed Description
This function performs the actual serialization of ranges into a multirange structure. It populates three main components of the multirange:

1. **Items array**: Contains offset/length information for efficient range access. Uses a stride-based approach where every MULTIRANGE_ITEM_OFFSET_STRIDE-th item stores an absolute offset (marked with MULTIRANGE_ITEM_OFF_BIT), while others store relative lengths from the previous offset.

2. **Flags array**: Stores the flags byte from each range (copied from the end of each range structure), preserving important range properties like emptiness and boundary inclusion.

3. **Boundaries data**: Contains the actual range bound data (excluding range headers) with proper alignment padding.

The function implements an optimization where the first range doesn't need an item entry since its position is implicit, and uses a compression scheme for item values to reduce storage overhead.

## Parameters / Member Variables
- : Pre-allocated MultirangeType structure to write data into
- : TypeCacheEntry containing type information for alignment calculations
- : Number of ranges to serialize
- : Array of RangeType pointers containing the source range data

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeGetItemsPtr (to get pointer to items array)
  - MultirangeGetFlagsPtr (to get pointer to flags array)
  - MultirangeGetBoundariesPtr (to get pointer to boundaries data)
  - MULTIRANGE_ITEM_OFFSET_STRIDE (constant for stride-based optimization)
  - MULTIRANGE_ITEM_OFF_BIT (bit flag marking offset items)
  - VARSIZE (to get size of range structures)
  - memcpy (to copy range boundary data)
  - att_align_nominal (for proper alignment of copied data)
- Called from (representative examples):
  - [make_multirange](../m/make_multirange.md)

## Notes and Other Information
- This is a static function used internally by the multirange construction process
- The stride-based item encoding reduces memory usage by storing absolute offsets only periodically
- Flags are extracted from the last byte of each range structure, preserving range metadata
- The function assumes the target multirange structure has been properly allocated with sufficient space
- Boundary data is copied without the RangeType header and flags byte, storing only the essential bound information
- Proper alignment is maintained throughout the serialization process to ensure efficient memory access
- The optimization of not storing an item for the first range reduces overhead for small multiranges
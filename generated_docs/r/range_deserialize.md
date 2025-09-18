# range_deserialize

## Location
src/backend/utils/adt/rangetypes.c: 1856 - 1922

## Overview
Deconstructs a serialized range value into its component bounds, flags, and empty status for processing by range functions.

## Definition


## Detailed Description
This function extracts the internal components of a serialized RangeType object, parsing the binary format to reconstruct the lower bound, upper bound, and empty flag. It reads the flags byte from the end of the range object, then uses type information to properly deserialize the bound values based on their storage characteristics. The function handles both fixed-length and variable-length element types, properly aligning data according to the element type's requirements. For pass-by-reference element types, the returned datums point directly into the original range object's memory.

## Parameters / Member Variables
- : Type cache entry containing metadata about the range type and its element type
- : Serialized range object to deserialize (must be fully detoasted)
- : Output parameter for lower bound information (value, inclusivity, infinity flags)
- : Output parameter for upper bound information (value, inclusivity, infinity flags)
- : Output parameter indicating whether the range is empty

## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid
  - VARSIZE
  - RANGE_HAS_LBOUND, RANGE_HAS_UBOUND
  - fetch_att
  - att_addlength_pointer
  - att_align_pointer
  - RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_INC, RANGE_UB_INF, RANGE_UB_INC
- Called from (representative examples):
  - range_out
  - range_send
  - range_lower
  - range_upper
  - range_eq_internal
  - range_overlaps_internal
  - range_cmp
  - hash_range

## Notes and Other Information
- Requires the input range to be fully detoasted (no short varlena headers)
- For pass-by-reference types, returned datums are pointers into the original range object
- The flags byte is stored at the last byte of the range object
- Properly handles data alignment requirements for different element types
- Widely used throughout the range type system for accessing range components
- Critical function for all range comparison, operation, and I/O functions
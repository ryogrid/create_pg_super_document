# range_serialize

## Location
src/backend/utils/adt/rangetypes.c: 1727 - 1855

## Overview
Constructs a range value from bound specifications and empty flag, performing validation and serialization into the internal RangeType format.

## Definition


## Detailed Description
This function creates a properly serialized RangeType object from bound specifications. It performs comprehensive validation including checking that lower bounds are not greater than upper bounds, handling infinite and inclusive boundary flags, and ensuring proper canonicalization. The function constructs the internal binary representation by calculating the required storage size, handling TOAST decompression for variable-length data types, and writing the bounds and flags in the correct format. It's primarily intended for use by canonicalization functions and internal range operations.

## Parameters / Member Variables
- : Type cache entry containing metadata about the range type and its element type
- : Lower bound specification with value, inclusivity, and infinity flags
- : Upper bound specification with value, inclusivity, and infinity flags  
- : Boolean flag indicating if the range should be empty
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - range_cmp_bound_values
  - PG_DETOAST_DATUM_PACKED
  - datum_compute_size
  - datum_write
  - SET_VARSIZE
  - RANGE_EMPTY, RANGE_LB_INF, RANGE_LB_INC, RANGE_UB_INF, RANGE_UB_INC
  - RANGE_HAS_LBOUND, RANGE_HAS_UBOUND
- Called from (representative examples):
  - int4range_canonical
  - int8range_canonical
  - daterange_canonical
  - make_range
  - rangesel
  - compute_range_stats

## Notes and Other Information
- Does not force canonicalization of the range value - that's left to caller functions
- Performs datatype-independent canonicalization checks for safety
- Handles TOAST values by decompressing them to avoid storing out-of-line pointers
- Supports soft error handling through the escontext parameter
- The serialized format includes varlena header, range type OID, bounds data, and flags byte
- Zero-fills allocated memory similar to heap tuples for consistency
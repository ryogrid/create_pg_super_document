# make_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 646 - 672

## Overview
Creates a new multirange from an array of ranges by canonicalizing (sorting and merging) the input ranges and serializing them into an optimized binary representation.

## Definition


## Detailed Description
This is the primary constructor function for creating multirange objects in PostgreSQL. It performs the complete process of multirange creation:

1. **Canonicalization**: Sorts the input ranges and merges any overlapping or adjacent ranges using multirange_canonicalize, which may reduce the final range count
2. **Size calculation**: Estimates the required memory using multirange_size_estimate to determine the exact allocation size
3. **Memory allocation**: Allocates zero-filled memory using palloc0 to ensure clean initialization
4. **Structure initialization**: Sets the VARSIZE header, multirange type OID, and range count
5. **Data serialization**: Calls write_multirange_data to serialize the canonicalized ranges into the optimized binary format

The function modifies the input ranges array during canonicalization but does not alter the contents of existing RangeType structures. It is designed to be the standard way to create multiranges from range arrays.

## Parameters / Member Variables
- : OID of the multirange type being created
- : TypeCacheEntry containing type information for the underlying range type
- : Initial number of ranges in the input array (may be reduced after canonicalization)
- : Array of RangeType pointers containing the source ranges (should be detoasted and non-null)

## Dependencies
- Functions called/Symbols referenced:
  - multirange_canonicalize (to sort and merge input ranges)
  - multirange_size_estimate (to calculate required memory size)
  - palloc0 (to allocate zero-filled memory)
  - SET_VARSIZE (to set the PostgreSQL variable-length type header)
  - write_multirange_data (to serialize ranges into the allocated structure)
- Called from (representative examples):
  - multirange_in (text input parsing)
  - multirange_recv (binary input processing)
  - make_empty_multirange (for empty multirange creation)
  - multirange_constructor2, multirange_constructor1, multirange_constructor0 (SQL constructors)
  - multirange_union (union operations)
  - multirange_minus_internal (difference operations)
  - multirange_intersect_internal (intersection operations)
  - range_agg_finalfn (aggregation functions)

## Notes and Other Information
- This function assumes all input ranges are non-null and already detoasted
- The canonicalization step is crucial for ensuring multirange consistency and optimal storage
- Zero-filling during allocation is required for proper PostgreSQL datum handling, similar to heap tuples
- The function may modify the order and content of the ranges array pointers during canonicalization
- This is the recommended function for most multirange creation scenarios in PostgreSQL's C code
- The resulting multirange is fully self-contained and ready for storage or further operations
- Memory allocation failures will be handled by PostgreSQL's standard error handling mechanisms
# multirange_deserialize

## Location
src/backend/utils/adt/multirangetypes.c: 826 - 847

## Overview
This function deconstructos a multirange value into an array of individual RangeType objects, converting from the compressed multirange format to separate range instances.

## Definition


## Detailed Description
The function performs a complete deserialization of a multirange by extracting each constituent range and creating independent RangeType objects. It allocates an array to hold pointers to all the extracted ranges and uses multirange_get_range() to reconstruct each individual range from the compressed multirange format.

The function handles both empty and non-empty multiranges appropriately: for empty multiranges, it sets the ranges array to NULL, while for non-empty multiranges, it allocates memory for the range pointer array and populates it with fully reconstructed range objects. This is essential for operations that need to work with individual ranges rather than the compressed multirange representation.

The input multirange must be fully detoasted and cannot have a short varlena header, ensuring that all data is accessible in the expected format.

## Parameters / Member Variables
- : TypeCacheEntry containing type information for range reconstruction
- : Pointer to the source MultirangeType structure (must be fully detoasted)
- : Output parameter receiving the number of ranges in the multirange
- : Output parameter receiving an allocated array of RangeType pointers

## Dependencies
- Functions called/Symbols referenced:
  - multirange_get_range
  - palloc
  - MultirangeType
- Called from (representative examples):
  - multirange_out
  - multirange_send
  - multirange_union
  - multirange_minus
  - multirange_intersect
  - multirange_agg_transfn
  - PG_RETURN_MULTIRANGE_P

## Notes and Other Information
- Requires the input multirange to be fully detoasted (no short varlena header)
- For empty multiranges, sets ranges to NULL rather than allocating an empty array
- Each extracted range is a complete, independent RangeType object
- The caller is responsible for managing the memory of the allocated ranges array
- Used extensively in multirange operations that need to process individual ranges
- Essential for serialization operations like multirange_out and multirange_send
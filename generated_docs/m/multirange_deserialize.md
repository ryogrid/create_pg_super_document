# multirange_deserialize

## Location
[src/backend/utils/adt/multirangetypes.c:826-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L826-L847)

## Overview
This function deconstructos a multirange value into an array of individual RangeType objects, converting from the compressed multirange format to separate range instances.

## Definition

```c
void
multirange_deserialize(TypeCacheEntry *rangetyp,
					   const MultirangeType *multirange, int32 *range_count,
					   RangeType ***ranges)
```
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
  - [multirange_get_range](multirange_get_range.md)
  - [palloc](../p/palloc.md)
  - MultirangeType
- Called from (representative examples):
  - [multirange_out](multirange_out.md)
  - [multirange_send](multirange_send.md)
  - [multirange_union](multirange_union.md)
  - [multirange_minus](multirange_minus.md)
  - [multirange_intersect](multirange_intersect.md)
  - [multirange_agg_transfn](multirange_agg_transfn.md)
  - PG_RETURN_MULTIRANGE_P

## Notes and Other Information
- Requires the input multirange to be fully detoasted (no short varlena header)
- For empty multiranges, sets ranges to NULL rather than allocating an empty array
- Each extracted range is a complete, independent RangeType object
- The caller is responsible for managing the memory of the allocated ranges array
- Used extensively in multirange operations that need to process individual ranges
- Essential for serialization operations like multirange_out and multirange_send
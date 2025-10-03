# make_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:646-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L646-L672)

## Overview
Creates a new multirange from an array of ranges by canonicalizing (sorting and merging) the input ranges and serializing them into an optimized binary representation.

## Definition

```c
MultirangeType *
make_multirange(Oid mltrngtypoid, TypeCacheEntry *rangetyp, int32 range_count,
				RangeType **ranges)
```
## Detailed Description
This is the primary constructor function for creating multirange objects in PostgreSQL. It performs the complete process of multirange creation:

1. **Canonicalization**: Sorts the input ranges and merges any overlapping or adjacent ranges using multirange_canonicalize, which may reduce the final range count
2. **Size calculation**: Estimates the required memory using multirange_size_estimate to determine the exact allocation size
3. **Memory allocation**: Allocates zero-filled memory using palloc0 to ensure clean initialization
4. **Structure initialization**: Sets the VARSIZE header, multirange type OID, and range count
5. **Data serialization**: Calls write_multirange_data to serialize the canonicalized ranges into the optimized binary format

The function modifies the input ranges array during canonicalization but does not alter the contents of existing RangeType structures. It is designed to be the standard way to create multiranges from range arrays.

## Parameters / Member Variables
- `mltrngtypoid`: OID of the multirange type being created
- `*rangetyp`: TypeCacheEntry containing type information for the underlying range type
- `range_count`: Initial number of ranges in the input array (may be reduced after canonicalization)
- `**ranges`: Array of RangeType pointers containing the source ranges (should be detoasted and non-null)
## Dependencies
- Functions called/Symbols referenced:
  - [multirange_canonicalize](multirange_canonicalize.md) (to sort and merge input ranges)
  - [multirange_size_estimate](multirange_size_estimate.md) (to calculate required memory size)
  - [palloc0](../p/palloc0.md) (to allocate zero-filled memory)
  - SET_VARSIZE (to set the PostgreSQL variable-length type header)
  - [write_multirange_data](../w/write_multirange_data.md) (to serialize ranges into the allocated structure)
- Called from (representative examples):
  - [multirange_in](multirange_in.md) (text input parsing)
  - [multirange_recv](multirange_recv.md) (binary input processing)
  - [make_empty_multirange](make_empty_multirange.md) (for empty multirange creation)
  - [multirange_constructor2](multirange_constructor2.md), multirange_constructor1, multirange_constructor0 (SQL constructors)
  - [multirange_union](multirange_union.md) (union operations)
  - [multirange_minus_internal](multirange_minus_internal.md) (difference operations)
  - [multirange_intersect_internal](multirange_intersect_internal.md) (intersection operations)
  - [range_agg_finalfn](../r/range_agg_finalfn.md) (aggregation functions)

## Notes and Other Information
- This function assumes all input ranges are non-null and already detoasted
- The canonicalization step is crucial for ensuring multirange consistency and optimal storage
- Zero-filling during allocation is required for proper PostgreSQL datum handling, similar to heap tuples
- The function may modify the order and content of the ranges array pointers during canonicalization
- This is the recommended function for most multirange creation scenarios in PostgreSQL's C code
- The resulting multirange is fully self-contained and ready for storage or further operations
- Memory allocation failures will be handled by PostgreSQL's standard error handling mechanisms
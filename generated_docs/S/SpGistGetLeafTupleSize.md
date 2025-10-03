# SpGistGetLeafTupleSize

## Location
[src/backend/access/spgist/spgutils.c:810-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L810-L862)

## Overview
Calculates the total storage space required for an SP-GiST leaf tuple that will hold the given attribute data, including proper alignment and minimum size constraints.

## Definition

```c
Size
SpGistGetLeafTupleSize(TupleDesc tupleDescriptor,
					   const Datum *datums, const bool *isnulls)
```
## Detailed Description
This function computes the total space needed for a leaf tuple in an SP-GiST index. It must match the size calculation logic used in spgFormLeafTuple to ensure consistency. The calculation includes:

1. **Null bitmap decision**: For compatibility with pre-v14 layout, single-attribute tuples (natts == 1) never use a null bitmask. Multi-attribute tuples use a bitmask only if any attribute is null.

2. **Data size calculation**: Uses heap_compute_data_size() to calculate the space needed for the actual attribute data, following the same logic as heap tuples.

3. **Header size**: Adds the appropriate header size using SGLTHDRSZ macro, which varies based on whether a null mask is needed.

4. **Alignment**: Ensures the total size is properly aligned using MAXALIGN.

5. **Minimum size**: Guarantees the tuple is at least SGDTSIZE bytes to allow future replacement with dead tuples.

## Parameters / Member Variables
- `tupleDescriptor`: TupleDesc structure describing the tuple's attribute schema
- `*datums`: Array of Datum values for each attribute
- `*isnulls`: Array of boolean flags indicating which attributes are null
## Dependencies
- Functions called/Symbols referenced:
  - [heap_compute_data_size](../h/heap_compute_data_size.md) (computes data storage requirements)
  - SGLTHDRSZ (macro for leaf tuple header size)
  - SGDTSIZE (macro for dead tuple minimum size)
  - MAXALIGN (macro for memory alignment)
  - [SpGistLeafTuple](SpGistLeafTuple.md) (related structure type)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md) (during index insertion operations)

## Notes and Other Information
- This function must be kept in sync with spgFormLeafTuple's size calculations
- The compatibility logic for single-attribute tuples maintains backward compatibility with PostgreSQL versions prior to v14
- The minimum size constraint (SGDTSIZE) is a safety measure to enable tuple replacement operations
- The calculation follows heap tuple conventions for data layout, making SP-GiST leaf tuples similar to regular heap tuples in their data organization
# compare_rows

## Location
[src/backend/commands/analyze.c:1315-1344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1315-L1344)

## Overview
A comparator function used for sorting HeapTuple arrays by their physical storage position (ItemPointer) to maintain tuples in table order during analysis operations.

## Definition


## Detailed Description
The compare_rows function serves as a comparator for qsort operations on arrays of HeapTuple pointers. It compares two HeapTuples based on their physical storage location within the table, specifically comparing their ItemPointer values (t_self field). 

The comparison is performed hierarchically: first by block number, then by offset number within the block. This ensures that tuples are ordered according to their physical position in the table, which is essential for computing correlation statistics during table analysis.

The function follows the standard qsort comparator convention, returning negative, zero, or positive values to indicate the relative ordering of the two input tuples.

## Parameters / Member Variables
- : Pointer to the first HeapTuple pointer to compare
- : Pointer to the second HeapTuple pointer to compare  
- : Unused argument (required by qsort_interruptible interface)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (via qsort_interruptible)

## Notes and Other Information
- Used specifically with qsort_interruptible for sorting sampled tuples by physical position
- The physical ordering is crucial for correlation analysis in PostgreSQL's statistics collection
- Compares block numbers first, then offset numbers within blocks for complete ordering
- Returns -1, 0, or 1 following standard C library qsort comparator conventions
- The arg parameter is unused but required for compatibility with qsort_interruptible interface
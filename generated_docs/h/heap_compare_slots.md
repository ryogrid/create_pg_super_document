# heap_compare_slots

## Location
src/backend/executor/nodeGatherMerge.c: 745 - 781

## Overview
Compares two tuples stored in TupleTableSlots by evaluating their sort key columns to determine their relative ordering for merge operations.

## Definition
```c
static int32 heap_compare_slots(Datum a, Datum b, void *arg)
```

## Detailed Description
This function is a comparator callback used in PostgreSQL's Gather Merge operations to maintain sorted order when merging results from multiple sources. It implements the standard comparator interface required by binary heap operations, taking two Datum values that represent slot numbers and a context argument.

The function receives slot numbers as Datum values, converts them to integers, and then retrieves the corresponding TupleTableSlots from the GatherMergeState. It performs a column-by-column comparison using the sort keys defined in the merge operation. For each sort key column, it extracts the attribute values from both tuples and applies the appropriate comparison function through ApplySortComparator.

The comparison follows PostgreSQL's standard sort semantics, handling NULL values according to the sort specification. The function uses INVERT_COMPARE_RESULT to adjust the comparison result for heap operations, as binary heaps typically maintain a min-heap structure but PostgreSQL merge operations may need different ordering.

The function stops as soon as it finds a difference between the tuples on any sort key column, returning the comparison result immediately. If all sort key columns are equal, it returns 0 indicating the tuples are equivalent for sorting purposes.

## Parameters / Member Variables
- `a`: Datum containing the slot number of the first tuple to compare (converted via DatumGetInt32)
- `b`: Datum containing the slot number of the second tuple to compare (converted via DatumGetInt32)
- `arg`: Void pointer to the GatherMergeState structure containing the tuple slots and sort key specifications

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - TupIsNull
  - slot_getattr
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - INVERT_COMPARE_RESULT
  - [GatherMergeState](../G/GatherMergeState.md)
  - SlotNumber
  - SortSupport
- Called from (representative examples):
  - [gather_merge_setup](../g/gather_merge_setup.md)
  - ExecInitMergeAppend (in nodeMergeAppend.c)

## Notes and Other Information
- Returns negative, zero, or positive value indicating the relative order of the two tuples
- Used as a callback function for binary heap operations in merge algorithms
- Implements multi-column sorting by iterating through all sort keys until a difference is found
- Handles NULL values according to PostgreSQL's sort semantics via ApplySortComparator
- The INVERT_COMPARE_RESULT macro adjusts the comparison result for proper heap ordering
- Also used in MergeAppend operations, indicating its general utility for tuple comparison in merge contexts
- Assumes both input slots contain valid (non-NULL) tuples as verified by assertions
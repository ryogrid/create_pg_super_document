# tuplesort_getheaptuple

## Location
src/backend/utils/sort/tuplesortvariants.c: 928 - 948

## Overview
Fetches the next heap tuple from a sorted tuplesort operation in either forward or backward direction, returning a direct pointer to the tuple.

## Definition
```c
HeapTuple tuplesort_getheaptuple(Tuplesortstate *state, bool forward)
```

## Detailed Description
This function retrieves the next HeapTuple from a completed sorting operation, providing direct access to the tuple data without copying. The returned tuple belongs to the tuplesort's memory context and must not be freed by the caller. This is a lightweight interface compared to tuplesort_gettupleslot, as it avoids the overhead of slot management and copying, but requires careful handling since the returned pointer becomes invalid after any further manipulation of the tuplesort state.

## Parameters / Member Variables
- `state`: Tuplesortstate pointer representing the sorting operation to fetch from
- `forward`: Boolean indicating direction of iteration (true for forward, false for backward)

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - MemoryContextSwitchTo
  - tuplesort_gettuple_common
- Called from (representative examples):
  - heapam_relation_copy_for_cluster

## Notes and Other Information
- Returns NULL if no more tuples are available
- The returned tuple pointer is only valid until the next tuplesort operation
- More efficient than tuplesort_gettupleslot for cases where direct tuple access is sufficient
- Primarily used in specialized contexts like table clustering operations
- No abbreviation support since it returns the raw tuple pointer
- Part of the low-level tuplesort interface for direct heap tuple access
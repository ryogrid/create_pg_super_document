# ExecInitNullTupleSlot

## Location
src/backend/executor/execTuples.c: 1934 - 1954

## Overview
Creates a tuple slot containing an all-nulls tuple of the specified type, primarily used as a substitute input tuple for outer join operations.

## Definition
```c
TupleTableSlot *ExecInitNullTupleSlot(EState *estate, TupleDesc tupType, const TupleTableSlotOps *tts_ops)
```

## Detailed Description
ExecInitNullTupleSlot creates a specialized tuple slot that contains a tuple where all attributes are NULL values. This function first creates an extra tuple slot using ExecInitExtraTupleSlot, then populates it with all NULL values using ExecStoreAllNullTuple. This is essential for implementing outer join semantics in PostgreSQL, where non-matching rows from one side of the join need to be paired with NULL values from the other side. The resulting slot is immediately ready for use as it comes pre-loaded with the NULL tuple.

## Parameters / Member Variables
- `estate`: Pointer to the execution state containing the tuple table
- `tupType`: Tuple descriptor defining the structure and types of the NULL tuple
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the operations for the tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecInitExtraTupleSlot: Creates the underlying tuple slot
  - ExecStoreAllNullTuple: Fills the slot with NULL values
  - TupleTableSlotOps: Structure defining slot operations
- Called from (representative examples):
  - ExecInitHashJoin: Hash join initialization for outer join NULL padding
  - ExecInitMergeJoin: Merge join initialization for outer join NULL padding
  - ExecInitNestLoop: Nested loop join initialization for outer join scenarios

## Notes and Other Information
- Specifically designed for outer join operations where NULL padding is required
- Returns a slot that is already populated with NULL values, ready for immediate use
- The NULL tuple respects the structure defined by the provided tuple descriptor
- Essential for LEFT, RIGHT, and FULL OUTER JOIN implementations
- Used primarily by join executor nodes that need to handle non-matching tuples
- Located in src/backend/executor/execTuples.c:1934-1954
# setop_retrieve_direct

## Location
[src/backend/executor/nodeSetOp.c:227-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L227-L338)

## Overview
setop_retrieve_direct implements the direct (non-hashed) strategy for set operations, processing sorted input tuples by grouping consecutive identical tuples and applying set operation logic.

## Definition

```c
static TupleTableSlot *
setop_retrieve_direct(SetOpState *setopstate)
```
## Detailed Description
This function implements the core logic for set operations when inputs are sorted and can be processed directly without hashing. It operates by:

1. **Group Detection**: Reads tuples from the outer plan and groups consecutive identical tuples together using equality comparison functions
2. **Tuple Counting**: For each group, counts occurrences and tracks tuple flags (used for distinguishing left vs right input in operations like EXCEPT)
3. **Output Determination**: Based on the set operation type and counts, determines how many copies of each group should be output
4. **State Management**: Maintains group boundaries by saving the first tuple of the next group when a boundary is crossed

The function processes one group at a time, scanning through all tuples in the current group before determining the output. It handles the transition between groups by preserving the first tuple of the next group for the subsequent iteration.

## Parameters / Member Variables
- : Pointer to the SetOpState structure containing the execution state, including tuple storage, counting information, equality functions, and output control

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (gets the outer plan state)
  - ExecProcNode (executes outer plan to get next tuple) 
  - TupIsNull (checks if tuple slot is empty)
  - ExecCopySlotHeapTuple (creates heap tuple copy)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md) (stores tuple in slot)
  - initialize_counts (resets per-group counters)
  - advance_counts (updates counters for a tuple)
  - fetch_tuple_flag (gets tuple's flag value)
  - ExecQualAndReset (evaluates equality expression)
  - set_output_count (determines output count for group)
  - ExecClearTuple (clears tuple slot)
- Called from (representative examples):
  - [ExecSetOp](../E/ExecSetOp.md) (when using direct strategy)

## Notes and Other Information
- Used for sorted inputs where tuples can be grouped by consecutive scanning
- Relies on input being sorted by the set operation's grouping columns
- Handles group boundary detection through equality function evaluation
- Maintains efficiency by processing one group at a time rather than materializing all input
- Part of PostgreSQL's two-strategy approach for set operations (direct vs hashed)
- Returns NULL when no more groups are available for processing
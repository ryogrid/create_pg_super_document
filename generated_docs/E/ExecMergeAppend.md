# ExecMergeAppend

## Location
[src/backend/executor/nodeMergeAppend.c:200-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergeAppend.c#L200-L272)

## Overview
The main execution function for MergeAppend nodes that merges pre-sorted tuples from multiple subplans into a single sorted output stream using a binary heap for efficient ordering.

## Definition
```c
static TupleTableSlot *ExecMergeAppend(PlanState *pstate)
```

## Detailed Description
ExecMergeAppend is the core execution function that implements the merge logic for combining sorted streams from multiple child plans. The function operates in two distinct phases:

**Initialization Phase** (first call, ms_initialized = false):
1. **Pruning Check**: If runtime partition pruning is enabled and valid subplans haven't been determined yet, it calls ExecFindMatchingSubPlans to identify which subplans should be executed
2. **Initial Tuple Retrieval**: Pulls the first tuple from each valid subplan using ExecProcNode
3. **Heap Setup**: Adds non-null tuples to the binary heap in unordered fashion, then calls binaryheap_build to establish the heap property
4. **State Update**: Sets ms_initialized to true to indicate initialization is complete

**Execution Phase** (subsequent calls):
1. **Next Tuple Retrieval**: Gets the next tuple from the subplan that provided the previously returned tuple (identified by the heap's first element)
2. **Heap Maintenance**: If the new tuple is non-null, replaces the first heap element; if null (subplan exhausted), removes the first element entirely
3. **Result Selection**: Returns the tuple from the subplan at the top of the heap (smallest according to sort order)

The function uses a binary heap as the core data structure to efficiently maintain sort order across multiple input streams, ensuring O(log n) complexity for each tuple retrieval where n is the number of active subplans.

## Parameters / Member Variables
- : The PlanState pointer that is cast to MergeAppendState, containing the merge execution state including subplans, heap, and sort configuration

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - CHECK_FOR_INTERRUPTS
  - ExecClearTuple
  - ExecFindMatchingSubPlans
  - bms_next_member
  - ExecProcNode
  - TupIsNull
  - binaryheap_add_unordered
  - binaryheap_build
  - binaryheap_first
  - binaryheap_replace_first
  - binaryheap_remove_first
  - binaryheap_empty
  - DatumGetInt32
- Called from (representative examples):
  - ExecInitMergeAppend (sets this as the ExecProcNode function)
  - PostgreSQL executor framework (via function pointer)

## Notes and Other Information
- The function is declared static, meaning it's only accessible within the nodeMergeAppend.c file
- Uses lazy evaluation - tuples are only pulled from subplans when needed, not all at once
- Handles the case where all subplans are exhausted by returning a cleared tuple slot
- The binary heap stores SlotNumber values (integers) that index into the ms_slots array
- Runtime partition pruning allows dynamic determination of which subplans to execute
- The function includes CHECK_FOR_INTERRUPTS() to allow query cancellation during long-running operations
- Returns tuples directly from subplan slots without copying, making it memory efficient
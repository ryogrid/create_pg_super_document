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
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecFindMatchingSubPlans](ExecFindMatchingSubPlans.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [ExecProcNode](ExecProcNode.md)
  - TupIsNull
  - [binaryheap_add_unordered](../b/binaryheap_add_unordered.md)
  - [binaryheap_build](../b/binaryheap_build.md)
  - [binaryheap_first](../b/binaryheap_first.md)
  - [binaryheap_replace_first](../b/binaryheap_replace_first.md)
  - [binaryheap_remove_first](../b/binaryheap_remove_first.md)
  - binaryheap_empty
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Called from (representative examples):
  - [ExecInitMergeAppend](ExecInitMergeAppend.md) (sets this as the ExecProcNode function)
  - PostgreSQL executor framework (via function pointer)

## Notes and Other Information
- The function is declared static, meaning it's only accessible within the nodeMergeAppend.c file
- Uses lazy evaluation - tuples are only pulled from subplans when needed, not all at once
- Handles the case where all subplans are exhausted by returning a cleared tuple slot
- The binary heap stores SlotNumber values (integers) that index into the ms_slots array
- Runtime partition pruning allows dynamic determination of which subplans to execute
- The function includes CHECK_FOR_INTERRUPTS() to allow query cancellation during long-running operations
- Returns tuples directly from subplan slots without copying, making it memory efficient

## Simplified Source

```c
static TupleTableSlot *
ExecMergeAppend(PlanState *pstate)
{
    MergeAppendState *node = castNode(MergeAppendState, pstate);
    TupleTableSlot *result;
    SlotNumber i;

    CHECK_FOR_INTERRUPTS();

    if (!node->ms_initialized) {
        // Nothing to do if all subplans were pruned
        if (node->ms_nplans == 0)
            return ExecClearTuple(node->ps.ps_ResultTupleSlot);

        // Determine valid subplans (runtime pruning)
        if (node->ms_valid_subplans == NULL)
            node->ms_valid_subplans =
                ExecFindMatchingSubPlans(node->ms_prune_state, false);

        // Pull first tuple from each valid subplan and build heap
        i = -1;
        while ((i = bms_next_member(node->ms_valid_subplans, i)) >= 0) {
            node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);
            if (!TupIsNull(node->ms_slots[i]))
                binaryheap_add_unordered(node->ms_heap, Int32GetDatum(i));
        }
        binaryheap_build(node->ms_heap);
        node->ms_initialized = true;
    }
    else {
        // Get next tuple from subplan that provided the last result
        i = DatumGetInt32(binaryheap_first(node->ms_heap));
        node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);

        if (!TupIsNull(node->ms_slots[i]))
            binaryheap_replace_first(node->ms_heap, Int32GetDatum(i));
        else
            binaryheap_remove_first(node->ms_heap);
    }

    // Return the tuple from the top of the heap
    if (binaryheap_empty(node->ms_heap)) {
        result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
    } else {
        i = DatumGetInt32(binaryheap_first(node->ms_heap));
        result = node->ms_slots[i];
    }

    return result;
}
```
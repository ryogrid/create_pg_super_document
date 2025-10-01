# ExecInitMergeAppend

## Location
[src/backend/executor/nodeMergeAppend.c:65-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergeAppend.c#L65-L199)

## Overview
Initializes a MergeAppend plan node by setting up the merge state, initializing subplans, configuring partition pruning if enabled, and preparing sort key information for merging sorted streams from multiple child plans.

## Definition
```c
MergeAppendState *ExecInitMergeAppend(MergeAppend *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitMergeAppend is the initialization function for the MergeAppend executor node, which merges pre-sorted tuples from multiple child plans into a single sorted output stream. The function performs several key setup tasks:

1. **State Creation**: Creates and initializes a MergeAppendState structure that will track the execution state
2. **Partition Pruning Setup**: If runtime partition pruning is enabled (node->part_prune_info != NULL), it initializes the pruning infrastructure and determines which subplans are valid
3. **Subplan Initialization**: Recursively initializes all valid child plan nodes using ExecInitNode
4. **Binary Heap Setup**: Allocates a binary heap data structure using heap_compare_slots as the comparison function to efficiently merge sorted streams
5. **Sort Key Configuration**: Sets up SortSupport structures for each sort column, preparing the comparison functions needed for merging
6. **Memory Management**: Allocates arrays for plan states and tuple slots

The function handles both cases where partition pruning is enabled and disabled, adjusting the set of valid subplans accordingly. It also ensures that abbreviated key conversion is disabled since tuples are pulled into the heap as needed rather than all at once.

## Parameters / Member Variables
- : The MergeAppend plan node containing merge configuration (sort columns, child plans, partition pruning info)
- : The execution state containing transaction context and other execution-wide information
- : Execution flags that control behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecInitPartitionPruning](ExecInitPartitionPruning.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_add_range](../b/bms_add_range.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [binaryheap_allocate](../b/binaryheap_allocate.md)
  - [heap_compare_slots](../h/heap_compare_slots.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecInitNode](ExecInitNode.md)
  - [list_nth](../l/list_nth.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main executor initialization dispatcher)

## Notes and Other Information
- The function explicitly asserts that EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported
- Abbreviated key conversion is intentionally disabled for performance reasons since tuples are processed incrementally
- The binary heap is the core data structure enabling efficient O(log n) merging of sorted streams
- Runtime partition pruning allows dynamic exclusion of unnecessary partitions based on query parameters
- The ms_initialized flag is set to false, indicating that the actual subplan execution hasn't started yet
- [Result](../R/Result.md) tuple slots use virtual tuple table slot operations (TTSOpsVirtual) since they point to tuples from subplans

## Simplified Source

```c
MergeAppendState *
ExecInitMergeAppend(MergeAppend *node, EState *estate, int eflags)
{
    MergeAppendState *mergestate = makeNode(MergeAppendState);
    PlanState **mergeplanstates;
    Bitmapset *validsubplans;
    int nplans, i, j;

    // Validate execution flags - backward scan and mark/restore not supported
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Initialize state structure
    mergestate->ps.plan = (Plan *) node;
    mergestate->ps.state = estate;
    mergestate->ps.ExecProcNode = ExecMergeAppend;

    // Set up partition pruning if enabled
    if (node->part_prune_info != NULL)
    {
        PartitionPruneState *prunestate;

        // Initialize pruning and determine valid subplans
        prunestate = ExecInitPartitionPruning(&mergestate->ps,
                                             list_length(node->mergeplans),
                                             node->part_prune_info,
                                             &validsubplans);
        mergestate->ms_prune_state = prunestate;
        nplans = bms_num_members(validsubplans);

        // Optimize for no runtime pruning case
        if (!prunestate->do_exec_prune && nplans > 0)
            mergestate->ms_valid_subplans = bms_add_range(NULL, 0, nplans - 1);
    }
    else
    {
        // No partition pruning - all subplans are valid
        nplans = list_length(node->mergeplans);
        Assert(nplans > 0);
        mergestate->ms_valid_subplans = validsubplans =
            bms_add_range(NULL, 0, nplans - 1);
        mergestate->ms_prune_state = NULL;
    }

    // Allocate arrays for plan states and slots
    mergeplanstates = (PlanState **) palloc(nplans * sizeof(PlanState *));
    mergestate->mergeplans = mergeplanstates;
    mergestate->ms_nplans = nplans;
    mergestate->ms_slots = (TupleTableSlot **) palloc0(sizeof(TupleTableSlot *) * nplans);

    // Initialize binary heap for merging sorted streams
    mergestate->ms_heap = binaryheap_allocate(nplans, heap_compare_slots, mergestate);

    // Initialize result slot
    ExecInitResultTupleSlotTL(&mergestate->ps, &TTSOpsVirtual);
    mergestate->ps.resultopsset = true;
    mergestate->ps.resultopsfixed = false; // Points to different subplan tuples

    // Initialize valid subplans
    j = 0;
    i = -1;
    while ((i = bms_next_member(validsubplans, i)) >= 0)
    {
        Plan *initNode = (Plan *) list_nth(node->mergeplans, i);
        mergeplanstates[j++] = ExecInitNode(initNode, estate, eflags);
    }

    mergestate->ps.ps_ProjInfo = NULL; // No projection needed

    // Initialize sort key information
    mergestate->ms_nkeys = node->numCols;
    mergestate->ms_sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);

    for (i = 0; i < node->numCols; i++)
    {
        SortSupport sortKey = mergestate->ms_sortkeys + i;

        sortKey->ssup_cxt = CurrentMemoryContext;
        sortKey->ssup_collation = node->collations[i];
        sortKey->ssup_nulls_first = node->nullsFirst[i];
        sortKey->ssup_attno = node->sortColIdx[i];
        sortKey->abbreviate = false; // Disabled for incremental processing

        PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
    }

    // Mark as not yet initialized for execution
    mergestate->ms_initialized = false;

    return mergestate;
}
```
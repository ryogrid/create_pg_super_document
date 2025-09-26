# ExecInitMergeAppend

## Location
src/backend/executor/nodeMergeAppend.c: 65 - 199

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
  - ExecInitPartitionPruning
  - bms_num_members
  - bms_add_range
  - bms_next_member
  - binaryheap_allocate
  - heap_compare_slots
  - ExecInitResultTupleSlotTL
  - ExecInitNode
  - list_nth
  - PrepareSortSupportFromOrderingOp
- Called from (representative examples):
  - ExecInitNode (main executor initialization dispatcher)

## Notes and Other Information
- The function explicitly asserts that EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported
- Abbreviated key conversion is intentionally disabled for performance reasons since tuples are processed incrementally
- The binary heap is the core data structure enabling efficient O(log n) merging of sorted streams
- Runtime partition pruning allows dynamic exclusion of unnecessary partitions based on query parameters
- The ms_initialized flag is set to false, indicating that the actual subplan execution hasn't started yet
- Result tuple slots use virtual tuple table slot operations (TTSOpsVirtual) since they point to tuples from subplans
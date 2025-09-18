# ExecFindMatchingSubPlans

## Location
src/backend/executor/execPartition.c: 2303 - 2365

## Overview
Determines which subplans match the partition pruning steps for the current comparison expression values, returning a bitmap of valid subplan indexes.

## Definition


## Detailed Description
This function is the main entry point for executor-time partition pruning. It iterates through all partition hierarchies defined in the pruning state and determines which subplans should be executed based on the current runtime parameter values. The function supports two modes of operation:

1. **Initial pruning**: Performed during executor initialization when PARAM_EXEC parameters cannot yet be evaluated
2. **Runtime pruning**: Performed during execution when all parameters are available and can be evaluated

The function uses a temporary memory context to avoid memory leaks in the executor's query-lifespan context. For each partition hierarchy, it delegates the actual pruning work to the recursive helper function . After processing all hierarchies, it adds any additional subplans that weren't handled by partition pruning logic.

## Parameters / Member Variables
- : Partition pruning state containing all pruning information, contexts, and subplan mappings
- : Boolean flag indicating whether this is initial pruning (true) or runtime pruning (false)

## Dependencies
- Functions called/Symbols referenced:
  - find_matching_subplans_recurse
  - ResetExprContext 
  - bms_add_members
  - bms_copy
  - MemoryContextSwitchTo
  - MemoryContextReset
- Called from (representative examples):
  - ExecInitPartitionPruning
  - choose_next_subplan_locally
  - choose_next_subplan_for_leader
  - choose_next_subplan_for_worker
  - ExecAppendAsyncBegin
  - ExecMergeAppend

## Notes and Other Information
- The function is defined in src/backend/executor/execPartition.c:2303-2365
- Memory management is carefully handled using temporary contexts to prevent memory leaks
- The function works with complex partition hierarchies and handles both simple partitioned tables and multi-level partitioning schemes
- Expression evaluation contexts are reset after use to free temporary memory
- The returned bitmapset must be freed by the caller
# ExecInitAppend

## Location
[src/backend/executor/nodeAppend.c:109-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L109-L287)

## Overview
Initializes an Append node executor state, setting up all subplans for execution and handling partition pruning, async execution capabilities, and memory allocation for the append operation.

## Definition

```c
structure.  This also initializes the set of
		 * subplans to initialize (validsubplans) by taking into account the
		 * result of performing initial pruning if any.
		 */
		prunestate = ExecInitPartitionPruning(&appendstate->ps,
											  list_length(node->appendplans),
											  node->part_prune_info,
											  &validsubplans);
```
## Detailed Description
ExecInitAppend is the initialization function for PostgreSQL's Append node executor. It creates and configures an AppendState structure that manages the execution of multiple subplans. The function handles several key aspects:

1. **Memory Management**: Allocates all required structures in the executor's top-level memory block to prevent fragmentation during execution
2. **Partition Pruning**: Sets up runtime partition pruning if enabled, which can eliminate unnecessary subplans during execution
3. **Async Execution**: Configures asynchronous execution capabilities for subplans that support it
4. **Subplan Initialization**: Recursively initializes all valid subplans through ExecInitNode
5. **Result Tuple Setup**: Configures the result tuple slot using virtual tuple table slot operations

The function is designed to be potentially wasteful in terms of initialization (as noted in comments) but ensures proper memory layout and avoids allocation during execution.

## Parameters / Member Variables
- : The Append plan node containing the list of subplans to execute
- : The executor state containing global execution context
- : Execution flags that control initialization behavior (EXEC_FLAG_MARK is not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for AppendState creation)
  - [ExecInitPartitionPruning](ExecInitPartitionPruning.md) (for partition pruning setup)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (for result tuple slot initialization)
  - [ExecInitNode](ExecInitNode.md) (for recursive subplan initialization)
  - [bms_num_members](../b/bms_num_members.md), bms_add_range, bms_next_member, bms_add_member (bitmap set operations)
  - [classify_matching_subplans](../c/classify_matching_subplans.md) (for async plan classification)
  - [choose_next_subplan_locally](../c/choose_next_subplan_locally.md) (default subplan selection strategy)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main executor initialization dispatcher)

## Notes and Other Information
- The function asserts that EXEC_FLAG_MARK is not set, as Append nodes do not support mark/restore functionality
- Async execution is disabled during EvalPlanQual (EPQ) operations to maintain consistency
- The function sets up partition pruning state even if no runtime pruning is needed to handle initial pruning results
- Memory allocation is done upfront to ensure all structures are in the same memory context
- The resultopsset flag is set to true but resultopsfixed is false, indicating the node returns slots from various subnodes with potentially different tuple formats
# MergeAppendState

## Location
[src/include/nodes/execnodes.h:1483-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1483-L1495)

## Overview
MergeAppendState is the runtime state structure for the MergeAppend executor node, which merges sorted output from multiple subplans in sorted order.

## Definition

```c
typedef struct MergeAppendState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **mergeplans;		/* array of PlanStates for my inputs */
	int			ms_nplans;
	int			ms_nkeys;
	SortSupport ms_sortkeys;	/* array of length ms_nkeys */
	TupleTableSlot **ms_slots;	/* array of length ms_nplans */
	struct binaryheap *ms_heap; /* binary heap of slot indices */
	bool		ms_initialized; /* are subplans started? */
	struct PartitionPruneState *ms_prune_state;
	Bitmapset  *ms_valid_subplans;
} MergeAppendState;
```
## Detailed Description
MergeAppendState maintains the execution state for a MergeAppend plan node, which combines sorted output from multiple child plans while preserving the sort order. It uses a binary heap to efficiently determine which subplan produces the next tuple in the merged result. The structure supports runtime partition pruning to eliminate unnecessary subplans during execution.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common executor node fields
- `mergeplans`: Array of PlanState pointers for each input subplan
- `ms_nplans`: Number of subplans in the mergeplans array
- `ms_nkeys`: Number of sort key columns used for merging
- `ms_sortkeys`: SortSupport array containing sort key information for efficient comparison
- `ms_slots`: Array of TupleTableSlot pointers, one for each subplan's current output tuple
- `ms_heap`: Binary heap structure used to efficiently find the next tuple in sort order
- `ms_initialized`: Boolean flag indicating whether all subplans have been started and their first tuples fetched
- `ms_prune_state`: Partition pruning state for runtime elimination of unnecessary partitions, or NULL if not applicable
- `ms_valid_subplans`: Bitmapset indicating which subplans are valid for runtime pruning

## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md)
  - [binaryheap](../b/binaryheap.md)
  - [PartitionPruneState](../P/PartitionPruneState.md)
- Called from (representative examples):
  - [ExecInitMergeAppend](../E/ExecInitMergeAppend.md)
  - [ExecMergeAppend](../E/ExecMergeAppend.md)
  - [ExecEndMergeAppend](../E/ExecEndMergeAppend.md)
  - [ExecReScanMergeAppend](../E/ExecReScanMergeAppend.md)

## Notes and Other Information
- Used primarily in partitioned table scans where each partition is sorted and results need to be merged in order
- The binary heap allows efficient O(log n) selection of the next tuple from n subplans
- Runtime partition pruning can significantly improve performance by eliminating subplans that cannot contribute to the result
- The structure extends the base PlanState to inherit common executor functionality
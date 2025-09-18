# ExecFindPartition

## Location
src/backend/executor/execPartition.c: 262 - 494

## Overview
Returns the ResultRelInfo for the leaf partition that a tuple should belong to, performing partition key evaluation and traversing the partition hierarchy to locate the appropriate destination partition.

## Definition


## Detailed Description
This function implements the core partition routing algorithm for PostgreSQL's partitioned tables. It evaluates partition key expressions against the input tuple and traverses the partition hierarchy from root to leaf, handling both single-level and multi-level partitioning schemes. The function employs lazy initialization, creating ResultRelInfo structures only when a partition is first accessed. It also handles tuple format conversion when moving between partitioning levels that have different tuple descriptors, and performs partition constraint validation for default partitions to ensure data consistency.

The algorithm starts at the root partitioned table and iteratively evaluates partition keys to determine the target child partition. For sub-partitioned tables, it recursively descends through the hierarchy until reaching a leaf partition. The function reuses existing ResultRelInfo structures when possible and creates new ones as needed, optimizing memory usage and performance.

## Parameters / Member Variables
- : ModifyTableState containing information about the modify operation and available ResultRelInfo structures
- : The ResultRelInfo for the root relation named in the query
- : PartitionTupleRouting structure containing partition dispatch information and cached ResultRelInfo structures
- : TupleTableSlot containing the tuple to be routed to its appropriate partition
- : Executor state providing expression evaluation context and memory management

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - GetPerTupleMemoryContext
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [FormPartitionKeyDatum](../F/FormPartitionKeyDatum.md)
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md)
  - [ExecBuildSlotPartitionKeyDescription](ExecBuildSlotPartitionKeyDescription.md)
  - [ExecLookupResultRelByOid](ExecLookupResultRelByOid.md)
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [ExecInitRoutingInfo](ExecInitRoutingInfo.md)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md)
  - [ExecInitPartitionDispatchInfo](ExecInitPartitionDispatchInfo.md)
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - ExecClearTuple
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (in copyfrom.c:1055)
  - [ExecPrepareTupleRouting](ExecPrepareTupleRouting.md) (in nodeModifyTable.c:3910)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md) (in worker.c:2942, 3097)

## Notes and Other Information
The function uses per-tuple memory context to avoid memory leaks during partition key evaluation. It handles tuple format conversion when traversing between partitioning levels with different tuple descriptors. Special attention is paid to default partitions, which require constraint validation to ensure the tuple actually belongs there. The function raises appropriate errors if no suitable partition is found or if the target partition is not valid for INSERT operations.
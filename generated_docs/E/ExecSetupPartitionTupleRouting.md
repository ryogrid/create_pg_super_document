# ExecSetupPartitionTupleRouting

## Location
src/backend/executor/execPartition.c: 215 - 261

## Overview
Sets up information needed during tuple routing for partitioned tables, encapsulating it in a PartitionTupleRouting structure that serves as the foundation for efficient partition lookup and tuple routing operations.

## Definition


## Detailed Description
This function initializes the partition routing infrastructure for a partitioned table by creating and configuring a PartitionTupleRouting structure. The design philosophy emphasizes lazy initialization - partition ResultRelInfo structures are built on demand only when a tuple actually needs to be routed to a specific partition. This approach optimizes for the common case where INSERT operations target a single partition, ensuring fast execution for simple scenarios.

The function allocates the main PartitionTupleRouting structure and initializes its core components, including setting up the partition dispatch information for the root partitioned table. The actual partition discovery and ResultRelInfo creation are deferred until ExecFindPartition() is called.

## Parameters / Member Variables
- : The executor state containing execution context and memory management information
- : The root partitioned table relation for which tuple routing needs to be set up

## Dependencies
- Functions called/Symbols referenced:
  - PartitionTupleRouting (struct allocation)
  - ExecInitPartitionDispatchInfo
  - palloc0
  - RelationGetRelid
- Called from (representative examples):
  - CopyFrom (in copyfrom.c:824)
  - ExecCrossPartitionUpdate (in nodeModifyTable.c:1811)
  - ExecInitMerge (in nodeModifyTable.c:3589)
  - ExecInitModifyTable (in nodeModifyTable.c:4644)
  - apply_handle_tuple_routing (in worker.c:2935)

## Notes and Other Information
The function uses the current memory context (typically estate->es_query_cxt) for all allocations, ensuring proper memory lifecycle management. The lazy initialization strategy significantly improves performance for single-partition INSERT operations, which represent a common use case in partitioned table scenarios.
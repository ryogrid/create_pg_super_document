# ExecInitPartitionInfo

## Location
src/backend/executor/execPartition.c: 495 - 985

## Overview
Locks a partition and initializes a complete ResultRelInfo structure for it, setting up all necessary execution state including indexes, constraints, projections, and conflict handling for INSERT/UPDATE/MERGE operations.

## Definition


## Detailed Description
This function performs comprehensive initialization of a partition's execution state when it's first accessed during tuple routing. It opens the partition relation with appropriate locks, creates and configures a ResultRelInfo structure, and sets up all execution components including indexes, WITH CHECK OPTION constraints, RETURNING projections, ON CONFLICT handling, and MERGE operation state. The function handles attribute number mapping between the root table and partition when they have different tuple descriptors, ensuring that expressions and projections work correctly across the partition hierarchy.

The initialization process is comprehensive, covering all aspects of DML operations that might be performed on the partition. This includes setting up speculative insertion capabilities for ON CONFLICT handling, translating column references in constraints and projections, and preparing merge action states for MERGE operations.

## Parameters / Member Variables
- : ModifyTableState containing the modify operation context and reusable ResultRelInfo structures
- : Executor state providing memory contexts, expression evaluation environment, and tuple table management
- : PartitionTupleRouting structure where the new ResultRelInfo will be stored for future reuse
- : PartitionDispatch information for the current partitioning level
- : ResultRelInfo for the root table, used as a template for constraint and projection setup
- : Index of the target partition within the current dispatch level

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (ResultRelInfo creation)
  - InitResultRelInfo
  - CheckValidResultRel
  - ExecOpenIndices
  - build_attrmap_by_name
  - map_variable_attnos
  - ExecInitQual
  - ExecBuildProjectionInfo
  - ExecInitRoutingInfo
  - RelationGetIndexList
  - get_partition_ancestors
  - ExecGetRootToChildMap
  - table_slot_create
  - ExecBuildUpdateProjection
  - ExecInitMergeTupleSlots
  - adjust_partition_colnos
  - adjust_partition_colnos_using_map
- Called from (representative examples):
  - ExecFindPartition (in execPartition.c:373)

## Notes and Other Information
This is a static function that handles the complete initialization of partition execution state. It's designed to be called lazily when a partition is first accessed, supporting efficient partition pruning. The function handles complex attribute mapping scenarios when partitions have different tuple descriptors from their parent tables. It also manages proper memory context switching to ensure allocations are made in the appropriate context for the partition routing infrastructure. The function supports all DML operations (INSERT, UPDATE, DELETE, MERGE) and their associated features like conflict resolution and constraint checking.
# ExecInitPartitionDispatchInfo

## Location
src/backend/executor/execPartition.c: 1094 - 1232

## Overview
Locks a partitioned table and initializes PartitionDispatch structure for efficient partition key evaluation and tuple routing, managing the hierarchical dispatch system for multi-level partitioning.

## Definition


## Detailed Description
This function creates and configures a PartitionDispatch structure for a partitioned table, which contains all the information needed to evaluate partition keys and route tuples to the correct child partitions. It handles both root-level partitioned tables and sub-partitioned tables within a partition hierarchy. For sub-partitioned tables, the function sets up tuple conversion infrastructure when the child table has a different column layout than its parent, ensuring correct partition key evaluation across different tuple formats.

The function manages the partition directory for tracking partition metadata and handles concurrency considerations by optionally excluding partitions being detached (except in snapshot-isolation mode). It also maintains dynamic arrays of PartitionDispatch structures and creates minimal ResultRelInfo structures for non-leaf partitions when needed for constraint checking.

## Parameters / Member Variables
- : Executor state providing partition directory management and memory contexts
- : PartitionTupleRouting structure where the new PartitionDispatch will be stored
- : Object ID of the partitioned table to initialize dispatch information for
- : Parent PartitionDispatch (NULL for root partitioned table) used to establish hierarchy links
- : Index of this partition within the parent's partition list (unused for root table)
- : ResultRelInfo for the root table, used as template for creating sub-partition ResultRelInfo structures

## Dependencies
- Functions called/Symbols referenced:
  - CreatePartitionDirectory
  - IsolationUsesXactSnapshot
  - table_open
  - PartitionDirectoryLookup
  - RelationGetPartitionKey
  - build_attrmap_by_name_if_req
  - MakeSingleTupleTableSlot
  - palloc
  - repalloc
  - makeNode
  - InitResultRelInfo
- Called from (representative examples):
  - ExecSetupPartitionTupleRouting (in execPartition.c:236)
  - ExecFindPartition (in execPartition.c:410)

## Notes and Other Information
This is a static function that handles the complex initialization of partition dispatch infrastructure. It implements sophisticated memory management with dynamically growing arrays using a doubling strategy for efficient scaling. The function handles tuple format conversion between parent and child partitioned tables when their tuple descriptors differ, which is essential for correct partition key evaluation in hierarchical partitioning schemes. The partition directory integration helps optimize partition metadata lookup and handles concurrency scenarios involving partition detachment operations.
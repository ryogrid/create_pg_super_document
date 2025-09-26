# PartitionTupleRouting

## Location
[src/backend/executor/execPartition.c:91-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L91-L142)

## Overview
PartitionTupleRouting encapsulates all information required to route a tuple inserted into a partitioned table to one of its leaf partitions, managing the complex hierarchy of partition dispatch information and result relations.

## Definition

```c
struct PartitionTupleRouting
{
	Relation	partition_root;
	PartitionDispatch *partition_dispatch_info;
	ResultRelInfo **nonleaf_partitions;
	int			num_dispatch;
	int			max_dispatch;
	ResultRelInfo **partitions;
	bool	   *is_borrowed_rel;
	int			num_partitions;
	int			max_partitions;
	MemoryContext memcxt;
};
```
## Detailed Description
PartitionTupleRouting is a central structure in PostgreSQL's partition tuple routing system that manages the complete state needed to route tuples from a partitioned table to their appropriate leaf partitions. It maintains arrays of partition dispatch information for intermediate partitioned tables and result relation information for leaf partitions, handling both borrowed and purpose-built relation info objects. The structure supports dynamic growth as new partitions are encountered during tuple routing operations.

## Parameters / Member Variables
- `partition_root`: The partitioned table that's the target of the command
- `*partition_dispatch_info`: Array of PartitionDispatch objects for every partitioned table touched by tuple routing (target table always at index 0)
- `**nonleaf_partitions`: Array of fake ResultRelInfo objects for nonleaf partitions, used for partition constraint checking
- `num_dispatch`: Current number of items in partition_dispatch_info array, also serves as next free index
- `max_dispatch`: Current allocated size of the partition_dispatch_info array
- `**partitions`: Array of ResultRelInfo pointers for every leaf partition touched by tuple routing
- `*is_borrowed_rel`: Boolean array tracking whether partitions entries are borrowed from ModifyTableState or built locally
- `num_partitions`: Current number of items in partitions array, also serves as next free index
- `max_partitions`: Current allocated size of the partitions array
- `memcxt`: Memory context used to allocate subsidiary structures

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDispatch](PartitionDispatch.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecSetupPartitionTupleRouting](../E/ExecSetupPartitionTupleRouting.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [ExecInitRoutingInfo](../E/ExecInitRoutingInfo.md)
  - [ExecCleanupTupleRouting](../E/ExecCleanupTupleRouting.md)
  - [ExecInsert](../E/ExecInsert.md)
  - [ExecPrepareTupleRouting](../E/ExecPrepareTupleRouting.md)

## Notes and Other Information
The structure is designed to efficiently manage partition hierarchy traversal during tuple insertion operations. The arrays use indexed access patterns where the indexes array in PartitionDispatchData coordinates access to both the partition_dispatch_info and partitions arrays. Memory management is centralized through the memcxt field to ensure proper cleanup of all subsidiary structures.
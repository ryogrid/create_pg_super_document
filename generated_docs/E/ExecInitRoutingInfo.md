# ExecInitRoutingInfo

## Location
[src/backend/executor/execPartition.c:986-1093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L986-L1093)

## Overview
Sets up information needed for translating tuples between root partitioned table format and partition format, managing the storage and tracking of partition-specific tuple routing information.

## Definition

```c
static void
ExecInitRoutingInfo(ModifyTableState *mtstate,
					EState *estate,
					PartitionTupleRouting *proute,
					PartitionDispatch dispatch,
					ResultRelInfo *partRelInfo,
					int partidx,
					bool is_borrowed_rel)
```
## Detailed Description
This function configures the tuple routing infrastructure for a specific partition by setting up tuple format conversion capabilities, initializing Foreign Data Wrapper (FDW) support for foreign table partitions, and managing the storage of ResultRelInfo structures in the partition routing system. It determines whether tuple conversion is needed between the root table and partition formats, creating dedicated tuple slots when necessary. The function also handles FDW-specific initialization for foreign table partitions, including batch insertion support configuration.

The function manages dynamic arrays that track all initialized partitions, growing them as needed using a doubling strategy. It maintains parallel arrays for partition ResultRelInfo structures and flags indicating whether each partition's ResultRelInfo was borrowed from the ModifyTableState or newly created.

## Parameters / Member Variables
- : ModifyTableState containing the execution context for the modify operation
- : Executor state providing tuple table management and other execution resources
- : PartitionTupleRouting structure that tracks all partition routing information
- : PartitionDispatch for the current partitioning level being processed
- : ResultRelInfo for the partition being initialized
- : Index of the partition within the current dispatch level
- : Flag indicating whether the ResultRelInfo was reused from ModifyTableState or newly created

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [ExecFindPartition](ExecFindPartition.md) (in execPartition.c:367)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md) (in execPartition.c:674)

## Notes and Other Information
This is a static helper function that handles the common routing setup tasks needed when a partition is first accessed. It optimizes memory usage by only creating partition-specific tuple slots when tuple format conversion is actually required. The function supports foreign table partitions by invoking appropriate FDW callbacks for initialization and batch size determination. The dynamic array management uses an exponential growth strategy (doubling) to efficiently handle workloads with varying numbers of accessed partitions. The function operates within the partition routing memory context to ensure proper memory lifecycle management.

## Simplified Source

```c
static void
ExecInitRoutingInfo(ModifyTableState *mtstate,
                    EState *estate,
                    PartitionTupleRouting *proute,
                    PartitionDispatch dispatch,
                    ResultRelInfo *partRelInfo,
                    int partidx,
                    bool is_borrowed_rel)
{
    MemoryContext oldcxt;
    int           rri_index;

    oldcxt = MemoryContextSwitchTo(proute->memcxt);

    // Set up tuple conversion if partition has different rowtype from root
    if (ExecGetRootToChildMap(partRelInfo, estate) != NULL) {
        Relation partrel = partRelInfo->ri_RelationDesc;
        partRelInfo->ri_PartitionTupleSlot =
            table_slot_create(partrel, &estate->es_tupleTable);
    } else {
        partRelInfo->ri_PartitionTupleSlot = NULL;
    }

    // Initialize FDW for foreign table partitions
    if (partRelInfo->ri_FdwRoutine != NULL &&
        partRelInfo->ri_FdwRoutine->BeginForeignInsert != NULL)
        partRelInfo->ri_FdwRoutine->BeginForeignInsert(mtstate, partRelInfo);

    // Determine batch size for FDW batch insertion
    if (partRelInfo->ri_FdwRoutine != NULL &&
        partRelInfo->ri_FdwRoutine->GetForeignModifyBatchSize &&
        partRelInfo->ri_FdwRoutine->ExecForeignBatchInsert)
        partRelInfo->ri_BatchSize =
            partRelInfo->ri_FdwRoutine->GetForeignModifyBatchSize(partRelInfo);
    else
        partRelInfo->ri_BatchSize = 1;

    partRelInfo->ri_CopyMultiInsertBuffer = NULL;

    // Add partition to routing arrays, growing them if needed
    rri_index = proute->num_partitions++;

    if (proute->num_partitions >= proute->max_partitions) {
        if (proute->max_partitions == 0) {
            proute->max_partitions = 8;
            proute->partitions = (ResultRelInfo **)
                palloc(sizeof(ResultRelInfo *) * proute->max_partitions);
            proute->is_borrowed_rel = (bool *)
                palloc(sizeof(bool) * proute->max_partitions);
        } else {
            proute->max_partitions *= 2;
            proute->partitions = (ResultRelInfo **)
                repalloc(proute->partitions, sizeof(ResultRelInfo *) * proute->max_partitions);
            proute->is_borrowed_rel = (bool *)
                repalloc(proute->is_borrowed_rel, sizeof(bool) * proute->max_partitions);
        }
    }

    // Store partition info and establish dispatch mapping
    proute->partitions[rri_index] = partRelInfo;
    proute->is_borrowed_rel[rri_index] = is_borrowed_rel;
    dispatch->indexes[partidx] = rri_index;

    MemoryContextSwitchTo(oldcxt);
}
```
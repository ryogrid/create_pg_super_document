# ExecInitPartitionDispatchInfo

## Location
[src/backend/executor/execPartition.c:1094-1232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1094-L1232)

## Overview
Locks a partitioned table and initializes PartitionDispatch structure for efficient partition key evaluation and tuple routing, managing the hierarchical dispatch system for multi-level partitioning.

## Definition

```c
static PartitionDispatch
ExecInitPartitionDispatchInfo(EState *estate,
							  PartitionTupleRouting *proute, Oid partoid,
							  PartitionDispatch parent_pd, int partidx,
							  ResultRelInfo *rootResultRelInfo)
```
## Detailed Description
This function creates and configures a PartitionDispatch structure for a partitioned table, which contains all the information needed to evaluate partition keys and route tuples to the correct child partitions. It handles both root-level partitioned tables and sub-partitioned tables within a partition hierarchy. For sub-partitioned tables, the function sets up tuple conversion infrastructure when the child table has a different column layout than its parent, ensuring correct partition key evaluation across different tuple formats.

The function manages the partition directory for tracking partition metadata and handles concurrency considerations by optionally excluding partitions being detached (except in snapshot-isolation mode). It also maintains dynamic arrays of PartitionDispatch structures and creates minimal ResultRelInfo structures for non-leaf partitions when needed for constraint checking.

## Parameters / Member Variables
- `*estate`: Executor state providing partition directory management and memory contexts
- `*proute`: PartitionTupleRouting structure where the new PartitionDispatch will be stored
- `partoid`: Object ID of the partitioned table to initialize dispatch information for
- `parent_pd`: Parent PartitionDispatch (NULL for root partitioned table) used to establish hierarchy links
- `partidx`: Index of this partition within the parent's partition list (unused for root table)
- `*rootResultRelInfo`: ResultRelInfo for the root table, used as template for creating sub-partition ResultRelInfo structures
## Dependencies
- Functions called/Symbols referenced:
  - [CreatePartitionDirectory](../C/CreatePartitionDirectory.md)
  - IsolationUsesXactSnapshot
  - [table_open](../t/table_open.md)
  - [PartitionDirectoryLookup](../P/PartitionDirectoryLookup.md)
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - makeNode
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
- Called from (representative examples):
  - [ExecSetupPartitionTupleRouting](ExecSetupPartitionTupleRouting.md) (in execPartition.c:236)
  - [ExecFindPartition](ExecFindPartition.md) (in execPartition.c:410)

## Notes and Other Information
This is a static function that handles the complex initialization of partition dispatch infrastructure. It implements sophisticated memory management with dynamically growing arrays using a doubling strategy for efficient scaling. The function handles tuple format conversion between parent and child partitioned tables when their tuple descriptors differ, which is essential for correct partition key evaluation in hierarchical partitioning schemes. The partition directory integration helps optimize partition metadata lookup and handles concurrency scenarios involving partition detachment operations.

## Simplified Source

```c
static PartitionDispatch
ExecInitPartitionDispatchInfo(EState *estate,
                              PartitionTupleRouting *proute, Oid partoid,
                              PartitionDispatch parent_pd, int partidx,
                              ResultRelInfo *rootResultRelInfo)
{
    Relation        rel;
    PartitionDesc   partdesc;
    PartitionDispatch pd;
    int             dispatchidx;
    MemoryContext   oldcxt;

    // Initialize partition directory if needed
    if (estate->es_partition_directory == NULL)
        estate->es_partition_directory =
            CreatePartitionDirectory(estate->es_query_cxt, !IsolationUsesXactSnapshot());

    oldcxt = MemoryContextSwitchTo(proute->memcxt);

    // Open the partitioned table (lock sub-partitions, root already locked)
    if (partoid != RelationGetRelid(proute->partition_root))
        rel = table_open(partoid, RowExclusiveLock);
    else
        rel = proute->partition_root;

    partdesc = PartitionDirectoryLookup(estate->es_partition_directory, rel);

    // Allocate and initialize PartitionDispatch structure
    pd = (PartitionDispatch) palloc(offsetof(PartitionDispatchData, indexes) +
                                    partdesc->nparts * sizeof(int));
    pd->reldesc = rel;
    pd->key = RelationGetPartitionKey(rel);
    pd->keystate = NIL;
    pd->partdesc = partdesc;

    // Set up tuple conversion for sub-partitioned tables
    if (parent_pd != NULL) {
        TupleDesc tupdesc = RelationGetDescr(rel);
        pd->tupmap = build_attrmap_by_name_if_req(RelationGetDescr(parent_pd->reldesc),
                                                  tupdesc, false);
        pd->tupslot = pd->tupmap ? MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual) : NULL;
    } else {
        pd->tupmap = NULL;
        pd->tupslot = NULL;
    }

    // Initialize partition indexes array
    memset(pd->indexes, -1, sizeof(int) * partdesc->nparts);

    // Grow dispatch arrays if needed
    dispatchidx = proute->num_dispatch++;
    if (proute->num_dispatch >= proute->max_dispatch) {
        if (proute->max_dispatch == 0) {
            proute->max_dispatch = 4;
            proute->partition_dispatch_info = (PartitionDispatch *)
                palloc(sizeof(PartitionDispatch) * proute->max_dispatch);
            proute->nonleaf_partitions = (ResultRelInfo **)
                palloc(sizeof(ResultRelInfo *) * proute->max_dispatch);
        } else {
            proute->max_dispatch *= 2;
            proute->partition_dispatch_info = (PartitionDispatch *)
                repalloc(proute->partition_dispatch_info,
                         sizeof(PartitionDispatch) * proute->max_dispatch);
            proute->nonleaf_partitions = (ResultRelInfo **)
                repalloc(proute->nonleaf_partitions,
                         sizeof(ResultRelInfo *) * proute->max_dispatch);
        }
    }
    proute->partition_dispatch_info[dispatchidx] = pd;

    // Create ResultRelInfo for non-leaf partitions
    if (parent_pd) {
        ResultRelInfo *rri = makeNode(ResultRelInfo);
        InitResultRelInfo(rri, rel, 0, rootResultRelInfo, 0);
        proute->nonleaf_partitions[dispatchidx] = rri;

        // Link parent to child for quick descent
        parent_pd->indexes[partidx] = dispatchidx;
    } else {
        proute->nonleaf_partitions[dispatchidx] = NULL;
    }

    MemoryContextSwitchTo(oldcxt);
    return pd;
}
```
# ExecGetRangeTableRelation

## Location
[src/backend/executor/execUtils.c:762-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L762-L813)

## Overview
Opens a Relation for a range table entry if not already opened, providing lazy initialization of table access during query execution.

## Definition

```c
Relation
ExecGetRangeTableRelation(EState *estate, Index rti)
```
## Detailed Description
This function implements lazy opening of relations referenced in the query's range table. It checks if the relation at the given range table index (rti) is already open in the execution state, and if not, opens it using the appropriate locking mechanism. The function handles both normal query execution and parallel worker scenarios differently - parallel workers must obtain their own local locks to ensure safe behavior if the parent process exits prematurely. All opened relations are stored in the execution state and will be automatically closed when the plan execution ends via ExecEndPlan().

## Parameters / Member Variables
- `*estate`: Execution state containing the range table and opened relations array
- `rti`: Range table index (1-based) identifying which relation to open
## Dependencies
- Functions called/Symbols referenced:
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [table_open](../t/table_open.md)
  - IsParallelWorker
  - [CheckRelationLockedByMe](../C/CheckRelationLockedByMe.md)
- Called from (representative examples):
  - [InitPlan](../I/InitPlan.md)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [ExecInitResultRelation](ExecInitResultRelation.md)

## Notes and Other Information
- Relations are opened with NoLock in normal execution (assumes appropriate lock already held)
- Parallel workers explicitly acquire the lock specified in rte->rellockmode
- Uses lazy initialization pattern - relations are only opened when first accessed
- Includes assertion checks to verify proper locking in non-parallel execution
- The function assumes the range table entry is of type RTE_RELATION

## Simplified Source

```c
Relation
ExecGetRangeTableRelation(EState *estate, Index rti)
{
    Relation rel;

    Assert(rti > 0 && rti <= estate->es_range_table_size);

    rel = estate->es_relations[rti - 1];
    if (rel == NULL)
    {
        // First time through, so open the relation
        RangeTblEntry *rte = exec_rt_fetch(rti, estate);
        Assert(rte->rtekind == RTE_RELATION);

        if (!IsParallelWorker())
        {
            // In normal query, we should already have the appropriate lock
            rel = table_open(rte->relid, NoLock);
            Assert(rte->rellockmode == AccessShareLock ||
                   CheckRelationLockedByMe(rel, rte->rellockmode, false));
        }
        else
        {
            // Parallel workers need their own local lock
            rel = table_open(rte->relid, rte->rellockmode);
        }

        estate->es_relations[rti - 1] = rel;
    }

    return rel;
}
```
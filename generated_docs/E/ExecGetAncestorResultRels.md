# ExecGetAncestorResultRels

## Location
[src/backend/executor/execMain.c:1371-1430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1371-L1430)

## Overview
Returns ResultRelInfo structures for all ancestor relations of a given leaf partition up to and including the query's root target relation, enabling operations that must traverse the partition hierarchy.

## Definition

```c
List *
ExecGetAncestorResultRels(EState *estate, ResultRelInfo *resultRelInfo)
```
## Detailed Description
ExecGetAncestorResultRels builds and caches a list of ResultRelInfo structures representing the complete ancestry chain of a partition relation. It uses get_partition_ancestors to obtain the OID list of ancestor relations, then creates corresponding ResultRelInfo structures for each ancestor up to (but not including) the root relation mentioned in the query. The root relation is added separately using the existing ri_RootResultRelInfo. This functionality is essential for operations like foreign key constraint checking that must propagate across partition boundaries.

## Parameters / Member Variables
- : The execution state containing instrumentation and context information  
- : The leaf partition's ResultRelInfo for which ancestors are needed

## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_ancestors](../g/get_partition_ancestors.md)
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - RelationGetRelid
  - [table_open](../t/table_open.md)
  - makeNode
  - [lappend](../l/lappend.md)
  - elog/elog
  - Assert
- Called from (representative examples):
  - [ExecCrossPartitionUpdateForeignKey](ExecCrossPartitionUpdateForeignKey.md) (nodeModifyTable.c:2212)

## Notes and Other Information
- Only works with partition relations; errors out if called on non-partitioned relations
- Results are cached in ri_ancestorResultRels to avoid repeated computation
- Assumes all ancestor relations are properly locked by planner or AcquireExecutorLocks
- Opens ancestor relations with NoLock since locks should already be held
- Stops climbing the hierarchy when reaching the query's root target relation
- Always includes the root relation as the final element in the returned list
- Essential for maintaining referential integrity across partition boundaries
- Used by foreign key enforcement during cross-partition updates
- Closed automatically by ExecCloseResultRelations during cleanup

## Simplified Source

```c
List *
ExecGetAncestorResultRels(EState *estate, ResultRelInfo *resultRelInfo)
{
    ResultRelInfo *rootRelInfo = resultRelInfo->ri_RootResultRelInfo;
    Relation partRel = resultRelInfo->ri_RelationDesc;
    Oid rootRelOid;

    // Validate input - must be a partition
    if (!partRel->rd_rel->relispartition)
        elog(ERROR, "cannot find ancestors of a non-partition result relation");

    Assert(rootRelInfo != NULL);
    rootRelOid = RelationGetRelid(rootRelInfo->ri_RelationDesc);

    // Build ancestor list if not already cached
    if (resultRelInfo->ri_ancestorResultRels == NIL)
    {
        List *oids = get_partition_ancestors(RelationGetRelid(partRel));
        List *ancResultRels = NIL;
        ListCell *lc;

        // Create ResultRelInfo for each ancestor (except root)
        foreach(lc, oids)
        {
            Oid ancOid = lfirst_oid(lc);
            Relation ancRel;
            ResultRelInfo *rInfo;

            // Stop when we reach the root relation
            if (ancOid == rootRelOid)
                break;

            // Open ancestor relation and create ResultRelInfo
            ancRel = table_open(ancOid, NoLock);
            rInfo = makeNode(ResultRelInfo);
            InitResultRelInfo(rInfo, ancRel, 0, NULL, estate->es_instrument);
            ancResultRels = lappend(ancResultRels, rInfo);
        }

        // Add root relation at the end
        ancResultRels = lappend(ancResultRels, rootRelInfo);
        resultRelInfo->ri_ancestorResultRels = ancResultRels;
    }

    Assert(resultRelInfo->ri_ancestorResultRels != NIL);
    return resultRelInfo->ri_ancestorResultRels;
}
```
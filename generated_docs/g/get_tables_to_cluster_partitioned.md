# get_tables_to_cluster_partitioned

## Location
[src/backend/commands/cluster.c:1690-1737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L1690-L1737)

## Overview
Returns a list of RelToCluster structures for all leaf table/index pairs that should be clustered, given an index on a partitioned table.

## Definition
static List *get_tables_to_cluster_partitioned(MemoryContext cluster_context, Oid indexOid)

## Detailed Description
This function handles clustering operations on partitioned tables by expanding a partitioned index into all of its leaf partition indexes and their corresponding tables. It starts with a given index OID on a partitioned table and uses find_all_inheritors() to discover all child indexes. For each child index found, it verifies that it's actually a leaf index (not another partitioned index) and that the current user has clustering privileges on the corresponding table.

The function is similar to expand_vacuum_rel but is specifically designed for clustering operations. The caller must already hold AccessExclusiveLock on the table containing the index. The function skips any partitions where the user lacks CLUSTER privileges, allowing partial clustering when permissions are limited.

## Parameters / Member Variables
- : Memory context in which to allocate the result list and RelToCluster structures
- : OID of the partitioned index to expand into leaf indexes

## Dependencies
- Functions called/Symbols referenced:
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [get_rel_relkind](get_rel_relkind.md)
  - [cluster_is_permitted_for_relation](../c/cluster_is_permitted_for_relation.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [cluster](../c/cluster.md)

## Notes and Other Information
- This is a static function internal to cluster.c
- Designed for partitioned table clustering operations
- Uses NoLock when calling find_all_inheritors since caller already holds AccessExclusiveLock
- Filters out non-leaf indexes using get_rel_relkind() check for RELKIND_INDEX
- Handles permission checking per partition, allowing partial clustering
- Returns NIL if no leaf partitions are found or user lacks privileges on all partitions
- Memory allocation is done in the specified cluster_context for proper cleanup

## Simplified Source

```c
static List *get_tables_to_cluster_partitioned(MemoryContext cluster_context, Oid indexOid)
{
    List *inhoids;
    ListCell *lc;
    List *rtcs = NIL;
    MemoryContext old_context;

    // Find all inheriting indexes (including partitioned index itself)
    inhoids = find_all_inheritors(indexOid, NoLock, NULL);

    // Process each child index
    foreach(lc, inhoids) {
        Oid indexrelid = lfirst_oid(lc);
        Oid relid = IndexGetRelation(indexrelid, false);
        RelToCluster *rtc;

        // Only consider leaf indexes (not partitioned indexes)
        if (get_rel_relkind(indexrelid) != RELKIND_INDEX)
            continue;

        // Check if user has clustering privileges on this partition
        if (!cluster_is_permitted_for_relation(relid, GetUserId()))
            continue;

        // Create RelToCluster entry in specified memory context
        old_context = MemoryContextSwitchTo(cluster_context);
        rtc = (RelToCluster *) palloc(sizeof(RelToCluster));
        rtc->tableOid = relid;
        rtc->indexOid = indexrelid;
        rtcs = lappend(rtcs, rtc);
        MemoryContextSwitchTo(old_context);
    }

    return rtcs;
}
```
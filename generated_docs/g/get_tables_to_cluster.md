# get_tables_to_cluster

## Location
[src/backend/commands/cluster.c:1636-1689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L1636-L1689)

## Overview
Returns a list of tables that the current user has privileges on and have the indisclustered flag set, identifying tables that are ready for clustering operations.

## Definition
static List *get_tables_to_cluster(MemoryContext cluster_context)

## Detailed Description
This function scans the pg_index system catalog to find all indexes that have the indisclustered flag set to true, indicating that the associated table should be clustered using that index. For each such index found, the function verifies that the current user has appropriate privileges on the underlying table before including it in the result list. The function creates RelToCluster structures containing both the table OID and the index OID for each qualifying table-index pair.

The function performs a catalog scan on the IndexRelationId relation, filtering for entries where indisclustered is true. It then checks permissions using cluster_is_permitted_for_relation() before adding entries to the result list. All result structures are allocated in the specified memory context to ensure proper memory management.

## Parameters / Member Variables
- : Memory context in which to allocate the result list and RelToCluster structures

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [cluster_is_permitted_for_relation](../c/cluster_is_permitted_for_relation.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [lappend](../l/lappend.md)
  - [table_endscan](../t/table_endscan.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [cluster](../c/cluster.md)

## Notes and Other Information
- This is a static function internal to cluster.c
- Uses AccessShareLock when opening the index relation
- Scans the pg_index catalog with indisclustered = true filter
- Returns NIL if no clusterable tables are found or user lacks privileges
- Memory allocation is done in the specified cluster_context for proper cleanup
- Each RelToCluster structure contains both tableOid and indexOid for the clustering operation

## Simplified Source

```c
static List *get_tables_to_cluster(MemoryContext cluster_context)
{
    Relation indRelation;
    TableScanDesc scan;
    ScanKeyData entry;
    HeapTuple indexTuple;
    Form_pg_index index;
    MemoryContext old_context;
    List *rtcs = NIL;

    // Open pg_index catalog and scan for clustered indexes
    indRelation = table_open(IndexRelationId, AccessShareLock);
    ScanKeyInit(&entry,
                Anum_pg_index_indisclustered,
                BTEqualStrategyNumber, F_BOOLEQ,
                BoolGetDatum(true));
    scan = table_beginscan_catalog(indRelation, 1, &entry);

    // Process each clustered index
    while ((indexTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        RelToCluster *rtc;
        index = (Form_pg_index) GETSTRUCT(indexTuple);

        // Check if user has clustering privileges on the table
        if (!cluster_is_permitted_for_relation(index->indrelid, GetUserId()))
            continue;

        // Create RelToCluster entry in specified memory context
        old_context = MemoryContextSwitchTo(cluster_context);
        rtc = (RelToCluster *) palloc(sizeof(RelToCluster));
        rtc->tableOid = index->indrelid;
        rtc->indexOid = index->indexrelid;
        rtcs = lappend(rtcs, rtc);
        MemoryContextSwitchTo(old_context);
    }

    table_endscan(scan);
    relation_close(indRelation, AccessShareLock);
    return rtcs;
}
```
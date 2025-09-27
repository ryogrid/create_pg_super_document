# RelationGetIndexList

## Location
[src/backend/utils/cache/relcache.c:4806-4926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4806-L4926)

## Overview
RelationGetIndexList returns a list of index OIDs associated with a relation, along with identifying primary key and replica identity indexes.

## Definition
```c
List *RelationGetIndexList(Relation relation)
```

## Detailed Description
RelationGetIndexList retrieves all indexes associated with a given relation by scanning pg_index for entries where indrelid matches the target relation. The function implements comprehensive caching - if the index list has already been computed and cached (rd_indexvalid is true), it returns a copy of the cached list.

During the scan, the function filters out indexes that are not live (indislive is false), as these are being dropped and should not be touched. All valid index OIDs are collected into a result list, which is then sorted by OID to ensure consistent ordering across all backends. This ordering is crucial for avoiding deadlocks when the executor needs to acquire exclusive locks on multiple indexes.

The function also identifies special indexes during the scan:
- Primary key index (rd_pkindex): Set when indisprimary is true
- Replica identity index (rd_replidindex): Determined based on the relation's replica identity setting (relreplident) and available suitable indexes

The function carefully manages memory contexts to prevent leaks, building the result list in the caller's context while caching a copy in CacheMemoryContext. The returned list is always a copy to protect against cache invalidation during subsequent operations.

## Parameters / Member Variables
- `relation`: The Relation structure for which index information should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [list_sort](../l/list_sort.md)
  - [list_oid_cmp](../l/list_oid_cmp.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [list_free](../l/list_free.md)
  - Form_pg_index (struct type)
- Called from (representative examples):
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [get_relation_info](../g/get_relation_info.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [cluster](../c/cluster.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- Implements caching via rd_indexlist and rd_indexvalid fields in the relation structure
- Returns index OIDs sorted by OID to prevent deadlocks during concurrent index locking
- Filters out indexes that are not live (being dropped) for safety
- Identifies and caches primary key index (rd_pkindex) and replica identity index (rd_replidindex)
- Returns a copy of the list to protect against cache invalidation during syscache lookups
- Updates replica identity index based on relation's relreplident setting and available indexes
- Memory management prevents leaks by using appropriate memory contexts for building and caching

## Simplified Source

```c
// Simplified version of RelationGetIndexList
List *RelationGetIndexList(Relation relation) {
    // Quick exit if index list already cached
    if (relation->rd_indexvalid) {
        return list_copy(relation->rd_indexlist);
    }

    // Initialize variables for scan and result collection
    List *result = NIL;
    Oid pkeyIndex = InvalidOid;
    Oid candidateIndex = InvalidOid;
    char replident = relation->rd_rel->relreplident;

    // Set up scan key to find all indexes for this relation
    ScanKeyData skey;
    ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    // Open pg_index catalog and begin scan
    Relation indrel = table_open(IndexRelationId, AccessShareLock);
    SysScanDesc indscan = systable_beginscan(indrel, IndexIndrelidIndexId,
                                             true, NULL, 1, &skey);

    // Process each index entry found
    HeapTuple htup;
    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

        // Skip indexes being dropped (not live)
        if (!index->indislive) {
            continue;
        }

        // Add this index OID to result list
        result = lappend_oid(result, index->indexrelid);

        // Check for special index types (primary key, replica identity)
        if (index->indisvalid && index->indisunique &&
            index->indimmediate &&
            heap_attisnull(htup, Anum_pg_index_indpred, NULL)) {

            if (index->indisprimary) {
                pkeyIndex = index->indexrelid;
            }
            if (index->indisreplident) {
                candidateIndex = index->indexrelid;
            }
        }
    }

    // Clean up scan
    systable_endscan(indscan);
    table_close(indrel, AccessShareLock);

    // Sort result by OID for consistent ordering (prevents deadlocks)
    list_sort(result, list_oid_cmp);

    // Cache the results in relation structure
    MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
    List *oldlist = relation->rd_indexlist;
    relation->rd_indexlist = list_copy(result);
    relation->rd_pkindex = pkeyIndex;

    // Set replica identity index based on relation settings
    if (replident == REPLICA_IDENTITY_DEFAULT && OidIsValid(pkeyIndex)) {
        relation->rd_replidindex = pkeyIndex;
    } else if (replident == REPLICA_IDENTITY_INDEX && OidIsValid(candidateIndex)) {
        relation->rd_replidindex = candidateIndex;
    } else {
        relation->rd_replidindex = InvalidOid;
    }

    relation->rd_indexvalid = true;
    MemoryContextSwitchTo(oldcxt);

    // Clean up old cached list
    list_free(oldlist);

    return result;
}
```

Key simplifications made:
- Consolidated variable declarations at the top for clarity
- Simplified the complex conditional logic for special index detection
- Removed detailed comments about memory leakage and HOT-safety
- Streamlined the replica identity index assignment logic
- Focused on the main execution path while preserving all essential functionality
- Maintained the critical caching behavior and memory management
- Preserved the important OID sorting for deadlock prevention
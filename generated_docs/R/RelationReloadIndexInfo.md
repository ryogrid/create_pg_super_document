# RelationReloadIndexInfo

## Location
[src/backend/utils/cache/relcache.c:2257-2370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2257-L2370)

## Overview
Reloads minimal information for an invalidated index relation without performing a complete cache rebuild, supporting only specific schema changes allowed for existing indexes.

## Definition
```c
static void RelationReloadIndexInfo(Relation relation)
```

## Detailed Description
RelationReloadIndexInfo is a specialized function designed to handle relcache invalidation events for index relations. When an index receives an invalidation signal (typically due to changes in pg_class or pg_index), this function selectively updates the cached information without performing a costly complete rebuild.

The function supports two main categories of updates:
1. **pg_class updates**: Complete replacement of the pg_class row data, including reloption parsing and physical address recalculation
2. **pg_index boolean field updates**: Selective copying of boolean fields that are allowed to change (like indisvalid, indisready, etc.)

Key design considerations:
- Cannot perform complete rebuilds for "nailed" indexes or those in active use
- Handles failed transaction scenarios where catalog reads might not be immediately possible
- Includes special handling for shared indexes during backend startup
- Avoids deadlock risks when updating system catalog indexes

## Parameters / Member Variables
- `relation`: Pointer to the invalidated index Relation structure that needs reloading

## Dependencies
- Functions called/Symbols referenced:
  - [RelationCloseSmgr](RelationCloseSmgr.md)
  - [ScanPgRelation](../S/ScanPgRelation.md)
  - [RelationParseRelOptions](RelationParseRelOptions.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - HeapTupleHeaderSetXmin
- Called from (representative examples):
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - [RelationReloadNailed](RelationReloadNailed.md)
  - [RelationClearRelation](RelationClearRelation.md)

## Notes and Other Information
- This is a static function, only callable from within relcache.c
- Requires AccessShareLock on the target index at call time
- For system catalog indexes, special deadlock prevention measures are needed
- Only updates boolean fields from pg_index; array fields are immutable for existing indexes
- Shared indexes receive minimal handling during backend startup when critical relcaches aren't built yet
- The function preserves expensive-to-rebuild information like support function lookup data
- AM (Access Method) cached data is cleared to ensure consistency after reload
- Includes assertion checks to ensure it's only called on appropriate index types in invalid state

## Simplified Source

```c
static void
RelationReloadIndexInfo(Relation relation)
{
    bool indexOK;
    HeapTuple pg_class_tuple;
    Form_pg_class relp;

    // Verify this is an invalidated index
    Assert((relation->rd_rel->relkind == RELKIND_INDEX ||
            relation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX) &&
           !relation->rd_isvalid &&
           relation->rd_droppedSubid == InvalidSubTransactionId);

    // Clean up storage and cached data
    RelationCloseSmgr(relation);
    if (relation->rd_amcache)
        pfree(relation->rd_amcache);
    relation->rd_amcache = NULL;

    // Special case: shared indexes during startup
    if (relation->rd_rel->relisshared && !criticalRelcachesBuilt)
    {
        relation->rd_isvalid = true;
        return;
    }

    // Read and update pg_class information
    indexOK = (RelationGetRelid(relation) != ClassOidIndexId);
    pg_class_tuple = ScanPgRelation(RelationGetRelid(relation), indexOK, false);
    if (!HeapTupleIsValid(pg_class_tuple))
        elog(ERROR, "could not find pg_class tuple for index %u",
             RelationGetRelid(relation));

    relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);
    memcpy(relation->rd_rel, relp, CLASS_TUPLE_SIZE);

    // Reload options and recalculate physical address
    if (relation->rd_options)
        pfree(relation->rd_options);
    RelationParseRelOptions(relation, pg_class_tuple);
    heap_freetuple(pg_class_tuple);
    RelationInitPhysicalAddr(relation);

    // Update pg_index boolean fields for non-system indexes
    if (!IsSystemRelation(relation))
    {
        HeapTuple tuple;
        Form_pg_index index;

        tuple = SearchSysCache1(INDEXRELID,
                               ObjectIdGetDatum(RelationGetRelid(relation)));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for index %u",
                 RelationGetRelid(relation));

        index = (Form_pg_index) GETSTRUCT(tuple);

        // Copy all boolean fields from pg_index
        relation->rd_index->indisunique = index->indisunique;
        relation->rd_index->indnullsnotdistinct = index->indnullsnotdistinct;
        relation->rd_index->indisprimary = index->indisprimary;
        relation->rd_index->indisexclusion = index->indisexclusion;
        relation->rd_index->indimmediate = index->indimmediate;
        relation->rd_index->indisclustered = index->indisclustered;
        relation->rd_index->indisvalid = index->indisvalid;
        relation->rd_index->indcheckxmin = index->indcheckxmin;
        relation->rd_index->indisready = index->indisready;
        relation->rd_index->indislive = index->indislive;
        relation->rd_index->indisreplident = index->indisreplident;

        // Copy xmin for indcheckxmin handling
        HeapTupleHeaderSetXmin(relation->rd_indextuple->t_data,
                              HeapTupleHeaderGetXmin(tuple->t_data));

        ReleaseSysCache(tuple);
    }

    // Mark as valid
    relation->rd_isvalid = true;
}
```
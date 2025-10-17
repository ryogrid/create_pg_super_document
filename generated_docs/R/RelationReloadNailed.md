# RelationReloadNailed

## Location
[src/backend/utils/cache/relcache.c:2371-2442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2371-L2442)

## Overview
RelationReloadNailed reloads minimal information for nailed relations after invalidations, ensuring critical catalog data like pg_class.relfrozenxid remains accurate while preserving the unchangeable structural information.

## Definition
```c
static void RelationReloadNailed(Relation relation)
```

## Detailed Description
RelationReloadNailed is a specialized function for handling invalidations of nailed relations in PostgreSQL's relation cache. Nailed relations are system catalogs that are permanently kept in the relation cache and have unchangeable structures. However, some of their metadata (like relfrozenxid) still needs periodic updates when invalidations arrive.

The function performs selective reloading by:
1. Re-initializing physical addressing for mapped relations whose mappings may have changed
2. Marking the relation as invalid to trigger revalidation
3. For actively used relations in a valid transaction state, immediately reloading catalog content
4. Handling index relations differently from regular relations using specialized reload logic

The function is transaction-aware and will defer reloading if not in a transaction state or if the relation isn't actively referenced beyond its nailed status.

## Parameters / Member Variables
- `relation`: The nailed Relation structure to reload. Must have rd_isnailed set to true.
## Dependencies
- Functions called/Symbols referenced:
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [RelationReloadIndexInfo](RelationReloadIndexInfo.md)
  - [ScanPgRelation](../S/ScanPgRelation.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - RELKIND_INDEX
  - Form_pg_class
  - CLASS_TUPLE_SIZE
- Called from (representative examples):
  - [RelationClearRelation](RelationClearRelation.md)

## Notes and Other Information
- Only operates on nailed relations (rd_isnailed must be true)
- Preserves structural invariants while updating mutable catalog data
- Uses different reload strategies for index vs non-index relations
- Includes self-recursion protection when rebuilding pg_class
- Defers reloading when not in transaction state or relation not actively used
- Part of PostgreSQL's relation cache invalidation and consistency mechanism

## Simplified Source

```c
static void RelationReloadNailed(Relation relation) {
    Assert(relation->rd_isnailed);

    // Reinitialize physical addressing for mapped relations
    RelationInitPhysicalAddr(relation);

    // Mark as needing revalidation
    relation->rd_isvalid = false;

    // Only reload if in transaction and relation is actively used
    if (!IsTransactionState() || relation->rd_refcnt <= 1)
        return;

    if (relation->rd_rel->relkind == RELKIND_INDEX) {
        // For nailed indexes, reload index-specific information
        RelationReloadIndexInfo(relation);
    } else {
        // For non-index relations, reload pg_class data if relcaches are built
        if (criticalRelcachesBuilt) {
            // Mark valid before scan to avoid self-recursion
            relation->rd_isvalid = true;

            // Fetch fresh pg_class tuple
            HeapTuple pg_class_tuple = ScanPgRelation(RelationGetRelid(relation),
                                                     true, false);
            Form_pg_class relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);

            // Update pg_class data in relation descriptor
            memcpy(relation->rd_rel, relp, CLASS_TUPLE_SIZE);
            heap_freetuple(pg_class_tuple);

            // Mark valid again to protect against concurrent invalidations
            relation->rd_isvalid = true;
        }
    }
}
```
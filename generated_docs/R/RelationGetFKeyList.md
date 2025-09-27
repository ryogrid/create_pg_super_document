# RelationGetFKeyList

## Location
[src/backend/utils/cache/relcache.c:4697-4805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4697-L4805)

## Overview
RelationGetFKeyList returns a list of foreign key constraints for a given relation by scanning pg_constraint and building ForeignKeyCacheInfo structures.

## Definition
```c
List *RelationGetFKeyList(Relation relation)
```

## Detailed Description
RelationGetFKeyList retrieves all foreign key constraints that reference the given relation as the referencing table. The function implements a caching mechanism - if the foreign key list has already been computed and cached (rd_fkeyvalid is true), it returns the cached list immediately.

For performance optimization, the function includes a fast path that returns NIL immediately for tables that cannot have foreign keys (non-partitioned tables without triggers).

When building the foreign key list, the function scans pg_constraint for CONSTRAINT_FOREIGN entries where conrelid matches the target relation. For each foreign key found, it creates a ForeignKeyCacheInfo structure containing the constraint OID, referencing relation OID, referenced relation OID, and detailed constraint information extracted via DeconstructFkConstraintRow.

The function carefully manages memory contexts to avoid leaks: it builds the result list in the caller's context, then copies it to CacheMemoryContext for caching. The cached list persists until the relcache entry is reset.

## Parameters / Member Variables
- `relation`: The Relation structure for which foreign key constraints should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - makeNode
  - [DeconstructFkConstraintRow](../D/DeconstructFkConstraintRow.md)
  - [lappend](../l/lappend.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - copyObject
  - [list_free_deep](../l/list_free_deep.md)
  - [ForeignKeyCacheInfo](../F/ForeignKeyCacheInfo.md) (struct type)
- Called from (representative examples):
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
  - [CloneFkReferencing](../C/CloneFkReferencing.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [get_relation_foreign_keys](../g/get_relation_foreign_keys.md)

## Notes and Other Information
- Implements caching via rd_fkeylist and rd_fkeyvalid fields in the relation structure
- Returns data directly from relcache - callers must be careful about cache invalidation
- Fast path optimization for tables that cannot have foreign keys
- Uses DeconstructFkConstraintRow to extract detailed constraint information from pg_constraint tuples
- Memory management prevents leaks by building lists in appropriate contexts
- [List](../L/List.md) items are returned in no particular order
- Callers should use copyObject() if they need to retain the list across potential cache flushes

## Simplified Source

```c
// Simplified version of RelationGetFKeyList
List *RelationGetFKeyList(Relation relation) {
    List *result;
    Relation constraint_relation;
    SysScanDesc scan;
    ScanKeyData scan_key;
    HeapTuple tuple;

    // Quick exit: return cached list if already computed
    if (relation->rd_fkeyvalid) {
        return relation->rd_fkeylist;
    }

    // Fast path: tables without triggers can't have foreign keys
    if (!relation->rd_rel->relhastriggers &&
        relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
        return NIL;
    }

    // Initialize result list
    result = NIL;

    // Set up scan key to find constraints for this relation
    ScanKeyInit(&scan_key, Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(relation)));

    // Open pg_constraint table and start scan
    constraint_relation = table_open(ConstraintRelationId, AccessShareLock);
    scan = systable_beginscan(constraint_relation, ConstraintRelidTypidNameIndexId,
                              true, NULL, 1, &scan_key);

    // Scan through all constraint tuples for this relation
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(tuple);

        // Only process foreign key constraints
        if (constraint->contype != CONSTRAINT_FOREIGN) {
            continue;
        }

        // Create foreign key info structure
        ForeignKeyCacheInfo *fk_info = makeNode(ForeignKeyCacheInfo);
        fk_info->conoid = constraint->oid;
        fk_info->conrelid = constraint->conrelid;
        fk_info->confrelid = constraint->confrelid;

        // Extract detailed constraint information
        DeconstructFkConstraintRow(tuple, &fk_info->nkeys,
                                   fk_info->conkey, fk_info->confkey,
                                   fk_info->conpfeqop,
                                   NULL, NULL, NULL, NULL);

        // Add to result list
        result = lappend(result, fk_info);
    }

    // Clean up scan
    systable_endscan(scan);
    table_close(constraint_relation, AccessShareLock);

    // Cache the result in the relation structure
    MemoryContext old_context = MemoryContextSwitchTo(CacheMemoryContext);
    List *old_list = relation->rd_fkeylist;
    relation->rd_fkeylist = copyObject(result);
    relation->rd_fkeyvalid = true;
    MemoryContextSwitchTo(old_context);

    // Clean up old cached list
    list_free_deep(old_list);

    return result;
}
```

Key simplifications made:
- Used more descriptive variable names (constraint_relation, scan, fk_info)
- Added clearer comments explaining each major step
- Consolidated variable declarations with their usage where appropriate
- Simplified the memory context switching logic while preserving functionality
- Maintained the essential caching mechanism and scan logic
- Preserved all critical operations: caching check, fast path, constraint scanning, and result building
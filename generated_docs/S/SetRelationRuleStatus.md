# SetRelationRuleStatus

## Location
[src/backend/rewrite/rewriteSupport.c:53-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSupport.c#L53-L91)

## Overview
Updates the relhasrules field in the pg_class system catalog to reflect whether a relation has any rewrite rules, and ensures proper cache invalidation across all backends.

## Definition
```c
void SetRelationRuleStatus(Oid relationId, bool relHasRules)
```

## Detailed Description
SetRelationRuleStatus is a critical function in PostgreSQL's rewrite rule management system that maintains the consistency of the relhasrules field in pg_class. This field serves as a quick indicator of whether a relation has any associated rewrite rules, allowing the system to optimize query processing by avoiding unnecessary rule checks for relations without rules.

The function performs two important operations: it updates the pg_class catalog entry if the status has changed, and it ensures that all backends receive a cache invalidation message regardless of whether the tuple was actually modified. This cache invalidation is essential because it forces all backends to refresh their relation cache entries with the current rule information.

The function requires the caller to hold an appropriate lock on the relation being modified, ensuring transactional consistency during the update process.

## Parameters / Member Variables
- `relationId`: The OID of the relation whose rule status is being updated
- `relHasRules`: Boolean flag indicating whether the relation has rewrite rules (true) or not (false)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcacheByTuple](../C/CacheInvalidateRelcacheByTuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Types used:
  - Form_pg_class
  - [Relation](../R/Relation.md)
  - HeapTuple
- Called from (representative examples):
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md)

## Notes and Other Information
- The caller must hold an appropriate lock on the relation before calling this function
- Cache invalidation occurs regardless of whether the tuple is actually modified, ensuring consistency across all backends
- Uses RowExclusiveLock when accessing pg_class to prevent concurrent modifications
- The function handles the case where no actual change is needed but still forces a relcache rebuild
- Critical for maintaining the integrity of PostgreSQL's rewrite rule system
- Located in src/backend/rewrite/rewriteSupport.c:53-91

## Simplified Source

```c
void SetRelationRuleStatus(Oid relationId, bool relHasRules) {
    Relation relationRelation;
    HeapTuple tuple;
    Form_pg_class classForm;

    // Open pg_class catalog and find the relation tuple
    relationRelation = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relationId));

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relationId);

    classForm = (Form_pg_class) GETSTRUCT(tuple);

    // Update the relhasrules field if it has changed
    if (classForm->relhasrules != relHasRules) {
        classForm->relhasrules = relHasRules;
        CatalogTupleUpdate(relationRelation, &tuple->t_self, tuple);
    } else {
        // Force relcache rebuild even if no change needed
        CacheInvalidateRelcacheByTuple(tuple);
    }

    // Clean up
    heap_freetuple(tuple);
    table_close(relationRelation, RowExclusiveLock);
}
```
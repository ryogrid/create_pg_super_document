# index_constraint_create

## Location
[src/backend/catalog/index.c:1881-2113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1881-L2113)

## Overview
Creates and configures a constraint (PRIMARY KEY, UNIQUE, or EXCLUSION) associated with an index, handling all necessary catalog entries, dependencies, and trigger creation.

## Definition
ObjectAddress index_constraint_create(Relation heapRelation, Oid indexRelationId, Oid parentConstraintId, const IndexInfo *indexInfo, const char *constraintName, char constraintType, bits16 constr_flags, bool allow_system_table_mods, bool is_internal)

## Detailed Description
This comprehensive function establishes a constraint backed by an index, creating all necessary catalog entries and relationships. It handles the complex process of linking an index to its constraint definition, managing dependencies, and setting up deferrable constraint mechanisms when needed.

The function performs several critical operations:
1. Validates constraint parameters and system table restrictions
2. Removes old dependencies when converting an existing index to a constraint
3. Creates a pg_constraint entry with appropriate inheritance and locality settings
4. Establishes dependency relationships between the constraint, index, and table
5. Creates deferred uniqueness checking triggers for deferrable constraints
6. Updates pg_index flags for primary key status and deferability
7. Handles partition constraint relationships with parent tables

The function supports various constraint types (PRIMARY KEY, UNIQUE, EXCLUSION) and can handle both regular and partitioned table scenarios. It ensures proper isolation and consistency through appropriate locking mechanisms.

## Parameters / Member Variables
- : The table relation owning the index (must be appropriately locked)
- : Object identifier of the backing index
- : OID of parent constraint for partitioned tables, or InvalidOid
- : Index metadata used by the executor for insertions
- : Name of the constraint (typically matches index name)  
- : Type of constraint (CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, or CONSTRAINT_EXCLUSION)
- : Bitmask controlling various creation options (deferrable, initially deferred, mark as primary, etc.)
- : Whether to allow constraints on system catalogs
- : Whether this constraint is created by an internal process

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNamespace
  - IsBootstrapProcessingMode
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - IsNormalProcessingMode
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [CreateTrigger](../C/CreateTrigger.md)
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [index_create](index_create.md)
  - [ATExecAddIndexConstraint](../A/ATExecAddIndexConstraint.md)

## Notes and Other Information
- Cannot be used during bootstrap processing mode due to constraint creation limitations
- Primary and unique constraints cannot have index expressions, only exclusion constraints can
- For deferrable constraints, creates a trigger using unique_key_recheck function for deferred validation
- When marking an existing index as primary, forces relcache invalidation to notify all sessions
- Handles partition inheritance by creating appropriate DEPENDENCY_PARTITION_PRI and DEPENDENCY_PARTITION_SEC relationships
- The INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS flag is used when converting pre-existing indexes to constraints
- Requires table-level locking to prevent concurrent modifications when updating index metadata
- Returns the ObjectAddress of the newly created constraint for dependency tracking

## Simplified Source

```c
ObjectAddress index_constraint_create(Relation heapRelation, Oid indexRelationId,
                                     Oid parentConstraintId, const IndexInfo *indexInfo,
                                     const char *constraintName, char constraintType,
                                     bits16 constr_flags, bool allow_system_table_mods,
                                     bool is_internal)
{
    Oid namespaceId = RelationGetNamespace(heapRelation);
    ObjectAddress myself, idxaddr;
    Oid conOid;
    bool deferrable, initdeferred, mark_as_primary;
    bool islocal, noinherit;
    int inhcount;

    // Extract flags
    deferrable = (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) != 0;
    initdeferred = (constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED) != 0;
    mark_as_primary = (constr_flags & INDEX_CONSTR_CREATE_MARK_AS_PRIMARY) != 0;

    // Validate constraints and system restrictions
    Assert(!IsBootstrapProcessingMode());
    if (!allow_system_table_mods && IsSystemRelation(heapRelation) && IsNormalProcessingMode())
        ereport(ERROR, "user-defined indexes on system catalog tables are not supported");

    if (indexInfo->ii_Expressions && constraintType != CONSTRAINT_EXCLUSION)
        elog(ERROR, "constraints cannot have index expressions");

    // Remove old dependencies if converting existing index
    if (constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS)
        deleteDependencyRecordsForClass(RelationRelationId, indexRelationId,
                                       RelationRelationId, DEPENDENCY_AUTO);

    // Set inheritance properties
    if (OidIsValid(parentConstraintId))
    {
        islocal = false;
        inhcount = 1;
        noinherit = false;
    }
    else
    {
        islocal = true;
        inhcount = 0;
        noinherit = true;
    }

    // Create constraint entry in pg_constraint
    conOid = CreateConstraintEntry(constraintName, namespaceId, constraintType,
                                  deferrable, initdeferred, true, parentConstraintId,
                                  RelationGetRelid(heapRelation),
                                  indexInfo->ii_IndexAttrNumbers,
                                  indexInfo->ii_NumIndexKeyAttrs,
                                  indexInfo->ii_NumIndexAttrs,
                                  InvalidOid, indexRelationId, InvalidOid,
                                  /* ... many other parameters ... */
                                  islocal, inhcount, noinherit, is_internal);

    // Register dependencies between constraint and index
    ObjectAddressSet(myself, ConstraintRelationId, conOid);
    ObjectAddressSet(idxaddr, RelationRelationId, indexRelationId);
    recordDependencyOn(&idxaddr, &myself, DEPENDENCY_INTERNAL);

    // Handle partition constraints
    if (OidIsValid(parentConstraintId))
    {
        ObjectAddress referenced;
        ObjectAddressSet(referenced, ConstraintRelationId, parentConstraintId);
        recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_PRI);
        ObjectAddressSet(referenced, RelationRelationId, RelationGetRelid(heapRelation));
        recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_SEC);
    }

    // Create deferred uniqueness trigger if needed
    if (deferrable)
    {
        CreateTrigStmt *trigger = makeNode(CreateTrigStmt);
        trigger->isconstraint = true;
        trigger->trigname = (constraintType == CONSTRAINT_PRIMARY) ?
            "PK_ConstraintTrigger" : "Unique_ConstraintTrigger";
        trigger->funcname = SystemFuncName("unique_key_recheck");
        trigger->row = true;
        trigger->timing = TRIGGER_TYPE_AFTER;
        trigger->events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE;
        trigger->deferrable = true;
        trigger->initdeferred = initdeferred;

        (void) CreateTrigger(trigger, NULL, RelationGetRelid(heapRelation),
                           InvalidOid, conOid, indexRelationId, InvalidOid,
                           InvalidOid, NULL, true, false);
    }

    // Update pg_index flags if needed
    if ((constr_flags & INDEX_CONSTR_CREATE_UPDATE_INDEX) && (mark_as_primary || deferrable))
    {
        // Update indisprimary and indimmediate flags in pg_index
        // ... catalog update logic ...
    }

    return myself;
}
```
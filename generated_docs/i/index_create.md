# index_create

## Location
[src/backend/catalog/index.c:724-1297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L724-L1297)

## Overview
index_create is the primary function for creating a new index relation in PostgreSQL, handling all aspects from relation creation to catalog entries and dependency management.

## Definition

```c
enumber map if and only if the parent does;
```
## Detailed Description
This function performs the complete process of creating a new index relation. It validates parameters, creates the physical relation structure, registers catalog entries in pg_class, pg_index, and pg_attribute, handles inheritance relationships for partitioned indexes, creates constraints if requested, records all necessary dependencies, and optionally builds the index data.

The function supports various index creation modes including concurrent creation, partitioned indexes, constraint creation, and can handle both regular and system table modifications. It performs extensive validation including checking for duplicate names, validating collation compatibility with operator classes, and ensuring system catalog restrictions are observed.

## Parameters / Member Variables
- : The table relation to build the index on (must be suitably locked)
- : Name for the new index relation
- : OID for the index (InvalidOid to auto-generate)
- : OID of parent index for partitioned indexes (InvalidOid otherwise)
- : OID of parent constraint for partitioned constraints (InvalidOid otherwise)
- : File number for index storage (InvalidRelFileNumber for new storage)
- : IndexInfo structure containing index metadata and properties
- : List of column names for the index
- : OID of the index access method to use
- : OID of tablespace where index should be created
- : Array of collation OIDs for index key columns
- : Array of operator class OIDs for index key columns
- : Array of opclass-specific options for index columns
- : Array of per-column index options
- : Array of statistics targets for index columns
- : Access method specific relation options
- : Bitmask controlling creation behavior (primary key, concurrent, etc.)
- : Additional flags for constraint creation
- : Whether to allow creating indexes on system tables
- : Whether this is an internal index creation
- : Output parameter receiving OID of created constraint

## Dependencies
- Functions called/Symbols referenced:
  - [heap_create](../h/heap_create.md) (for creating the index relation)
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md) (for building index tuple descriptor)
  - [UpdateIndexRelation](../U/UpdateIndexRelation.md) (for pg_index catalog entry)
  - [InsertPgClassTuple](../I/InsertPgClassTuple.md) (for pg_class catalog entry)
  - [InitializeAttributeOids](../I/InitializeAttributeOids.md) (for attribute OID assignment)
  - [AppendAttributeTuples](../A/AppendAttributeTuples.md) (for pg_attribute entries)
  - [index_constraint_create](index_constraint_create.md) (for constraint creation)
  - [StoreSingleInheritance](../S/StoreSingleInheritance.md) (for partitioned index inheritance)
  - [recordDependencyOn](../r/recordDependencyOn.md) (for dependency recording)
  - [index_build](index_build.md) (for building index data)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (from CREATE INDEX command)
  - [create_toast_table](../c/create_toast_table.md) (for TOAST table indexes)
  - [index_concurrently_create_copy](index_concurrently_create_copy.md) (for concurrent index creation)

## Notes and Other Information
- Returns the OID of the created index relation
- The function handles both regular and partitioned index creation
- Supports concurrent index creation with special validation and marking
- Performs extensive parameter validation and error checking
- Creates all necessary catalog entries and dependency relationships
- Can skip index building phase for deferred construction (ALTER TABLE scenarios)
- Properly handles inheritance relationships for partitioned tables
- Located at src/backend/catalog/index.c:724-1297

## Simplified Source

```c
Oid
index_create(Relation heapRelation, const char *indexRelationName,
             Oid indexRelationId, Oid parentIndexRelid, Oid parentConstraintId,
             RelFileNumber relFileNumber, IndexInfo *indexInfo,
             const List *indexColNames, Oid accessMethodId, Oid tableSpaceId,
             const Oid *collationIds, const Oid *opclassIds,
             const Datum *opclassOptions, const int16 *coloptions,
             const NullableDatum *stattargets, Datum reloptions,
             bits16 flags, bits16 constr_flags, bool allow_system_table_mods,
             bool is_internal, Oid *constraintId)
{
    Oid heapRelationId = RelationGetRelid(heapRelation);
    Relation pg_class;
    Relation indexRelation;
    TupleDesc indexTupDesc;
    bool isprimary = (flags & INDEX_CREATE_IS_PRIMARY) != 0;
    bool concurrent = (flags & INDEX_CREATE_CONCURRENT) != 0;
    bool partitioned = (flags & INDEX_CREATE_PARTITIONED) != 0;
    char relkind = partitioned ? RELKIND_PARTITIONED_INDEX : RELKIND_INDEX;

    // Basic parameter validation
    if (indexInfo->ii_NumIndexAttrs < 1)
        elog(ERROR, "must index at least one column");

    // Check system table modification permissions
    if (!allow_system_table_mods && IsSystemRelation(heapRelation))
        ereport(ERROR, "user-defined indexes on system catalog tables are not supported");

    // Validate collation compatibility with operator classes
    for (int i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
    {
        if (collationIds[i] &&
            (opclassIds[i] == TEXT_BTREE_PATTERN_OPS_OID ||
             opclassIds[i] == VARCHAR_BTREE_PATTERN_OPS_OID ||
             opclassIds[i] == BPCHAR_BTREE_PATTERN_OPS_OID) &&
            !get_collation_isdeterministic(collationIds[i]))
        {
            ereport(ERROR, "nondeterministic collations not supported for pattern ops");
        }
    }

    // Check for duplicate index name
    if (get_relname_relid(indexRelationName, RelationGetNamespace(heapRelation)))
    {
        if (flags & INDEX_CREATE_IF_NOT_EXISTS)
        {
            ereport(NOTICE, "relation \"%s\" already exists, skipping", indexRelationName);
            return InvalidOid;
        }
        ereport(ERROR, "relation \"%s\" already exists", indexRelationName);
    }

    // Build index tuple descriptor
    indexTupDesc = ConstructTupleDescriptor(heapRelation, indexInfo, indexColNames,
                                          accessMethodId, collationIds, opclassIds);

    // Generate OID if not provided
    if (!OidIsValid(indexRelationId))
    {
        indexRelationId = GetNewRelFileNumber(tableSpaceId, pg_class,
                                            heapRelation->rd_rel->relpersistence);
    }

    // Create the physical index relation
    indexRelation = heap_create(indexRelationName,
                               RelationGetNamespace(heapRelation),
                               tableSpaceId, indexRelationId, relFileNumber,
                               accessMethodId, indexTupDesc, relkind,
                               heapRelation->rd_rel->relpersistence,
                               heapRelation->rd_rel->relisshared,
                               RelationIsMapped(heapRelation),
                               allow_system_table_mods,
                               &relfrozenxid, &relminmxid, create_storage);

    // Lock the new index relation
    LockRelation(indexRelation, AccessExclusiveLock);

    // Update pg_class entry
    indexRelation->rd_rel->relowner = heapRelation->rd_rel->relowner;
    indexRelation->rd_rel->relam = accessMethodId;
    indexRelation->rd_rel->relispartition = OidIsValid(parentIndexRelid);

    InsertPgClassTuple(pg_class, indexRelation, indexRelationId, (Datum) 0, reloptions);

    // Initialize attribute information
    InitializeAttributeOids(indexRelation, indexInfo->ii_NumIndexAttrs, indexRelationId);
    AppendAttributeTuples(indexRelation, opclassOptions, stattargets);

    // Update pg_index catalog
    UpdateIndexRelation(indexRelationId, heapRelationId, parentIndexRelid,
                       indexInfo, collationIds, opclassIds, coloptions,
                       isprimary, (indexInfo->ii_ExclusionOps != NULL),
                       (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) == 0,
                       !concurrent, !concurrent);

    // Invalidate relcache for heap relation
    CacheInvalidateRelcache(heapRelation);

    // Handle inheritance for partitioned indexes
    if (OidIsValid(parentIndexRelid))
    {
        StoreSingleInheritance(indexRelationId, parentIndexRelid, 1);
        SetRelationHasSubclass(parentIndexRelid, true);
    }

    // Create constraint if requested
    if ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0)
    {
        char constraintType = isprimary ? CONSTRAINT_PRIMARY :
                             (indexInfo->ii_Unique ? CONSTRAINT_UNIQUE : CONSTRAINT_EXCLUSION);

        ObjectAddress localaddr = index_constraint_create(heapRelation, indexRelationId,
                                                         parentConstraintId, indexInfo,
                                                         indexRelationName, constraintType,
                                                         constr_flags, allow_system_table_mods,
                                                         is_internal);
        if (constraintId)
            *constraintId = localaddr.objectId;
    }

    // Record dependencies (simplified)
    record_index_dependencies(indexRelationId, heapRelationId, indexInfo,
                             collationIds, opclassIds, parentIndexRelid);

    // Advance command counter
    CommandCounterIncrement();

    // Build the index data unless skipped
    if (!IsBootstrapProcessingMode() && !(flags & INDEX_CREATE_SKIP_BUILD))
    {
        index_build(heapRelation, indexRelation, indexInfo, false, true);
    }

    // Close index relation (keep lock)
    index_close(indexRelation, NoLock);

    return indexRelationId;
}
```
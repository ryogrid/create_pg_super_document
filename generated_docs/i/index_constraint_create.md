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
  - table_open
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
  - table_close
- Called from (representative examples):
  - index_create
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
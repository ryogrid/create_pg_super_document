heap_create

## Overview
heap_create creates an uncataloged heap relation in memory with optional physical storage, serving as the foundation for table and index creation in PostgreSQL.

## Definition
Relation heap_create(const char *relname, Oid relnamespace, Oid reltablespace, Oid relid, RelFileNumber relfilenumber, Oid accessmtd, TupleDesc tupDesc, char relkind, char relpersistence, bool shared_relation, bool mapped_relation, bool allow_system_table_mods, TransactionId *relfrozenxid, MultiXactId *relminmxid, bool create_storage)

## Detailed Description
heap_create is a fundamental function that creates a new relation (table, index, etc.) in PostgreSQL. It builds an in-memory relation cache entry using RelationBuildLocalRelation and optionally creates the physical storage files on disk. The function handles various relation kinds and enforces security restrictions against creating objects in system catalogs.

The function validates parameters, normalizes tablespace settings, and delegates storage creation to appropriate access methods. For tables, it uses table_relation_set_new_filelocator to create both main and init forks, while for other storage-based relations it uses RelationCreateStorage. The function also handles dependency tracking for tablespaces and statistics initialization.

## Parameters / Member Variables
- relname: The name of the relation to create
- relnamespace: OID of the namespace (schema) containing the relation
- reltablespace: OID of the tablespace for the relation (InvalidOid for default)
- relid: The OID to assign to the new relation (must be valid)
- relfilenumber: File number for storage (can be invalid to use relid)
- accessmtd: OID of the access method to use
- tupDesc: Tuple descriptor defining the relation structure
- relkind: Character indicating relation kind (table, index, sequence, etc.)
- relpersistence: Persistence level (permanent, temporary, unlogged)
- shared_relation: Whether this is a cluster-wide shared relation
- mapped_relation: Whether this relation uses the relation mapper
- allow_system_table_mods: Whether to allow creation in system catalogs
- relfrozenxid: Output parameter for the frozen transaction ID
- relminmxid: Output parameter for minimum multixact ID
- create_storage: Whether to create physical storage files

## Dependencies
- Functions called/Symbols referenced:
  - [RelationBuildLocalRelation](../R/RelationBuildLocalRelation.md) (builds in-memory relation structure)
  - [table_relation_set_new_filelocator](../t/table_relation_set_new_filelocator.md) (creates table storage)
  - [RelationCreateStorage](../R/RelationCreateStorage.md) (creates generic storage)
  - [recordDependencyOnTablespace](../r/recordDependencyOnTablespace.md) (records tablespace dependencies)
  - [pgstat_create_relation](../p/pgstat_create_relation.md) (initializes statistics)
  - [IsCatalogNamespace](../I/IsCatalogNamespace.md), IsToastNamespace (namespace validation)
  - [get_namespace_name](../g/get_namespace_name.md) (error reporting)
- Called from (representative examples):
  - [heap_create_with_catalog](heap_create_with_catalog.md) (in src/backend/catalog/heap.c:1296)
  - [index_create](../i/index_create.md) (in src/backend/catalog/index.c:974)

## Notes and Other Information
- The function requires a valid relid to be provided by the caller (API change from earlier versions)
- Enforces security restrictions preventing creation of objects in pg_catalog and toast namespaces unless explicitly allowed
- Handles tablespace normalization, forcing invalid tablespace for relation kinds that do not support tablespaces
- Sets relfrozenxid and relminmxid output parameters to invalid values initially
- Storage creation is conditional based on relkind and create_storage parameter
- Creates dependency records for non-storage relations using explicit tablespaces
- Located in src/backend/catalog/heap.c:290-412

## Simplified Source

```c
Relation
heap_create(const char *relname,
            Oid relnamespace,
            Oid reltablespace,
            Oid relid,
            RelFileNumber relfilenumber,
            Oid accessmtd,
            TupleDesc tupDesc,
            char relkind,
            char relpersistence,
            bool shared_relation,
            bool mapped_relation,
            bool allow_system_table_mods,
            TransactionId *relfrozenxid,
            MultiXactId *relminmxid,
            bool create_storage)
{
    Relation rel;

    // Caller must provide valid relation OID
    Assert(OidIsValid(relid));

    // Security check: prevent creation in system catalogs unless allowed
    if (!allow_system_table_mods &&
        ((IsCatalogNamespace(relnamespace) && relkind != RELKIND_INDEX) ||
         IsToastNamespace(relnamespace)) &&
        IsNormalProcessingMode())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied to create \"%s.%s\"",
                        get_namespace_name(relnamespace), relname),
                 errdetail("System catalog modifications are currently disallowed.")));

    // Initialize transaction IDs
    *relfrozenxid = InvalidTransactionId;
    *relminmxid = InvalidMultiXactId;

    // Clear tablespace for relation kinds that don't support it
    if (!RELKIND_HAS_TABLESPACE(relkind))
        reltablespace = InvalidOid;

    // Skip storage creation for virtual relations
    if (!RELKIND_HAS_STORAGE(relkind))
        create_storage = false;
    else {
        // Use relid as file number if not specified
        if (!RelFileNumberIsValid(relfilenumber))
            relfilenumber = relid;
    }

    // Force default tablespace to InvalidOid for database cloning compatibility
    if (reltablespace == MyDatabaseTableSpace)
        reltablespace = InvalidOid;

    // Build the in-memory relation structure
    rel = RelationBuildLocalRelation(relname,
                                     relnamespace,
                                     tupDesc,
                                     relid,
                                     accessmtd,
                                     relfilenumber,
                                     reltablespace,
                                     shared_relation,
                                     mapped_relation,
                                     relpersistence,
                                     relkind);

    // Create physical storage if requested
    if (create_storage) {
        if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind))
            table_relation_set_new_filelocator(rel, &rel->rd_locator,
                                              relpersistence,
                                              relfrozenxid, relminmxid);
        else if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
            RelationCreateStorage(rel->rd_locator, relpersistence, true);
    }

    // Record tablespace dependency for non-storage relations
    if (!create_storage && reltablespace != InvalidOid)
        recordDependencyOnTablespace(RelationRelationId, relid, reltablespace);

    // Initialize statistics tracking
    pgstat_create_relation(rel);

    return rel;
}
```
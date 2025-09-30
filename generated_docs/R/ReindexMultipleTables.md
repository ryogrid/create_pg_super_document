# ReindexMultipleTables

## Location
[src/backend/commands/indexcmds.c:2977-3195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2977-L3195)

## Overview
ReindexMultipleTables recreates indexes for multiple tables selected by objectName/objectKind (schema, database, or system catalogs) with each table processed in a separate transaction to reduce deadlock probability.

## Definition
```c
static void ReindexMultipleTables(const ReindexStmt *stmt, const ReindexParams *params)
```

## Detailed Description
This function orchestrates bulk reindexing operations across multiple tables within a specified scope (schema, database, or system catalogs). Key behaviors include:

1. **Scope Validation**: Validates the target object (schema, database, or system catalogs) and checks appropriate permissions
2. **Permission Checking**: Performs different permission checks based on object type (schema ownership, database ownership, or ROLE_PG_MAINTAIN privileges)
3. **Table Discovery**: Scans pg_class to identify candidate tables for reindexing based on the specified scope
4. **Filtering Logic**: Applies multiple filters to exclude inappropriate relations:
   - Only processes regular tables (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)  
   - Skips temporary tables from other backends
   - Handles system vs user catalogs based on object kind
   - Enforces concurrent reindexing restrictions for system catalogs
   - Manages tablespace restrictions for mapped relations and system tables
5. **Transaction Management**: Processes each relation in a separate transaction to minimize deadlock risk
6. **Ordering Optimization**: Prioritizes pg_class to ensure catalog integrity before processing other relations

## Parameters / Member Variables
- `stmt`: ReindexStmt containing reindex statement details including target object name and kind
- `params`: ReindexParams specifying reindex options like concurrency, tablespace, and other flags

## Dependencies
- Functions called/Symbols referenced:
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_database_name](../g/get_database_name.md)
  - AllocSetContextCreate
  - [table_open](../t/table_open.md)/table_close
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [IsCatalogRelationOid](../I/IsCatalogRelationOid.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [ReindexMultipleInternal](ReindexMultipleInternal.md)
- Called from:
  - [ExecReindex](../E/ExecReindex.md)

## Notes and Other Information
- Must not be called within a user transaction block due to internal transaction commits
- Concurrent reindexing of system catalogs is explicitly prohibited with an error
- The function creates a private memory context to survive transaction commits
- pg_class is always reindexed first when selected to ensure catalog integrity
- Provides warnings for skipped relations due to concurrent or tablespace restrictions
- Uses separate transactions for each table to reduce deadlock probability and allow immediate lock release
- Supports filtering by relation persistence (temporary vs permanent) and ownership checks for shared catalogs

## Simplified Source

```c
static void ReindexMultipleTables(const ReindexStmt *stmt, const ReindexParams *params) {
    Oid objectOid;
    List *relids = NIL;
    MemoryContext private_context;

    // Validate reindex object type (schema, database, or system)
    Assert(stmt->kind == REINDEX_OBJECT_SCHEMA ||
           stmt->kind == REINDEX_OBJECT_SYSTEM ||
           stmt->kind == REINDEX_OBJECT_DATABASE);

    // Prohibit concurrent reindexing of system catalogs
    if (stmt->kind == REINDEX_OBJECT_SYSTEM &&
        (params->options & REINDEXOPT_CONCURRENTLY)) {
        ereport(ERROR, "cannot reindex system catalogs concurrently");
    }

    // Get target object OID and check permissions
    if (stmt->kind == REINDEX_OBJECT_SCHEMA) {
        objectOid = get_namespace_oid(stmt->name, false);
        // Check schema ownership or maintenance privileges
        if (!object_ownercheck(NamespaceRelationId, objectOid, GetUserId()) &&
            !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA, stmt->name);
        }
    } else {
        objectOid = MyDatabaseId;
        // Check database ownership or maintenance privileges
        if (!object_ownercheck(DatabaseRelationId, objectOid, GetUserId()) &&
            !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, get_database_name(objectOid));
        }
    }

    // Create memory context to survive transaction commits
    private_context = AllocSetContextCreate(PortalContext, "ReindexMultipleTables",
                                          ALLOCSET_SMALL_SIZES);

    // Scan pg_class to find tables to reindex
    Relation relationRelation = table_open(RelationRelationId, AccessShareLock);
    TableScanDesc scan = table_beginscan_catalog(relationRelation,
                                               stmt->kind == REINDEX_OBJECT_SCHEMA ? 1 : 0,
                                               scan_keys);

    HeapTuple tuple;
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class classtuple = (Form_pg_class) GETSTRUCT(tuple);
        Oid relid = classtuple->oid;

        // Filter: only regular tables and materialized views
        if (classtuple->relkind != RELKIND_RELATION &&
            classtuple->relkind != RELKIND_MATVIEW) {
            continue;
        }

        // Filter: skip temp tables from other backends
        if (classtuple->relpersistence == RELPERSISTENCE_TEMP &&
            !isTempNamespace(classtuple->relnamespace)) {
            continue;
        }

        // Filter: system vs user catalogs based on object kind
        if (stmt->kind == REINDEX_OBJECT_SYSTEM && !IsCatalogRelationOid(relid)) {
            continue;
        }
        if (stmt->kind == REINDEX_OBJECT_DATABASE && IsCatalogRelationOid(relid)) {
            continue;
        }

        // Filter: check permissions for shared catalogs
        if (classtuple->relisshared &&
            pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) != ACLCHECK_OK) {
            continue;
        }

        // Add to relation list (pg_class first for integrity)
        MemoryContext old = MemoryContextSwitchTo(private_context);
        if (relid == RelationRelationId) {
            relids = lcons_oid(relid, relids);  // Add to front
        } else {
            relids = lappend_oid(relids, relid); // Add to end
        }
        MemoryContextSwitchTo(old);
    }

    table_endscan(scan);
    table_close(relationRelation, AccessShareLock);

    // Process each relation in separate transactions
    ReindexMultipleInternal(stmt, relids, params);

    MemoryContextDelete(private_context);
}
```
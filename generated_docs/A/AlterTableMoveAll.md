# AlterTableMoveAll

## Location
[src/backend/commands/tablecmds.c:15385-15546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15385-L15546)

## Overview
AlterTableMoveAll implements the ALTER TABLE ALL ... SET TABLESPACE command, allowing batch movement of all objects of a specified type from one tablespace to another, with optional filtering by object owner.

## Definition
```c
Oid AlterTableMoveAll(AlterTableMoveAllStmt *stmt)
```

## Detailed Description
This function provides bulk tablespace migration functionality for database objects by scanning the pg_class system catalog to find all relations of the specified type in the source tablespace and moving them to the destination tablespace. It supports filtering by object type (tables, indexes, materialized views) and owner roles, performing comprehensive permission checks and locking operations to ensure safe concurrent execution.

The function operates in phases: first validating tablespace permissions and resolving OIDs, then scanning pg_class to identify candidate objects while applying various filters (object type, ownership, system restrictions), collecting and locking all target relations, and finally executing individual ALTER TABLE commands for each relation. The process includes safeguards against moving system catalogs, shared tables, temporary tables, and TOAST tables, which are handled automatically with their parent tables.

## Parameters / Member Variables
- `stmt`: AlterTableMoveAllStmt structure containing the command parameters including source tablespace, destination tablespace, object type filter, role filters, and NOWAIT option

## Dependencies
- Functions called/Symbols referenced:
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Converts role specifications to OID list
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Resolves tablespace names to OIDs
  - [object_aclcheck](../o/object_aclcheck.md): Checks user permissions on tablespaces
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission-related errors
  - [table_open](../t/table_open.md)/table_close: Opens and closes system catalogs
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Starts catalog scan
  - [heap_getnext](../h/heap_getnext.md): Retrieves next tuple from scan
  - [IsCatalogNamespace](../I/IsCatalogNamespace.md): Checks if namespace is system catalog
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md): Checks if namespace is temporary
  - [IsToastNamespace](../I/IsToastNamespace.md): Checks if namespace is for TOAST tables
  - [object_ownercheck](../o/object_ownercheck.md): Verifies object ownership
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md): Attempts non-blocking lock acquisition
  - [LockRelationOid](../L/LockRelationOid.md): Acquires exclusive lock on relation
  - [AlterTableInternal](AlterTableInternal.md): Executes individual ALTER TABLE operations
  - [EventTriggerAlterTableStart](../E/EventTriggerAlterTableStart.md)/End: Manages event trigger execution

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing function

## Notes and Other Information
- Supports three object types: tables (including partitioned tables), indexes (including partitioned indexes), and materialized views
- Automatically excludes system catalogs, shared relations, temporary tables, and TOAST tables from bulk operations
- Requires CREATE permission on destination tablespace and ownership of each object being moved
- Implements NOWAIT semantics for lock acquisition to avoid blocking on busy objects
- Performs no-op detection when source and destination tablespaces are identical
- Uses AccessExclusiveLock to prevent concurrent modifications during the move operation
- Handles database default tablespace by converting to InvalidOid for internal representation
- Provides informative notice when no matching objects are found in the source tablespace
- Integrates with event trigger system for proper dependency tracking and custom logic execution
- Returns the destination tablespace OID upon successful completion

## Simplified Source

```c
Oid AlterTableMoveAll(AlterTableMoveAllStmt *stmt)
{
    List       *relations = NIL;
    ListCell   *l;
    ScanKeyData key[1];
    Relation    rel;
    TableScanDesc scan;
    HeapTuple   tuple;
    Oid         orig_tablespaceoid;
    Oid         new_tablespaceoid;
    List       *role_oids = roleSpecsToIds(stmt->roles);

    // Validate object type (tables, indexes, materialized views only)
    if (stmt->objtype != OBJECT_TABLE && stmt->objtype != OBJECT_INDEX &&
        stmt->objtype != OBJECT_MATVIEW)
        ereport(ERROR, "only tables, indexes, and materialized views exist in tablespaces");

    // Resolve tablespace names to OIDs
    orig_tablespaceoid = get_tablespace_oid(stmt->orig_tablespacename, false);
    new_tablespaceoid = get_tablespace_oid(stmt->new_tablespacename, false);

    // Cannot move shared relations to/from pg_global
    if (orig_tablespaceoid == GLOBALTABLESPACE_OID ||
        new_tablespaceoid == GLOBALTABLESPACE_OID)
        ereport(ERROR, "cannot move relations in to or out of pg_global tablespace");

    // Check CREATE permission on destination tablespace
    if (OidIsValid(new_tablespaceoid) && new_tablespaceoid != MyDatabaseTableSpace)
    {
        AclResult aclresult = object_aclcheck(TableSpaceRelationId, new_tablespaceoid,
                                             GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_TABLESPACE,
                          get_tablespace_name(new_tablespaceoid));
    }

    // Handle default tablespace representation
    if (orig_tablespaceoid == MyDatabaseTableSpace)
        orig_tablespaceoid = InvalidOid;
    if (new_tablespaceoid == MyDatabaseTableSpace)
        new_tablespaceoid = InvalidOid;

    // Early exit if source and destination are the same
    if (orig_tablespaceoid == new_tablespaceoid)
        return new_tablespaceoid;

    // Scan pg_class for objects in the source tablespace
    ScanKeyInit(&key[0], Anum_pg_class_reltablespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(orig_tablespaceoid));

    rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 1, key);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
        Oid relOid = relForm->oid;

        // Skip system catalogs, shared tables, temp tables, TOAST tables
        if (IsCatalogNamespace(relForm->relnamespace) ||
            relForm->relisshared ||
            isAnyTempNamespace(relForm->relnamespace) ||
            IsToastNamespace(relForm->relnamespace))
            continue;

        // Filter by requested object type
        if ((stmt->objtype == OBJECT_TABLE &&
             relForm->relkind != RELKIND_RELATION &&
             relForm->relkind != RELKIND_PARTITIONED_TABLE) ||
            (stmt->objtype == OBJECT_INDEX &&
             relForm->relkind != RELKIND_INDEX &&
             relForm->relkind != RELKIND_PARTITIONED_INDEX) ||
            (stmt->objtype == OBJECT_MATVIEW &&
             relForm->relkind != RELKIND_MATVIEW))
            continue;

        // Filter by owner if roles specified
        if (role_oids != NIL && !list_member_oid(role_oids, relForm->relowner))
            continue;

        // Check ownership permission
        if (!object_ownercheck(RelationRelationId, relOid, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relOid)),
                          NameStr(relForm->relname));

        // Acquire lock (with NOWAIT option support)
        if (stmt->nowait && !ConditionalLockRelationOid(relOid, AccessExclusiveLock))
            ereport(ERROR, "aborting because lock on relation is not available");
        else
            LockRelationOid(relOid, AccessExclusiveLock);

        // Add to list of objects to move
        relations = lappend_oid(relations, relOid);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    // Notify if no objects found
    if (relations == NIL)
        ereport(NOTICE, "no matching relations in tablespace found");

    // Move each relation to the new tablespace
    foreach(l, relations)
    {
        List *cmds = NIL;
        AlterTableCmd *cmd = makeNode(AlterTableCmd);

        cmd->subtype = AT_SetTableSpace;
        cmd->name = stmt->new_tablespacename;
        cmds = lappend(cmds, cmd);

        EventTriggerAlterTableStart((Node *) stmt);
        AlterTableInternal(lfirst_oid(l), cmds, false);
        EventTriggerAlterTableEnd();
    }

    return new_tablespaceoid;
}
```
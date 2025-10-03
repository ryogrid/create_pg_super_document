# CreateTableSpace

## Location
[src/backend/commands/tablespace.c:208-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L208-L394)

## Overview
Creates a new tablespace by validating parameters, inserting catalog entries, creating filesystem directories, and logging the operation in WAL, with strict permission checks and validation.

## Definition

```c
Oid
CreateTableSpace(CreateTableSpaceStmt *stmt)
```
## Detailed Description
CreateTableSpace implements the CREATE TABLESPACE SQL command, handling the complete process of tablespace creation. The function performs extensive validation including superuser privilege checks, path validation, name collision detection, and filesystem structure creation.

The process involves multiple phases: parameter validation and canonicalization, catalog insertion with proper locking to prevent race conditions, filesystem directory creation, WAL logging for crash recovery, and dependency tracking. The function uses forced synchronous commit to minimize the window between filesystem changes and transaction commit.

Special handling is provided for binary upgrade scenarios and in-place tablespaces (developer feature). The function integrates with PostgreSQL's object management system through dependency recording and post-creation hooks.

## Parameters / Member Variables
- `*stmt`: CreateTableSpaceStmt structure containing tablespace name, location, owner specification, and options
## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md): Checks if current user has superuser privileges
  - [get_rolespec_oid](../g/get_rolespec_oid.md): Resolves role specification to OID
  - [canonicalize_path](../c/canonicalize_path.md): Normalizes filesystem path
  - is_absolute_path: Validates path is absolute
  - [IsReservedName](../I/IsReservedName.md): Checks for reserved name patterns
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Checks for existing tablespace with same name
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Allocates new OID for tablespace
  - [transformRelOptions](../t/transformRelOptions.md): Processes tablespace options
  - [tablespace_reloptions](../t/tablespace_reloptions.md): Validates tablespace-specific options
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates catalog tuple
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts tuple into system catalog
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Records ownership dependency
  - [create_tablespace_directories](../c/create_tablespace_directories.md): Creates filesystem structure
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert: WAL logging functions
  - [ForceSyncCommit](../F/ForceSyncCommit.md): Forces synchronous transaction commit
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): During SQL command processing

## Notes and Other Information
- Requires superuser privileges for execution
- Performs comprehensive path validation including length checks and security restrictions
- Warns against creating tablespaces within the data directory
- Reserves 'pg_' prefix for system tablespaces
- Uses binary upgrade OID override when in binary upgrade mode
- Implements double-checked locking pattern for name collision detection
- Forces synchronous commit to ensure atomicity between filesystem and catalog changes
- Integrates with PostgreSQL's dependency system and object creation hooks

## Simplified Source

```c
Oid CreateTableSpace(CreateTableSpaceStmt *stmt) {
    Relation rel;
    Datum values[Natts_pg_tablespace];
    bool nulls[Natts_pg_tablespace] = {0};
    HeapTuple tuple;
    Oid tablespaceoid;
    char *location;
    Oid ownerId;
    Datum newOptions;

    // Must be superuser to create tablespaces
    if (!superuser()) {
        ereport(ERROR, "permission denied to create tablespace \"%s\"",
                stmt->tablespacename);
    }

    // Determine owner (specified or current user)
    if (stmt->owner)
        ownerId = get_rolespec_oid(stmt->owner, false);
    else
        ownerId = GetUserId();

    // Canonicalize and validate the location path
    location = pstrdup(stmt->location);
    canonicalize_path(location);

    // Security checks on location
    if (strchr(location, '\''))
        ereport(ERROR, "tablespace location cannot contain single quotes");

    bool in_place = allow_in_place_tablespaces && strlen(location) == 0;

    if (!in_place && !is_absolute_path(location))
        ereport(ERROR, "tablespace location must be an absolute path");

    // Check path length constraints
    if (strlen(location) + path_components_length > MAXPGPATH)
        ereport(ERROR, "tablespace location \"%s\" is too long", location);

    // Warn about data directory usage
    if (path_is_prefix_of_path(DataDir, location))
        ereport(WARNING, "tablespace location should not be inside data directory");

    // Validate tablespace name (no "pg_" prefix)
    if (!allowSystemTableMods && IsReservedName(stmt->tablespacename))
        ereport(ERROR, "unacceptable tablespace name \"%s\"", stmt->tablespacename);

    // Check for duplicate tablespace name
    if (OidIsValid(get_tablespace_oid(stmt->tablespacename, true)))
        ereport(ERROR, "tablespace \"%s\" already exists", stmt->tablespacename);

    // Open catalog and allocate OID
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    if (IsBinaryUpgrade) {
        tablespaceoid = binary_upgrade_next_pg_tablespace_oid;
        binary_upgrade_next_pg_tablespace_oid = InvalidOid;
    } else {
        tablespaceoid = GetNewOidWithIndex(rel, TablespaceOidIndexId,
                                          Anum_pg_tablespace_oid);
    }

    // Build catalog tuple
    values[Anum_pg_tablespace_oid - 1] = ObjectIdGetDatum(tablespaceoid);
    values[Anum_pg_tablespace_spcname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->tablespacename));
    values[Anum_pg_tablespace_spcowner - 1] = ObjectIdGetDatum(ownerId);
    nulls[Anum_pg_tablespace_spcacl - 1] = true;

    // Process tablespace options
    newOptions = transformRelOptions((Datum) 0, stmt->options,
                                    NULL, NULL, false, false);
    tablespace_reloptions(newOptions, true);  // Validate options
    if (newOptions != (Datum) 0)
        values[Anum_pg_tablespace_spcoptions - 1] = newOptions;
    else
        nulls[Anum_pg_tablespace_spcoptions - 1] = true;

    // Insert into catalog
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tuple);
    heap_freetuple(tuple);

    // Record ownership dependency
    recordDependencyOnOwner(TableSpaceRelationId, tablespaceoid, ownerId);

    // Post-creation hook
    InvokeObjectPostCreateHook(TableSpaceRelationId, tablespaceoid, 0);

    // Create filesystem directories
    create_tablespace_directories(location, tablespaceoid);

    // Log to WAL for crash recovery
    xl_tblspc_create_rec xlrec;
    xlrec.ts_id = tablespaceoid;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, offsetof(xl_tblspc_create_rec, ts_path));
    XLogRegisterData((char *) location, strlen(location) + 1);
    XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_CREATE);

    // Force synchronous commit for atomicity
    ForceSyncCommit();

    pfree(location);
    table_close(rel, NoLock);

    return tablespaceoid;
}
```
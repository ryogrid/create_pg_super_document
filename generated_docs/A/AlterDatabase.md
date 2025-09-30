# AlterDatabase

## Location
[src/backend/commands/dbcommands.c:2328-2500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2328-L2500)

## Overview
AlterDatabase processes ALTER DATABASE statements to modify database properties such as template status, connection permissions, connection limits, and tablespace assignments.

## Definition
```c
Oid AlterDatabase(ParseState *pstate, AlterDatabaseStmt *stmt, bool isTopLevel)
```

## Detailed Description
AlterDatabase handles various ALTER DATABASE operations by parsing statement options and updating the corresponding database properties in the pg_database system catalog. It supports modifying the template status (is_template), connection allowance (allow_connections), connection limits (connection_limit), and tablespace assignment. For tablespace changes, it delegates to the movedb function to physically relocate database files. The function includes comprehensive validation to prevent dangerous operations like disabling connections to the current database or setting invalid connection limits.

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting
- `stmt`: AlterDatabaseStmt structure containing the database name and list of modification options  
- `isTopLevel`: Boolean indicating whether this is a top-level statement (affects transaction block restrictions)

## Dependencies
- Functions called/Symbols referenced:
  - [AlterDatabaseStmt](AlterDatabaseStmt.md): Statement structure containing alter database parameters
  - [DefElem](../D/DefElem.md): Definition element structure for parsing individual options
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md): Reports errors for duplicate options
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents certain operations within transaction blocks
  - [movedb](../m/movedb.md): Handles database tablespace relocation
  - [defGetBoolean](../d/defGetBoolean.md)/defGetInt32/defGetString: Extract typed values from DefElem
  - [database_is_invalid_form](../d/database_is_invalid_form.md): Checks if database is in invalid state
  - [object_ownercheck](../o/object_ownercheck.md): Validates database ownership permissions
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified catalog tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates database catalog entry
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility statement processing function

## Notes and Other Information
- Supports four main options: is_template, allow_connections, connection_limit, and tablespace
- Tablespace option cannot be combined with other options and requires special handling via movedb
- Prevents disabling connections to the currently connected database to avoid lockout
- Validates connection limits to ensure they are not below the minimum allowed value
- Uses tuple locking to prevent concurrent modifications during the update process
- Returns the database OID for most operations, InvalidOid for tablespace moves
- Includes checks for invalid databases and proper error reporting with parser position information

## Simplified Source

```c
Oid AlterDatabase(ParseState *pstate, AlterDatabaseStmt *stmt, bool isTopLevel) {
    bool dbistemplate = false;
    bool dballowconnections = true;
    int dbconnlimit = DATCONNLIMIT_UNLIMITED;
    DefElem *distemplate = NULL, *dallowconnections = NULL,
            *dconnlimit = NULL, *dtablespace = NULL;

    // Parse statement options
    foreach(option, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "is_template") == 0)
            distemplate = defel;
        else if (strcmp(defel->defname, "allow_connections") == 0)
            dallowconnections = defel;
        else if (strcmp(defel->defname, "connection_limit") == 0)
            dconnlimit = defel;
        else if (strcmp(defel->defname, "tablespace") == 0)
            dtablespace = defel;
        else
            ereport(ERROR, "unrecognized option");
    }

    // Handle tablespace change specially
    if (dtablespace) {
        PreventInTransactionBlock(isTopLevel, "ALTER DATABASE SET TABLESPACE");
        movedb(stmt->dbname, defGetString(dtablespace));
        return InvalidOid;
    }

    // Extract option values
    if (distemplate && distemplate->arg)
        dbistemplate = defGetBoolean(distemplate);
    if (dallowconnections && dallowconnections->arg)
        dballowconnections = defGetBoolean(dallowconnections);
    if (dconnlimit && dconnlimit->arg)
        dbconnlimit = defGetInt32(dconnlimit);

    // Find and lock the database tuple
    rel = table_open(DatabaseRelationId, RowExclusiveLock);
    tuple = /* search for database by name */;
    dboid = datform->oid;

    // Permission and safety checks
    if (!object_ownercheck(DatabaseRelationId, dboid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, stmt->dbname);

    if (!dballowconnections && dboid == MyDatabaseId)
        ereport(ERROR, "cannot disallow connections for current database");

    // Build updated tuple and update catalog
    if (distemplate)
        new_record[Anum_pg_database_datistemplate - 1] = BoolGetDatum(dbistemplate);
    if (dallowconnections)
        new_record[Anum_pg_database_datallowconn - 1] = BoolGetDatum(dballowconnections);
    if (dconnlimit)
        new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(dbconnlimit);

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), new_record, nulls, replaces);
    CatalogTupleUpdate(rel, &tuple->t_self, newtuple);

    InvokeObjectPostAlterHook(DatabaseRelationId, dboid, 0);
    table_close(rel, NoLock);

    return dboid;
}
```
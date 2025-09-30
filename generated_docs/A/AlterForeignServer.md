# AlterForeignServer

## Location
[src/backend/commands/foreigncmds.c:985-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L985-L1085)

## Overview
Modifies an existing foreign server definition by updating its version string and/or options while maintaining proper access control and validation.

## Definition
```c
ObjectAddress AlterForeignServer(AlterForeignServerStmt *stmt)
```

## Detailed Description
This function implements the ALTER SERVER SQL command by modifying an existing foreign server entry in the pg_foreign_server system catalog. It performs access control validation ensuring that only the server owner or a superuser can make modifications. The function supports selective updates to the server version string and server options, with options being validated through the associated foreign-data wrapper's validator function. It uses heap_modify_tuple for efficient partial updates, updating only the specified attributes rather than replacing the entire tuple.

## Parameters / Member Variables
- `stmt`: AlterForeignServerStmt structure containing the parsed ALTER SERVER statement details including server name, version changes, and option modifications

## Dependencies
- Functions called/Symbols referenced:
  - [AlterForeignServerStmt](AlterForeignServerStmt.md)
  - Form_pg_foreign_server
  - SearchSysCacheCopy1
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [ForeignDataWrapper](../F/ForeignDataWrapper.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Enforces ownership-based access control - only server owner or superuser can alter servers
- Validates server existence before attempting modifications
- Supports selective updates through has_version flag for version changes
- Server options are validated using the associated FDW's validator function
- Version string can be set to a new value or cleared (set to NULL)
- Uses heap_modify_tuple for efficient selective column updates
- Triggers object alteration hooks for extensibility
- Returns ObjectAddress of the modified server for further reference
- Does not modify server name or associated FDW - these are immutable after creation
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling dynamic reconfiguration of server properties

## Simplified Source

```c
ObjectAddress AlterForeignServer(AlterForeignServerStmt *stmt) {
    Relation rel;
    HeapTuple tp;
    Datum repl_val[Natts_pg_foreign_server];
    bool repl_null[Natts_pg_foreign_server];
    bool repl_repl[Natts_pg_foreign_server];
    Oid srvId;
    Form_pg_foreign_server srvForm;
    ObjectAddress address;

    // Open catalog table with exclusive lock
    rel = table_open(ForeignServerRelationId, RowExclusiveLock);

    // Find the foreign server tuple
    tp = SearchSysCacheCopy1(FOREIGNSERVERNAME, CStringGetDatum(stmt->servername));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("server \"%s\" does not exist", stmt->servername)));

    srvForm = (Form_pg_foreign_server) GETSTRUCT(tp);
    srvId = srvForm->oid;

    // Check ownership - only owner or superuser can alter
    if (!object_ownercheck(ForeignServerRelationId, srvId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER, stmt->servername);

    // Initialize update arrays
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Update version string if specified
    if (stmt->has_version) {
        if (stmt->version)
            repl_val[Anum_pg_foreign_server_srvversion - 1] = CStringGetTextDatum(stmt->version);
        else
            repl_null[Anum_pg_foreign_server_srvversion - 1] = true;

        repl_repl[Anum_pg_foreign_server_srvversion - 1] = true;
    }

    // Update server options if specified
    if (stmt->options) {
        ForeignDataWrapper *fdw = GetForeignDataWrapper(srvForm->srvfdw);
        Datum datum;
        bool isnull;

        // Get current server options
        datum = SysCacheGetAttr(FOREIGNSERVEROID, tp,
                               Anum_pg_foreign_server_srvoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        // Transform and validate options using FDW validator
        datum = transformGenericOptions(ForeignServerRelationId, datum,
                                       stmt->options, fdw->fdwvalidator);

        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_foreign_server_srvoptions - 1] = datum;
        else
            repl_null[Anum_pg_foreign_server_srvoptions - 1] = true;

        repl_repl[Anum_pg_foreign_server_srvoptions - 1] = true;
    }

    // Update the catalog tuple
    tp = heap_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(rel, &tp->t_self, tp);

    // Trigger post-alter hooks and cleanup
    InvokeObjectPostAlterHook(ForeignServerRelationId, srvId, 0);
    ObjectAddressSet(address, ForeignServerRelationId, srvId);

    heap_freetuple(tp);
    table_close(rel, RowExclusiveLock);

    return address;
}
```
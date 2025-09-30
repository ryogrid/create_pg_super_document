# CreateForeignServer

## Location
[src/backend/commands/foreigncmds.c:849-984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L849-L984)

## Overview
Creates a new foreign server definition in PostgreSQL, establishing a logical connection endpoint for accessing external data through a specified foreign-data wrapper.

## Definition
```c
ObjectAddress CreateForeignServer(CreateForeignServerStmt *stmt)
```

## Detailed Description
This function implements the CREATE SERVER SQL command by creating a new foreign server entry in the pg_foreign_server system catalog. It performs comprehensive validation including server name uniqueness checks, FDW existence verification, and access control validation (USAGE privilege on the FDW). The function handles IF NOT EXISTS logic with proper extension membership validation for security, processes optional server type and version specifications, and validates server options using the associated FDW's validator function. It establishes proper dependencies between the server and its underlying FDW while ensuring appropriate ownership and extension membership.

## Parameters / Member Variables
- `stmt`: CreateForeignServerStmt structure containing the parsed CREATE SERVER statement details including server name, FDW name, server type, version, options, and IF NOT EXISTS flag

## Dependencies
- Functions called/Symbols referenced:
  - [CreateForeignServerStmt](CreateForeignServerStmt.md)
  - [AclResult](../A/AclResult.md)
  - [ForeignDataWrapper](../F/ForeignDataWrapper.md)
  - [get_foreign_server_oid](../g/get_foreign_server_oid.md)
  - ObjectAddressSet
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
  - [GetForeignDataWrapperByName](../G/GetForeignDataWrapperByName.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [namein](../n/namein.md)
  - DirectFunctionCall1
  - [CStringGetDatum](CStringGetDatum.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Automatically assigns the effective user ID as the server owner (cannot be overridden during creation)
- Supports IF NOT EXISTS semantics with proper extension membership validation for security
- Requires USAGE privilege on the underlying foreign-data wrapper
- Optional server type and version parameters can be specified for documentation purposes
- Server options are validated using the FDW's validator function if available
- Records dependency on the associated FDW to prevent orphaned servers
- Supports extension membership through recordDependencyOnCurrentExtension
- Triggers object creation hooks for extensibility
- Returns InvalidObjectAddress when IF NOT EXISTS is used and server already exists
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling logical organization of external data sources

## Simplified Source

```c
ObjectAddress
CreateForeignServer(CreateForeignServerStmt *stmt)
{
    Relation rel;
    Datum srvoptions;
    Datum values[Natts_pg_foreign_server];
    bool nulls[Natts_pg_foreign_server];
    HeapTuple tuple;
    Oid srvId, ownerId;
    AclResult aclresult;
    ObjectAddress myself, referenced;
    ForeignDataWrapper *fdw;

    rel = table_open(ForeignServerRelationId, RowExclusiveLock);
    ownerId = GetUserId();

    // Check for duplicate server name with IF NOT EXISTS support
    srvId = get_foreign_server_oid(stmt->servername, true);
    if (OidIsValid(srvId)) {
        if (stmt->if_not_exists) {
            // Security check for extension membership
            ObjectAddressSet(myself, ForeignServerRelationId, srvId);
            checkMembershipInCurrentExtension(&myself);

            ereport(NOTICE, "server already exists, skipping");
            table_close(rel, RowExclusiveLock);
            return InvalidObjectAddress;
        } else {
            ereport(ERROR, "server already exists");
        }
    }

    // Validate FDW exists and check USAGE permission
    fdw = GetForeignDataWrapperByName(stmt->fdwname, false);
    aclresult = object_aclcheck(ForeignDataWrapperRelationId, fdw->fdwid, ownerId, ACL_USAGE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, OBJECT_FDW, fdw->fdwname);
    }

    // Prepare tuple values
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    srvId = GetNewOidWithIndex(rel, ForeignServerOidIndexId, Anum_pg_foreign_server_oid);
    values[Anum_pg_foreign_server_oid - 1] = ObjectIdGetDatum(srvId);
    values[Anum_pg_foreign_server_srvname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->servername));
    values[Anum_pg_foreign_server_srvowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_foreign_server_srvfdw - 1] = ObjectIdGetDatum(fdw->fdwid);

    // Add optional server type and version
    if (stmt->servertype)
        values[Anum_pg_foreign_server_srvtype - 1] = CStringGetTextDatum(stmt->servertype);
    else
        nulls[Anum_pg_foreign_server_srvtype - 1] = true;

    if (stmt->version)
        values[Anum_pg_foreign_server_srvversion - 1] = CStringGetTextDatum(stmt->version);
    else
        nulls[Anum_pg_foreign_server_srvversion - 1] = true;

    nulls[Anum_pg_foreign_server_srvacl - 1] = true;

    // Transform and validate server options
    srvoptions = transformGenericOptions(ForeignServerRelationId,
                                        PointerGetDatum(NULL),
                                        stmt->options,
                                        fdw->fdwvalidator);

    if (PointerIsValid(DatumGetPointer(srvoptions)))
        values[Anum_pg_foreign_server_srvoptions - 1] = srvoptions;
    else
        nulls[Anum_pg_foreign_server_srvoptions - 1] = true;

    // Insert catalog entry
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tuple);
    heap_freetuple(tuple);

    // Record dependencies
    myself.classId = ForeignServerRelationId;
    myself.objectId = srvId;
    myself.objectSubId = 0;

    referenced.classId = ForeignDataWrapperRelationId;
    referenced.objectId = fdw->fdwid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnOwner(ForeignServerRelationId, srvId, ownerId);
    recordDependencyOnCurrentExtension(&myself, false);

    // Invoke creation hook
    InvokeObjectPostCreateHook(ForeignServerRelationId, srvId, 0);

    table_close(rel, RowExclusiveLock);
    return myself;
}
```
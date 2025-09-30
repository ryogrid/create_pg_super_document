# AlterUserMapping

## Location
[src/backend/commands/foreigncmds.c:1237-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L1237-L1334)

## Overview
Modifies the options and configuration of an existing user mapping for accessing a foreign server in PostgreSQL's foreign data wrapper system.

## Definition
```c
ObjectAddress AlterUserMapping(AlterUserMappingStmt *stmt)
```

## Detailed Description
This function implements the ALTER USER MAPPING SQL command by updating an existing entry in the pg_user_mapping system catalog. It allows users to modify connection options and authentication parameters for existing foreign server mappings without recreating them. The function performs comprehensive validation including existence checks, permission verification through the same access control mechanisms as other user mapping operations, and option validation through the foreign data wrapper's validator function. The function supports both regular users and the special PUBLIC role, and uses PostgreSQL's standard tuple modification mechanisms to update the catalog entry.

## Parameters / Member Variables
- `stmt`: Pointer to AlterUserMappingStmt structure containing the parsed ALTER USER MAPPING command details including user specification, server name, and new options to be applied

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [get_rolespec_oid](../g/get_rolespec_oid.md)
  - [GetForeignServerByName](../G/GetForeignServerByName.md)
  - GetSysCacheOid2
  - [user_mapping_ddl_aclcheck](../u/user_mapping_ddl_aclcheck.md)
  - SearchSysCacheCopy1
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function performs in-place modification of the user mapping's options by retrieving the existing tuple, modifying only the options column, and updating the catalog. It properly handles NULL options by using replacement arrays to control which columns are updated. The function maintains all existing dependencies and relationships while allowing option changes, making it safe for ongoing foreign data wrapper operations.

## Simplified Source

```c
ObjectAddress AlterUserMapping(AlterUserMappingStmt *stmt)
{
    Relation    rel;
    HeapTuple   tp;
    Datum       repl_val[Natts_pg_user_mapping];
    bool        repl_null[Natts_pg_user_mapping];
    bool        repl_repl[Natts_pg_user_mapping];
    Oid         useId;
    Oid         umId;
    ForeignServer *srv;
    ObjectAddress address;
    RoleSpec   *role = (RoleSpec *) stmt->user;

    // Open the user mapping catalog
    rel = table_open(UserMappingRelationId, RowExclusiveLock);

    // Resolve user ID (handle PUBLIC role specially)
    if (role->roletype == ROLESPEC_PUBLIC)
        useId = ACL_ID_PUBLIC;
    else
        useId = get_rolespec_oid(stmt->user, false);

    // Get the foreign server
    srv = GetForeignServerByName(stmt->servername, false);

    // Find the existing user mapping
    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
                           ObjectIdGetDatum(useId), ObjectIdGetDatum(srv->serverid));
    if (!OidIsValid(umId))
        ereport(ERROR, "user mapping does not exist for server");

    // Check permissions to modify this user mapping
    user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

    // Get the current tuple
    tp = SearchSysCacheCopy1(USERMAPPINGOID, ObjectIdGetDatum(umId));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for user mapping");

    // Initialize replacement arrays
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Process new options if provided
    if (stmt->options)
    {
        ForeignDataWrapper *fdw;
        Datum       datum;
        bool        isnull;

        // Get the foreign data wrapper for validation
        fdw = GetForeignDataWrapper(srv->fdwid);

        // Get current options
        datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
                               Anum_pg_user_mapping_umoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        // Transform and validate the new options
        datum = transformGenericOptions(UserMappingRelationId, datum,
                                       stmt->options, fdw->fdwvalidator);

        // Set up replacement for options column
        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_user_mapping_umoptions - 1] = datum;
        else
            repl_null[Anum_pg_user_mapping_umoptions - 1] = true;

        repl_repl[Anum_pg_user_mapping_umoptions - 1] = true;
    }

    // Update the tuple with new options
    tp = heap_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    // Update the catalog
    CatalogTupleUpdate(rel, &tp->t_self, tp);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(UserMappingRelationId, umId, 0);

    // Set up return address
    ObjectAddressSet(address, UserMappingRelationId, umId);

    // Clean up
    heap_freetuple(tp);
    table_close(rel, RowExclusiveLock);

    return address;
}
```
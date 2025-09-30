# CreateUserMapping

## Location
[src/backend/commands/foreigncmds.c:1111-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L1111-L1236)

## Overview
Creates a new user mapping that defines authentication and connection information for a specific user to access a foreign server in PostgreSQL's foreign data wrapper system.

## Definition
```c
ObjectAddress CreateUserMapping(CreateUserMappingStmt *stmt)
```

## Detailed Description
This function implements the CREATE USER MAPPING SQL command by creating a new entry in the pg_user_mapping system catalog. It handles user authentication mapping for foreign data wrappers, allowing users to define connection credentials and options for accessing external data sources. The function performs comprehensive validation including uniqueness checks, permission verification, and proper dependency tracking. It supports both regular users and the special PUBLIC role, handles the IF NOT EXISTS clause, and validates user-provided options through the foreign data wrapper's validator function.

## Parameters / Member Variables
- `stmt`: Pointer to CreateUserMappingStmt structure containing the parsed CREATE USER MAPPING command details including user specification, server name, options, and conditional flags

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [get_rolespec_oid](../g/get_rolespec_oid.md)
  - [GetForeignServerByName](../G/GetForeignServerByName.md)
  - [user_mapping_ddl_aclcheck](../u/user_mapping_ddl_aclcheck.md)
  - GetSysCacheOid2
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function creates dependencies on both the foreign server and the mapped user to ensure proper cleanup when either is dropped. User mappings are intentionally not made members of extensions since roles themselves cannot be extension members. The function includes special handling for the PUBLIC role and supports conditional creation with IF NOT EXISTS to avoid duplicate mapping errors.

## Simplified Source

```c
ObjectAddress CreateUserMapping(CreateUserMappingStmt *stmt)
{
    Oid useId, umId;
    Datum useoptions, values[Natts_pg_user_mapping];
    bool nulls[Natts_pg_user_mapping];
    HeapTuple tuple;
    ObjectAddress myself, referenced;
    ForeignServer *srv;
    ForeignDataWrapper *fdw;
    RoleSpec *role = (RoleSpec *) stmt->user;

    Relation rel = table_open(UserMappingRelationId, RowExclusiveLock);

    // Determine user ID (PUBLIC role or specific user)
    if (role->roletype == ROLESPEC_PUBLIC)
        useId = ACL_ID_PUBLIC;
    else
        useId = get_rolespec_oid(stmt->user, false);

    // Validate foreign server exists
    srv = GetForeignServerByName(stmt->servername, false);

    // Check permissions for user mapping DDL
    user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

    // Check for existing user mapping
    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
                          ObjectIdGetDatum(useId), ObjectIdGetDatum(srv->serverid));

    if (OidIsValid(umId)) {
        if (stmt->if_not_exists) {
            ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("user mapping for \"%s\" already exists for server \"%s\", skipping",
                                  MappingUserName(useId), stmt->servername)));
            table_close(rel, RowExclusiveLock);
            return InvalidObjectAddress;
        }
        else {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("user mapping for \"%s\" already exists for server \"%s\"",
                                  MappingUserName(useId), stmt->servername)));
        }
    }

    // Get foreign data wrapper for option validation
    fdw = GetForeignDataWrapper(srv->fdwid);

    // Build tuple for pg_user_mapping
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    umId = GetNewOidWithIndex(rel, UserMappingOidIndexId, Anum_pg_user_mapping_oid);
    values[Anum_pg_user_mapping_oid - 1] = ObjectIdGetDatum(umId);
    values[Anum_pg_user_mapping_umuser - 1] = ObjectIdGetDatum(useId);
    values[Anum_pg_user_mapping_umserver - 1] = ObjectIdGetDatum(srv->serverid);

    // Process and validate user options through FDW validator
    useoptions = transformGenericOptions(UserMappingRelationId,
                                       PointerGetDatum(NULL),
                                       stmt->options,
                                       fdw->fdwvalidator);

    if (PointerIsValid(DatumGetPointer(useoptions)))
        values[Anum_pg_user_mapping_umoptions - 1] = useoptions;
    else
        nulls[Anum_pg_user_mapping_umoptions - 1] = true;

    // Insert tuple into catalog
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tuple);
    heap_freetuple(tuple);

    // Record dependencies
    myself.classId = UserMappingRelationId;
    myself.objectId = umId;
    myself.objectSubId = 0;

    // Dependency on foreign server
    referenced.classId = ForeignServerRelationId;
    referenced.objectId = srv->serverid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    // Dependency on mapped user (if not PUBLIC)
    if (OidIsValid(useId))
        recordDependencyOnOwner(UserMappingRelationId, umId, useId);

    // Post-creation hook
    InvokeObjectPostCreateHook(UserMappingRelationId, umId, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}
```
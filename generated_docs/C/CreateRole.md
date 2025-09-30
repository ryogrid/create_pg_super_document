# CreateRole

## Location
[src/backend/commands/user.c:132-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L132-L618)

## Overview
The main function that implements the CREATE ROLE SQL statement, creating new database roles with specified attributes and permissions.

## Definition
```c
Oid CreateRole(ParseState *pstate, CreateRoleStmt *stmt)
```

## Detailed Description
CreateRole is the core implementation function for the CREATE ROLE, CREATE USER, and CREATE GROUP SQL statements. It parses role creation options, validates permissions, creates a new role entry in the pg_authid system catalog, and sets up role memberships. The function handles all role attributes including superuser status, login privileges, password settings, connection limits, and role memberships. It enforces security constraints ensuring only authorized users can create roles with specific privileges, and implements automatic role administration grants for non-superuser creators.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parsing context and state information
- `stmt`: CreateRoleStmt structure containing the parsed CREATE ROLE statement with role name and options

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [has_createrole_privilege](../h/has_createrole_privilege.md)
  - [superuser_arg](../s/superuser_arg.md)
  - [have_createdb_privilege](../h/have_createdb_privilege.md)
  - [has_rolreplication](../h/has_rolreplication.md)
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [IsReservedName](../I/IsReservedName.md)
  - [get_role_oid](../g/get_role_oid.md)
  - [table_open](../t/table_open.md)
  - RelationGetDescr
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [AddRoleMems](../A/AddRoleMems.md)
  - InvokeObjectPostCreateHook
  - [encrypt_password](../e/encrypt_password.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Returns the OID of the newly created role
- Supports three statement types: ROLE, USER (with default LOGIN), and GROUP
- Validates that role names don't start with "pg_" (reserved namespace)
- Handles password encryption using the configured password encryption method
- Automatically grants admin privileges to non-superuser creators of roles
- Implements createrole_self_grant feature for automatic role inheritance
- Performs extensive privilege validation before allowing role creation
- Uses binary upgrade mode support for pg_upgrade operations

## Simplified Source

```c
Oid CreateRole(ParseState *pstate, CreateRoleStmt *stmt) {
    Relation pg_authid_rel;
    HeapTuple tuple;
    Datum new_record[Natts_pg_authid] = {0};
    bool new_record_nulls[Natts_pg_authid] = {0};
    Oid currentUserId = GetUserId();
    Oid roleid;

    // Default role attributes
    char *password = NULL;
    bool issuper = false;
    bool inherit = true;
    bool createrole = false;
    bool createdb = false;
    bool canlogin = false;
    bool isreplication = false;
    bool bypassrls = false;
    int connlimit = -1;
    char *validUntil = NULL;

    // Set defaults based on statement type
    switch (stmt->stmt_type) {
        case ROLESTMT_USER:
            canlogin = true;  // Users can login by default
            break;
        // ROLE and GROUP keep default values
    }

    // Parse options from the statement
    foreach(option, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "password") == 0)
            password = strVal(defel->arg);
        else if (strcmp(defel->defname, "superuser") == 0)
            issuper = boolVal(defel->arg);
        else if (strcmp(defel->defname, "inherit") == 0)
            inherit = boolVal(defel->arg);
        else if (strcmp(defel->defname, "createrole") == 0)
            createrole = boolVal(defel->arg);
        else if (strcmp(defel->defname, "createdb") == 0)
            createdb = boolVal(defel->arg);
        else if (strcmp(defel->defname, "canlogin") == 0)
            canlogin = boolVal(defel->arg);
        else if (strcmp(defel->defname, "isreplication") == 0)
            isreplication = boolVal(defel->arg);
        else if (strcmp(defel->defname, "bypassrls") == 0)
            bypassrls = boolVal(defel->arg);
        else if (strcmp(defel->defname, "connectionlimit") == 0)
            connlimit = intVal(defel->arg);
        else if (strcmp(defel->defname, "validUntil") == 0)
            validUntil = strVal(defel->arg);
        // Handle role membership options...
    }

    // Permission checks
    if (!superuser_arg(currentUserId)) {
        if (!has_createrole_privilege(currentUserId))
            ereport(ERROR, "permission denied to create role");
        if (issuper)
            ereport(ERROR, "only superusers can create superuser roles");
        if (createdb && !have_createdb_privilege())
            ereport(ERROR, "only users with CREATEDB can create CREATEDB roles");
        // Additional privilege checks...
    }

    // Validate role name (no "pg_" prefix)
    if (IsReservedName(stmt->role))
        ereport(ERROR, "role name \"%s\" is reserved", stmt->role);

    // Check for duplicate role
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    if (OidIsValid(get_role_oid(stmt->role, true)))
        ereport(ERROR, "role \"%s\" already exists", stmt->role);

    // Allocate new role OID
    roleid = GetNewOidWithIndex(pg_authid_rel, AuthIdOidIndexId, Anum_pg_authid_oid);

    // Build catalog tuple
    new_record[Anum_pg_authid_rolname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->role));
    new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper);
    new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit);
    new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole);
    new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb);
    new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin);
    new_record[Anum_pg_authid_rolreplication - 1] = BoolGetDatum(isreplication);
    new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);
    new_record[Anum_pg_authid_rolbypassrls - 1] = BoolGetDatum(bypassrls);
    new_record[Anum_pg_authid_oid - 1] = ObjectIdGetDatum(roleid);

    // Handle password encryption
    if (password) {
        if (password[0] == '\0') {
            // Empty password - clear it
            new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
        } else {
            char *shadow_pass = encrypt_password(Password_encryption, stmt->role, password);
            new_record[Anum_pg_authid_rolpassword - 1] = CStringGetTextDatum(shadow_pass);
        }
    } else {
        new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
    }

    // Insert new role into catalog
    tuple = heap_form_tuple(RelationGetDescr(pg_authid_rel), new_record, new_record_nulls);
    CatalogTupleInsert(pg_authid_rel, tuple);

    // Handle role memberships
    // ... (process addroleto, rolemembers, adminmembers)

    // Grant admin privileges to non-superuser creators
    if (!superuser()) {
        // Make creator an admin of the new role
        // ... (automatic role grants)
    }

    // Post-creation hook
    InvokeObjectPostCreateHook(AuthIdRelationId, roleid, 0);

    table_close(pg_authid_rel, NoLock);
    return roleid;
}
```
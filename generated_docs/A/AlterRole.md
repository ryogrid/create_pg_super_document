# AlterRole

## Location
[src/backend/commands/user.c:619-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L619-L999)

## Overview
The main function that implements the ALTER ROLE SQL statement, modifying existing database role attributes and membership.

## Definition
```c
Oid AlterRole(ParseState *pstate, AlterRoleStmt *stmt)
```

## Detailed Description
AlterRole implements the ALTER ROLE, ALTER USER, and ALTER GROUP SQL statements by modifying existing role entries in the pg_authid system catalog. The function validates permissions extensively, ensuring that only authorized users can modify role attributes. It supports changing all role attributes including superuser status, login privileges, password settings, connection limits, and role memberships. The function enforces complex privilege rules where superusers can modify other superusers, but non-superusers need both CREATEROLE privilege and ADMIN option on the target role to make most changes.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parsing context and state information
- `stmt`: AlterRoleStmt structure containing the parsed ALTER ROLE statement with role specification and options

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [check_rolespec_name](../c/check_rolespec_name.md)
  - [have_createrole_privilege](../h/have_createrole_privilege.md)
  - [is_admin_of_role](../i/is_admin_of_role.md)
  - [superuser](../s/superuser.md)
  - [have_createdb_privilege](../h/have_createdb_privilege.md)
  - [has_rolreplication](../h/has_rolreplication.md)
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [get_rolespec_tuple](../g/get_rolespec_tuple.md)
  - [table_open](../t/table_open.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [AddRoleMems](AddRoleMems.md)
  - [DelRoleMems](../D/DelRoleMems.md)
  - InvokeObjectPostAlterHook
  - [encrypt_password](../e/encrypt_password.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Returns the OID of the altered role
- Prevents modification of reserved roles (those starting with "pg_")
- Implements sophisticated privilege checking with different rules for superusers vs. regular users
- Supports backwards-compatible ALTER GROUP syntax through rolemembers option
- Users can always change their own password, but need additional privileges to change other users' passwords
- Bootstrap superuser cannot have superuser privilege revoked
- Validates connection limits and password formats
- Uses heap_modify_tuple for atomic updates to the catalog

## Simplified Source

```c
Oid AlterRole(ParseState *pstate, AlterRoleStmt *stmt) {
    char *password = NULL;
    int connlimit = -1;
    char *validUntil = NULL;
    DefElem *dpassword = NULL, *dissuper = NULL, *dinherit = NULL,
            *dcreaterole = NULL, *dcreatedb = NULL, *dcanlogin = NULL,
            *disreplication = NULL, *dconnlimit = NULL, *drolemembers = NULL,
            *dvalidUntil = NULL, *dbypassRLS = NULL;
    Oid roleid, currentUserId = GetUserId();

    check_rolespec_name(stmt->role, "Cannot alter reserved roles.");

    // Parse statement options
    foreach(option, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "password") == 0)
            dpassword = defel;
        else if (strcmp(defel->defname, "superuser") == 0)
            dissuper = defel;
        // ... other options
    }

    // Extract option values
    if (dpassword && dpassword->arg)
        password = strVal(dpassword->arg);
    if (dconnlimit)
        connlimit = intVal(dconnlimit->arg);

    // Find the role in pg_authid
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    tuple = get_rolespec_tuple(stmt->role);
    authform = (Form_pg_authid) GETSTRUCT(tuple);
    roleid = authform->oid;

    // Extensive permission checks
    if (!superuser() && authform->rolsuper)
        ereport(ERROR, "permission denied to alter role");

    if (!have_createrole_privilege() || !is_admin_of_role(GetUserId(), roleid)) {
        // Allow user to change own password
        if (dpassword && roleid != currentUserId)
            ereport(ERROR, "permission denied to alter role");
    }

    // Additional privilege checks for specific attributes
    if (!superuser()) {
        if (dcreatedb && !have_createdb_privilege())
            ereport(ERROR, "permission denied");
        if (disreplication && !has_rolreplication(currentUserId))
            ereport(ERROR, "permission denied");
        if (dbypassRLS && !has_bypassrls_privilege(currentUserId))
            ereport(ERROR, "permission denied");
    }

    // Call password checking hook if defined
    if (check_password_hook && password)
        (*check_password_hook)(rolename, password, ...);

    // Build updated tuple
    if (dissuper) {
        new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(should_be_super);
        new_record_repl[Anum_pg_authid_rolsuper - 1] = true;
    }
    // ... other attributes

    // Handle password encryption
    if (password) {
        shadow_pass = encrypt_password(Password_encryption, rolename, password);
        new_record[Anum_pg_authid_rolpassword - 1] = CStringGetTextDatum(shadow_pass);
        new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
    }

    // Update catalog
    new_tuple = heap_modify_tuple(tuple, pg_authid_dsc, new_record, nulls, replaces);
    CatalogTupleUpdate(pg_authid_rel, &tuple->t_self, new_tuple);

    // Handle role membership changes
    if (drolemembers) {
        if (stmt->action == +1)
            AddRoleMems(currentUserId, rolename, roleid, rolemembers, ...);
        else if (stmt->action == -1)
            DelRoleMems(currentUserId, rolename, roleid, rolemembers, ...);
    }

    InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);
    table_close(pg_authid_rel, NoLock);

    return roleid;
}
```
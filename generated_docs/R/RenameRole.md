# RenameRole

## Location
[src/backend/commands/user.c:1334-1479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1334-L1479)

## Overview
Changes the name of an existing PostgreSQL role while maintaining all its attributes and permissions, with appropriate security checks and validation.

## Definition

```c
struct the modified tuple */
	for (i = 0;
```
## Detailed Description
RenameRole is responsible for safely renaming a PostgreSQL role from one name to another. The function performs comprehensive validation to ensure the rename operation is secure and valid, including checking permissions, verifying that reserved names are not used, and handling special cases like MD5 password clearing. The function updates the pg_authid system catalog and ensures proper locking during the operation.

Key behaviors include:
- Prevents renaming of system roles and current session/effective users
- Enforces reserved name restrictions (pg_ prefix)
- Requires appropriate privileges (superuser for superuser roles, CREATEROLE + ADMIN for others)
- Automatically clears MD5 passwords since they use username as salt
- Maintains referential integrity through proper catalog updates

## Parameters / Member Variables
- : The current name of the role to be renamed
- : The desired new name for the role

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close: Access pg_authid catalog
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache: Cache-based role lookup
  - [GetSessionUserId](../G/GetSessionUserId.md)/GetOuterUserId: Current user validation
  - [IsReservedName](../I/IsReservedName.md): Reserved name checking
  - [superuser](../s/superuser.md)/have_createrole_privilege/is_admin_of_role: Permission validation
  - [get_password_type](../g/get_password_type.md): Password type detection for MD5 handling
  - [heap_modify_tuple](../h/heap_modify_tuple.md)/CatalogTupleUpdate: Catalog modification
  - InvokeObjectPostAlterHook: Post-operation hooks
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md): ALTER ROLE RENAME statement execution

## Notes and Other Information
- [Session](../S/Session.md) users and current effective users cannot be renamed to prevent application confusion
- MD5 passwords are automatically cleared during rename since they incorporate the username as salt
- The function maintains exclusive locks on pg_authid until transaction commit
- Regression testing enforces 'regress_' prefix convention when compiled with appropriate flags
- Superuser privileges are required to rename superuser roles; CREATEROLE + ADMIN privileges are required for regular roles

## Simplified Source

```c
ObjectAddress
RenameRole(const char *oldname, const char *newname)
{
    HeapTuple oldtuple, newtuple;
    Relation rel;
    Datum repl_val[Natts_pg_authid];
    bool repl_null[Natts_pg_authid];
    bool repl_repl[Natts_pg_authid];
    Oid roleid;
    ObjectAddress address;
    Form_pg_authid authform;

    // Open pg_authid catalog for modification
    rel = table_open(AuthIdRelationId, RowExclusiveLock);

    // Find the existing role
    oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(oldtuple))
        ereport(ERROR, "role does not exist");

    authform = (Form_pg_authid) GETSTRUCT(oldtuple);
    roleid = authform->oid;

    // Security checks - prevent renaming current session/effective users
    if (roleid == GetSessionUserId())
        ereport(ERROR, "session user cannot be renamed");
    if (roleid == GetOuterUserId())
        ereport(ERROR, "current user cannot be renamed");

    // Check reserved name restrictions
    if (IsReservedName(NameStr(authform->rolname)))
        ereport(ERROR, "role name is reserved (pg_ prefix)");
    if (IsReservedName(newname))
        ereport(ERROR, "new role name is reserved (pg_ prefix)");

    // Check if new name already exists
    if (SearchSysCacheExists1(AUTHNAME, CStringGetDatum(newname)))
        ereport(ERROR, "role with new name already exists");

    // Permission validation
    if (authform->rolsuper) {
        // Only superusers can rename superuser roles
        if (!superuser())
            ereport(ERROR, "permission denied - superuser required");
    } else {
        // Need CREATEROLE privilege and ADMIN option on the role
        if (!have_createrole_privilege() || !is_admin_of_role(GetUserId(), roleid))
            ereport(ERROR, "permission denied - CREATEROLE and ADMIN required");
    }

    // Prepare tuple modification - set new name
    memset(repl_repl, false, sizeof(repl_repl));
    repl_repl[Anum_pg_authid_rolname - 1] = true;
    repl_val[Anum_pg_authid_rolname - 1] = DirectFunctionCall1(namein, CStringGetDatum(newname));
    repl_null[Anum_pg_authid_rolname - 1] = false;

    // Handle MD5 password clearing (MD5 uses username as salt)
    Datum password_datum = heap_getattr(oldtuple, Anum_pg_authid_rolpassword, RelationGetDescr(rel), &isnull);
    if (!isnull && get_password_type(TextDatumGetCString(password_datum)) == PASSWORD_TYPE_MD5) {
        repl_repl[Anum_pg_authid_rolpassword - 1] = true;
        repl_null[Anum_pg_authid_rolpassword - 1] = true;
        ereport(NOTICE, "MD5 password cleared because of role rename");
    }

    // Update the catalog
    newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(rel, &oldtuple->t_self, newtuple);

    // Cleanup and return
    InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);
    ObjectAddressSet(address, AuthIdRelationId, roleid);
    ReleaseSysCache(oldtuple);
    table_close(rel, NoLock);

    return address;
}
```
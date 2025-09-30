# DropRole

## Location
[src/backend/commands/user.c:1090-1333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1090-L1333)

## Overview
The main function that implements the DROP ROLE SQL statement, removing database roles and cleaning up all associated dependencies and metadata.

## Definition
```c
void DropRole(DropRoleStmt *stmt)
```

## Detailed Description
DropRole implements the DROP ROLE, DROP USER, and DROP GROUP SQL statements by removing role entries from the pg_authid system catalog and cleaning up all associated dependencies. The function performs extensive validation to ensure only authorized users can drop roles, prevents dropping currently active roles, and handles dependency cleanup in a two-phase process. First, it removes pg_auth_members entries that can be silently removed, then checks for remaining dependencies that would prevent the drop operation. The function also cleans up role-related comments, security labels, and configuration settings.

## Parameters / Member Variables
- `stmt`: DropRoleStmt structure containing the parsed DROP ROLE statement with list of roles to drop and missing_ok flag

## Dependencies
- Functions called/Symbols referenced:
  - [have_createrole_privilege](../h/have_createrole_privilege.md)
  - [table_open](../t/table_open.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [GetUserId](../G/GetUserId.md)
  - [GetOuterUserId](../G/GetOuterUserId.md)
  - [GetSessionUserId](../G/GetSessionUserId.md)
  - [superuser](../s/superuser.md)
  - [is_admin_of_role](../i/is_admin_of_role.md)
  - InvokeObjectDropHook
  - [LockSharedObject](../L/LockSharedObject.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [checkSharedDependencies](../c/checkSharedDependencies.md)
  - [DeleteSharedComments](DeleteSharedComments.md)
  - [DeleteSharedSecurityLabel](DeleteSharedSecurityLabel.md)
  - [DropSetting](DropSetting.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Returns void (no return value)
- Supports IF EXISTS syntax through missing_ok flag for graceful handling of non-existent roles
- Prevents dropping the current user, outer user, or session user to avoid security issues
- Requires CREATEROLE privilege and ADMIN option on target roles
- Only superusers can drop other superuser roles
- Uses a two-phase dependency cleanup process to handle complex role membership scenarios
- Maintains exclusive locks on roles during the drop process to prevent concurrent modifications
- Automatically removes role memberships, comments, security labels, and configuration settings
- Uses AccessExclusiveLock to prevent other transactions from accessing the role during deletion

## Simplified Source

```c
void DropRole(DropRoleStmt *stmt) {
    Relation pg_authid_rel, pg_auth_members_rel;
    List *role_oids = NIL;

    // Check if user has permission to drop roles
    if (!have_createrole_privilege())
        ereport(ERROR, "permission denied to drop role");

    // Open system catalogs with exclusive locks
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    pg_auth_members_rel = table_open(AuthMemRelationId, RowExclusiveLock);

    // First pass: validate each role and collect role OIDs
    foreach(item, stmt->roles) {
        RoleSpec *rolspec = lfirst(item);
        char *role = rolspec->rolename;
        HeapTuple tuple;
        Form_pg_authid roleform;
        Oid roleid;

        // Look up role by name
        tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(role));
        if (!HeapTupleIsValid(tuple)) {
            if (stmt->missing_ok) {
                ereport(NOTICE, "role does not exist, skipping");
                continue;
            } else {
                ereport(ERROR, "role does not exist");
            }
        }

        roleform = (Form_pg_authid) GETSTRUCT(tuple);
        roleid = roleform->oid;

        // Safety checks: cannot drop current/session users
        if (roleid == GetUserId() || roleid == GetOuterUserId() ||
            roleid == GetSessionUserId())
            ereport(ERROR, "current/session user cannot be dropped");

        // Permission checks: only superusers can drop superuser roles
        if (roleform->rolsuper && !superuser())
            ereport(ERROR, "permission denied to drop superuser role");

        // Must have ADMIN option on the role
        if (!is_admin_of_role(GetUserId(), roleid))
            ereport(ERROR, "permission denied - need ADMIN option");

        // Invoke drop hook and lock the role
        InvokeObjectDropHook(AuthIdRelationId, roleid, 0);
        ReleaseSysCache(tuple);
        LockSharedObject(AuthIdRelationId, roleid, 0, AccessExclusiveLock);

        // Remove role membership entries (both as member and grantor)
        remove_role_memberships(pg_auth_members_rel, roleid);

        CommandCounterIncrement();
        role_oids = list_append_unique_oid(role_oids, roleid);
    }

    // Second pass: check dependencies and actually drop the roles
    foreach(item, role_oids) {
        Oid roleid = lfirst_oid(item);
        HeapTuple tuple;
        Form_pg_authid roleform;

        // Re-find the role tuple
        tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "could not find tuple for role");

        roleform = (Form_pg_authid) GETSTRUCT(tuple);

        // Check for remaining dependencies that prevent dropping
        if (checkSharedDependencies(AuthIdRelationId, roleid, &detail, &detail_log))
            ereport(ERROR, "role cannot be dropped because objects depend on it");

        // Remove the role from pg_authid
        CatalogTupleDelete(pg_authid_rel, &tuple->t_self);
        ReleaseSysCache(tuple);

        // Clean up associated metadata
        DeleteSharedComments(roleid, AuthIdRelationId);
        DeleteSharedSecurityLabel(roleid, AuthIdRelationId);
        DropSetting(InvalidOid, roleid);
    }

    // Close relations (keep locks until commit)
    table_close(pg_auth_members_rel, NoLock);
    table_close(pg_authid_rel, NoLock);
}

// Helper function for membership cleanup (conceptual)
static void remove_role_memberships(Relation pg_auth_members_rel, Oid roleid) {
    ScanKeyData scankey;
    SysScanDesc sscan;
    HeapTuple tuple;

    // Remove entries where this role is the roleid
    ScanKeyInit(&scankey, Anum_pg_auth_members_roleid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));
    sscan = systable_beginscan(pg_auth_members_rel, AuthMemRoleMemIndexId,
                              true, NULL, 1, &scankey);
    while (HeapTupleIsValid(tuple = systable_getnext(sscan))) {
        Form_pg_auth_members authmem_form = (Form_pg_auth_members) GETSTRUCT(tuple);
        deleteSharedDependencyRecordsFor(AuthMemRelationId, authmem_form->oid, 0);
        CatalogTupleDelete(pg_auth_members_rel, &tuple->t_self);
    }
    systable_endscan(sscan);

    // Remove entries where this role is the member
    ScanKeyInit(&scankey, Anum_pg_auth_members_member,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));
    sscan = systable_beginscan(pg_auth_members_rel, AuthMemMemRoleIndexId,
                              true, NULL, 1, &scankey);
    while (HeapTupleIsValid(tuple = systable_getnext(sscan))) {
        Form_pg_auth_members authmem_form = (Form_pg_auth_members) GETSTRUCT(tuple);
        deleteSharedDependencyRecordsFor(AuthMemRelationId, authmem_form->oid, 0);
        CatalogTupleDelete(pg_auth_members_rel, &tuple->t_self);
    }
    systable_endscan(sscan);
}
```
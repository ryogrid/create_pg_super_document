# AddRoleMems

## Location
[src/backend/commands/user.c:1681-1977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1681-L1977)

## Overview
AddRoleMems adds specified member roles to a target role with various grant options, handling membership validation, circular dependency checks, and catalog updates.

## Definition

```c
static void
AddRoleMems(Oid currentUserId, const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			Oid grantorId, GrantRoleOptions *popt)
```
## Detailed Description
AddRoleMems is a core function in PostgreSQL's role management system that implements the GRANT ROLE functionality. It performs comprehensive validation to prevent membership loops and circular admin option grants, then updates the pg_auth_members catalog table. The function validates that pg_database_owner cannot be a member of any role and ensures that granting admin options doesn't create circular dependencies where a grantor could lose their ability to perform the grant.

The function operates in several phases:
1. Validates the grantor using check_role_grantor
2. Acquires proper locking to prevent race conditions
3. Performs sanity checks including membership loop detection
4. Checks for circular admin option grants when applicable
5. Updates or inserts records in pg_auth_members catalog
6. Manages ACL dependencies for new memberships

## Parameters / Member Variables
- : OID of the role performing the operation (used for authorization checks)
- : Name of the target role to add members to (used only for error messages)
- : OID of the target role to add members to
- : List of RoleSpec structures for the roles to add (used for error messages)
- : List of OIDs for the roles to add as members
- : OID that should be recorded as having granted the membership (InvalidOid if not explicitly set)
- : GrantRoleOptions structure containing information about grant options (admin, inherit, set)

## Dependencies
- Functions called/Symbols referenced:
  - [check_role_grantor](../c/check_role_grantor.md)
  - [LockSharedObject](../L/LockSharedObject.md)
  - [is_member_of_role_nosuper](../i/is_member_of_role_nosuper.md)
  - [initialize_revoke_actions](../i/initialize_revoke_actions.md)
  - [plan_member_revoke](../p/plan_member_revoke.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [CreateRole](../C/CreateRole.md)
  - [AlterRole](AlterRole.md)
  - [GrantRole](../G/GrantRole.md)

## Notes and Other Information
- Uses ShareUpdateExclusiveLock on the target role to prevent concurrent modifications
- Implements sophisticated circular dependency detection for both membership loops and admin option chains
- Supports updating existing memberships with new options rather than creating duplicates
- Issues NOTICE when attempting to grant already-existing identical memberships
- Maintains referential integrity through ACL dependency tracking
- Uses CommandCounterIncrement after each change to handle potential duplicates in the member list

## Simplified Source

```c
static void
AddRoleMems(Oid currentUserId, const char *rolename, Oid roleid,
            List *memberSpecs, List *memberIds,
            Oid grantorId, GrantRoleOptions *popt)
{
    Relation    pg_authmem_rel;
    TupleDesc   pg_authmem_dsc;
    ListCell   *specitem;
    ListCell   *iditem;

    // Validate grantor and resolve if needed
    grantorId = check_role_grantor(currentUserId, roleid, grantorId, true);

    // Open the authorization membership catalog
    pg_authmem_rel = table_open(AuthMemRelationId, RowExclusiveLock);
    pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

    // Lock the role to prevent concurrent changes
    LockSharedObject(AuthIdRelationId, roleid, 0, ShareUpdateExclusiveLock);

    // Check each member for validity
    forboth(specitem, memberSpecs, iditem, memberIds)
    {
        RoleSpec   *memberRole = lfirst_node(RoleSpec, specitem);
        Oid         memberid = lfirst_oid(iditem);

        // Prevent pg_database_owner from being a member
        if (memberid == ROLE_PG_DATABASE_OWNER)
            ereport(ERROR, errmsg("role \"%s\" cannot be a member of any role",
                                  get_rolespec_name(memberRole)));

        // Prevent membership loops
        if (is_member_of_role_nosuper(roleid, memberid))
            ereport(ERROR, errmsg("role \"%s\" is a member of role \"%s\"",
                                  rolename, get_rolespec_name(memberRole)));
    }

    // Check for circular admin option grants if granting admin
    if (popt->admin && grantorId != BOOTSTRAP_SUPERUSERID)
    {
        // Complex circularity check logic simplified
        // Ensures grantor retains admin ability after grant
        // ... (detailed validation omitted for brevity)
    }

    // Process each member addition
    forboth(specitem, memberSpecs, iditem, memberIds)
    {
        RoleSpec   *memberRole = lfirst_node(RoleSpec, specitem);
        Oid         memberid = lfirst_oid(iditem);
        HeapTuple   authmem_tuple;

        // Check if membership already exists
        authmem_tuple = SearchSysCache3(AUTHMEMROLEMEM,
                                        ObjectIdGetDatum(roleid),
                                        ObjectIdGetDatum(memberid),
                                        ObjectIdGetDatum(grantorId));

        if (HeapTupleIsValid(authmem_tuple))
        {
            // Update existing membership with new options
            // Only change if options differ
            // ... (update logic)
        }
        else
        {
            // Create new membership record
            // Set default inherit option based on member role if not specified
            // Insert new tuple with all options
            // Update ACL dependencies
        }

        CommandCounterIncrement();
    }

    // Close catalog (keep lock until commit)
    table_close(pg_authmem_rel, NoLock);
}
```
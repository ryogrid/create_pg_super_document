# AddRoleMems

## Location
src/backend/commands/user.c: 1681 - 1977

## Overview
AddRoleMems adds specified member roles to a target role with various grant options, handling membership validation, circular dependency checks, and catalog updates.

## Definition


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
  - is_member_of_role_nosuper
  - [initialize_revoke_actions](../i/initialize_revoke_actions.md)
  - plan_member_revoke
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - CommandCounterIncrement
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
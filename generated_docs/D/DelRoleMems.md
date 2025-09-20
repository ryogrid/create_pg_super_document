# DelRoleMems

## Location
[src/backend/commands/user.c:1978-2109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1978-L2109)

## Overview
DelRoleMems removes specified member roles from a target role, handling cascade/restrict behavior for dependent privileges and updating the pg_auth_members catalog appropriately.

## Definition

```c
static void
DelRoleMems(Oid currentUserId, const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			Oid grantorId, GrantRoleOptions *popt, DropBehavior behavior)
```
## Detailed Description
DelRoleMems implements the REVOKE ROLE functionality in PostgreSQL's role management system. It removes role memberships while respecting dependency chains and cascade/restrict semantics. The function works by first planning all necessary revoke actions using initialize_revoke_actions and plan_single_revoke, then executing those actions on the pg_auth_members catalog.

The function can handle partial revocations (removing only specific grant options like admin, inherit, or set) or complete membership removal. When CASCADE behavior is specified, it will recursively remove dependent privileges. With RESTRICT behavior, it will refuse to proceed if dependencies exist.

The operation phases include:
1. Validating the grantor and obtaining proper locks
2. Planning revoke actions for all affected memberships
3. Executing the planned actions (delete tuples or update specific options)
4. Managing catalog dependencies appropriately

## Parameters / Member Variables
- : OID of the role performing the revoke operation
- : Name of the target role to remove members from (used for error messages)
- : OID of the target role to remove members from
- : List of RoleSpec structures for roles to remove (used for error messages)
- : List of OIDs for roles to remove as members
- : OID of the role that originally granted the membership being revoked
- : GrantRoleOptions structure specifying which options to revoke
- : DropBehavior (CASCADE or RESTRICT) for handling dependent privileges

## Dependencies
- Functions called/Symbols referenced:
  - [check_role_grantor](../c/check_role_grantor.md)
  - [LockSharedObject](../L/LockSharedObject.md)
  - SearchSysCacheList1
  - [initialize_revoke_actions](../i/initialize_revoke_actions.md)
  - plan_single_revoke
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [AlterRole](../A/AlterRole.md)
  - [GrantRole](../G/GrantRole.md)

## Notes and Other Information
- Uses ShareUpdateExclusiveLock to prevent concurrent modifications to the same role
- Issues WARNING when attempting to revoke non-existent memberships
- Supports partial revocation of specific grant options rather than complete membership removal
- Handles both complete tuple deletion and selective option removal through different RevokeRoleGrantAction values
- Maintains referential integrity by removing shared dependency records when deleting membership grants
- Plans all actions before execution to ensure consistency when handling multiple members
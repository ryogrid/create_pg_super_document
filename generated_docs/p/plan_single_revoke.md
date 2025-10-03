# plan_single_revoke

## Location
[src/backend/commands/user.c:2319-2388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L2319-L2388)

## Overview
Determines the specific actions needed to revoke a role grant or admin option, handling dependent privileges and checking for conflicts in the grant hierarchy.

## Definition

```c
static bool
plan_single_revoke(CatCList *memlist, RevokeRoleGrantAction *actions,
				   Oid member, Oid grantor, GrantRoleOptions *popt,
				   DropBehavior behavior)
```
## Detailed Description
This function analyzes what actions are required to revoke a specific role grant based on the options specified. It searches through the membership list to find the matching grant (by member and grantor) and determines the appropriate revocation action. The function handles different types of revocations:

- Revoking specific options (INHERIT, SET) which don't affect dependent privileges
- Revoking the entire grant or ADMIN option, which may require recursive handling of dependent grants
- Enforcing DROP_RESTRICT or DROP_CASCADE behavior for dependent privileges

The function updates the actions array to indicate what type of revocation should be performed for the matched grant.

## Parameters / Member Variables
- `*memlist`: CatCList containing all role membership grants for the target role
- `*actions`: Array of RevokeRoleGrantAction values to be updated with planned actions
- `member`: OID of the role member whose grant is being revoked
- `grantor`: OID of the role that granted the membership
- `*popt`: GrantRoleOptions structure specifying which aspects of the grant to revoke
- `behavior`: DropBehavior indicating whether to use RESTRICT or CASCADE for dependent grants
## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount32](pg_popcount32.md)
  - [plan_recursive_revoke](plan_recursive_revoke.md)
  - GETSTRUCT
- Types used:
  - [RevokeRoleGrantAction](../R/RevokeRoleGrantAction.md)
  - [CatCList](../C/CatCList.md)
  - [GrantRoleOptions](../G/GrantRoleOptions.md)
  - DropBehavior
  - Form_pg_auth_members
- Constants:
  - GRANT_ROLE_SPECIFIED_INHERIT
  - GRANT_ROLE_SPECIFIED_SET
  - GRANT_ROLE_SPECIFIED_ADMIN
  - RRG_REMOVE_INHERIT_OPTION
  - RRG_REMOVE_SET_OPTION
- Called from:
  - [DelRoleMems](../D/DelRoleMems.md)

## Notes and Other Information
- The function asserts that at most one option bit is set in popt->specified, as the current syntax doesn't support revoking multiple options simultaneously
- INHERIT and SET option revocations don't require recursive processing since they don't affect dependent privileges
- When revoking the entire grant or just the ADMIN option, recursive planning is needed to handle dependent grants properly
- Returns true if the matching grant was found in the membership list, false otherwise
- The function is static and only used within the user.c module for role management operations

## Simplified Source

```c
static bool plan_single_revoke(CatCList *memlist, RevokeRoleGrantAction *actions,
                               Oid member, Oid grantor, GrantRoleOptions *popt,
                               DropBehavior behavior) {
    // Ensure only one option bit is set
    Assert(pg_popcount32(popt->specified) <= 1);

    // Search for matching grant in membership list
    for (int i = 0; i < memlist->n_members; ++i) {
        HeapTuple authmem_tuple = &memlist->members[i]->tuple;
        Form_pg_auth_members authmem_form = (Form_pg_auth_members) GETSTRUCT(authmem_tuple);

        // Check if this is the target grant
        if (authmem_form->member == member && authmem_form->grantor == grantor) {

            if (popt->specified & GRANT_ROLE_SPECIFIED_INHERIT) {
                // Revoke INHERIT option - no dependent privileges affected
                actions[i] = RRG_REMOVE_INHERIT_OPTION;
            }
            else if (popt->specified & GRANT_ROLE_SPECIFIED_SET) {
                // Revoke SET option - no dependent privileges affected
                actions[i] = RRG_REMOVE_SET_OPTION;
            }
            else {
                // Revoke entire grant or ADMIN option - handle dependencies
                bool revoke_admin_only = (popt->specified & GRANT_ROLE_SPECIFIED_ADMIN) != 0;
                plan_recursive_revoke(memlist, actions, i, revoke_admin_only, behavior);
            }
            return true;
        }
    }

    return false; // Grant not found
}
```
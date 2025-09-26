# plan_member_revoke

## Location
[src/backend/commands/user.c:2389-2412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L2389-L2412)

## Overview
Plans the revocation of all role grants for a specific member, handling dependent privileges by cascading the revocation to all related grants.

## Definition
```c
static void plan_member_revoke(CatCList *memlist, RevokeRoleGrantAction *actions, Oid member)
```

## Detailed Description
This function identifies all role membership grants for a specified member and plans their complete revocation. Unlike plan_single_revoke which handles individual grants, this function operates on all grants for a given member role. It iterates through the membership list and for each grant where the specified OID is the member, it calls plan_recursive_revoke to handle the cascading deletion of dependent privileges.

The function always uses DROP_CASCADE behavior and revokes entire grants (not just admin options), ensuring that all privileges granted to the member are completely removed along with any dependent grants.

## Parameters / Member Variables
- `memlist`: CatCList containing all role membership grants for the target role
- `actions`: Array of RevokeRoleGrantAction values to be updated with planned revocation actions
- `member`: OID of the role member whose grants should be revoked

## Dependencies
- Functions called/Symbols referenced:
  - [plan_recursive_revoke](plan_recursive_revoke.md)
  - GETSTRUCT
- Types used:
  - [CatCList](../C/CatCList.md)
  - [RevokeRoleGrantAction](../R/RevokeRoleGrantAction.md)
  - Form_pg_auth_members
  - HeapTuple
- Constants:
  - DROP_CASCADE
- Called from:
  - [AddRoleMems](../A/AddRoleMems.md)

## Notes and Other Information
- This function is used when completely removing a member from role relationships, typically during role deletion or major role restructuring
- Always uses DROP_CASCADE behavior, meaning dependent grants will be automatically removed without user confirmation
- The function iterates through all membership entries and processes each one where the specified OID appears as a member
- Used in conjunction with AddRoleMems to ensure clean role membership management
- The function is static and only accessible within the user.c module
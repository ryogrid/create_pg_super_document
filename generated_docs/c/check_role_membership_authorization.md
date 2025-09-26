# check_role_membership_authorization

## Location
[src/backend/commands/user.c:2110-2202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L2110-L2202)

## Overview
check_role_membership_authorization validates that the current user has sufficient privileges to modify the membership list of a target role, enforcing superuser and admin option requirements.

## Definition

```c
static void
check_role_membership_authorization(Oid currentUserId, Oid roleid,
									bool is_grant)
```
## Detailed Description
This function implements authorization checks for role membership operations (GRANT ROLE and REVOKE ROLE). It enforces PostgreSQL's security model by ensuring that only properly authorized users can modify role memberships. The function implements a hierarchical permission model where superuser privileges are required to modify superuser roles, and admin option is required for non-superuser roles.

The function also enforces special restrictions on pg_database_owner, preventing it from having explicit members to maintain its charter as a role with exactly one implicit, situation-dependent member. This design decision maintains security boundaries around database ownership.

Key authorization rules enforced:
1. pg_database_owner cannot have explicit members
2. Only superusers can grant/revoke superuser roles
3. Non-superuser roles require admin option on the target role
4. Different error messages for grant vs revoke operations

## Parameters / Member Variables
- : OID of the role attempting to perform the membership operation
- : OID of the target role whose membership is being modified
- : Boolean flag indicating whether this is a grant (true) or revoke (false) operation

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
  - [superuser_arg](../s/superuser_arg.md)
  - [is_admin_of_role](../i/is_admin_of_role.md)
- Called from (representative examples):
  - [CreateRole](../C/CreateRole.md)
  - [GrantRole](../G/GrantRole.md)

## Notes and Other Information
- Throws ERROR with appropriate SQLSTATE codes when authorization fails
- Provides detailed error messages distinguishing between grant and revoke operations
- Special handling for pg_database_owner reflects its unique role in the system
- The function is purely for authorization - it doesn't modify any catalog state
- Uses is_admin_of_role() which respects the admin option hierarchy
- Error messages include both the operation type and the specific privilege requirements
# member_can_set_role

## Location
[src/backend/utils/adt/acl.c:5185-5207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5185-L5207)

## Overview
Determines whether a member role can use SET ROLE to assume the identity and privileges of a target role.

## Definition
```c
bool
member_can_set_role(Oid member, Oid role)
```

## Detailed Description
This function checks if a member role has the ability to use the SET ROLE command to become another role. It requires a chain of role grants from the member to the target role where each grant has set_option = true.

Unlike has_privs_of_role(), this function is specifically about SET ROLE capability and doesn't care about privilege inheritance (inherit_option). A role can have SET ROLE permission without inheriting privileges, and vice versa.

The function is used in several contexts:
1. Validating SET ROLE commands during session role changes
2. Checking permissions for object ownership changes
3. Determining if a user can create objects owned by another role

## Parameters / Member Variables
- `member`: The OID of the role that wants to perform SET ROLE
- `role`: The OID of the target role to potentially assume

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - [roles_is_member_of](../r/roles_is_member_of.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [ROLERECURSE_SETROLE](../R/ROLERECURSE_SETROLE.md) (constant)
- Called from:
  - [check_role](../c/check_role.md)
  - [pg_role_aclcheck](../p/pg_role_aclcheck.md)
  - [check_can_set_role](../c/check_can_set_role.md)
  - [SwitchToUntrustedUser](../S/SwitchToUntrustedUser.md)

## Notes and Other Information
- Uses ROLERECURSE_SETROLE to traverse only grants with set_option enabled
- Independent of privilege inheritance - focuses solely on SET ROLE capability
- Superusers can always SET ROLE to any role regardless of explicit grants
- Forms the basis for PostgreSQL's role-switching security model
- Complementary to has_privs_of_role() which handles privilege inheritance separately
- Essential for object ownership validation and session security controls

## Simplified Source

```c
bool
member_can_set_role(Oid member, Oid role)
{
    // Same role can always set to itself
    if (member == role)
        return true;

    // Superusers can set to any role
    if (superuser_arg(member))
        return true;

    // Check if member has SET ROLE privileges through role chain
    // Get all roles accessible via SET ROLE and check if target is among them
    return list_member_oid(roles_is_member_of(member, ROLERECURSE_SETROLE,
                                            InvalidOid, NULL),
                         role);
}
```
# check_role_grantor

## Location
[src/backend/commands/user.c:2203-2287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L2203-L2287)

## Overview
check_role_grantor validates and infers the grantor for role membership operations, ensuring only authorized roles with admin option can be recorded as grantors.

## Definition

```c
structs an array indicating that no actions are to be performed;
```
## Detailed Description
This function implements the grantor validation and inference logic for PostgreSQL's role membership system. It ensures that the grantor recorded for a role membership grant has the necessary admin option privileges on the target role. The function serves dual purposes: validating explicitly specified grantors and automatically selecting appropriate grantors when none is specified.

The function implements PostgreSQL's grantor hierarchy rules:
1. The bootstrap superuser can always be the grantor
2. Regular roles must have admin option on the target role
3. When inferring grantors, superusers default to bootstrap superuser
4. For non-superusers, the function selects the "best" admin role (fewest inheritance hops)

For explicit grantors, the function enforces integrity constraints:
- The current user must have privileges of the specified grantor role
- The grantor must have admin option on the target role (for grants)
- These constraints apply even to superusers to maintain grant chain integrity

## Parameters / Member Variables
- : OID of the role performing the operation
- : OID of the target role whose membership is being granted/revoked
- : OID of the proposed grantor (InvalidOid if not specified)
- : Boolean indicating whether this is a grant (true) or revoke (false) operation

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - [select_best_admin](../s/select_best_admin.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
- Called from (representative examples):
  - [AddRoleMems](../A/AddRoleMems.md)
  - [DelRoleMems](../D/DelRoleMems.md)

## Notes and Other Information
- Returns the OID of the validated/inferred grantor to be used in the operation
- For superusers without explicit grantor, always prefers BOOTSTRAP_SUPERUSERID to minimize grant dependencies
- Uses select_best_admin() to find the most direct admin relationship when inferring grantors
- Enforces stricter validation for grants than revokes (allows cleanup of invalid existing grants)
- The function maintains the integrity of the grant chain structure essential for CASCADE revokes
- Error messages distinguish between grant and revoke operations for better user experience

## Simplified Source

```c
static Oid check_role_grantor(Oid currentUserId, Oid roleid, Oid grantorId, bool is_grant) {
    // If no grantor specified, infer one
    if (!OidIsValid(grantorId)) {
        // Superusers default to bootstrap superuser for grant independence
        if (superuser_arg(currentUserId))
            return BOOTSTRAP_SUPERUSERID;

        // Find best admin role for the target role
        grantorId = select_best_admin(currentUserId, roleid);
        if (!OidIsValid(grantorId))
            elog(ERROR, "no possible grantors");
        return grantorId;
    }

    // Validate explicit grantor
    if (is_grant) {
        // Check current user has privileges of grantor role
        if (!has_privs_of_role(currentUserId, grantorId))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("permission denied to grant privileges as role \"%s\"",
                                  GetUserNameFromId(grantorId, false))));

        // Verify grantor has admin option on target role
        if (grantorId != BOOTSTRAP_SUPERUSERID &&
            select_best_admin(grantorId, roleid) != grantorId)
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("permission denied to grant privileges as role \"%s\"",
                                  GetUserNameFromId(grantorId, false))));
    } else {
        // For revokes, only check role privilege inheritance
        if (!has_privs_of_role(currentUserId, grantorId))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("permission denied to revoke privileges granted by role \"%s\"",
                                  GetUserNameFromId(grantorId, false))));
    }

    return grantorId;
}
```
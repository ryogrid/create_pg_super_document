# GrantRole

## Location
[src/backend/commands/user.c:1480-1582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1480-L1582)

## Overview
Processes GRANT ROLE and REVOKE ROLE statements by managing role membership grants between roles with configurable options for admin, inherit, and set privileges.

## Definition

```c
void
GrantRole(ParseState *pstate, GrantRoleStmt *stmt)
```
## Detailed Description
GrantRole implements the core logic for PostgreSQL's GRANT ROLE and REVOKE ROLE commands, which manage role membership relationships. The function parses role grant options (admin, inherit, set), validates permissions, and delegates to AddRoleMems or DelRoleMems to perform the actual catalog modifications in pg_auth_members.

Key responsibilities include:
- Parsing and validating role grant options (admin, inherit, set privileges)
- Resolving role specifications to OIDs for grantees and grantor
- Enforcing authorization checks through check_role_membership_authorization
- Coordinating with AddRoleMems for grants and DelRoleMems for revocations
- Maintaining proper locking on the pg_authid catalog during operations

The function supports flexible role membership management with granular control over inheritance and administrative privileges.

## Parameters / Member Variables
- : Parse state for error reporting and location tracking
- : GrantRoleStmt containing the complete grant/revoke specification including roles, options, and grantor information

## Dependencies
- Functions called/Symbols referenced:
  - InitGrantRoleOptions: Initialize role grant options structure
  - [defGetString](../d/defGetString.md)/parse_bool: Parse statement options
  - get_rolespec_oid/get_role_oid: Role name to OID resolution
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Convert role specifications to OID list
  - [check_role_membership_authorization](../c/check_role_membership_authorization.md): Permission validation
  - [AddRoleMems](../A/AddRoleMems.md): Add role memberships (for grants)
  - [DelRoleMems](../D/DelRoleMems.md): Remove role memberships (for revocations)
  - table_open/table_close: pg_authid catalog access
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command processing

## Notes and Other Information
- Supports three role grant options: admin (administrative rights), inherit (privilege inheritance), and set (ability to SET ROLE)
- The grantor can be explicitly specified or defaults to the current user
- Uses AccessShareLock on pg_authid since it doesn't directly modify that catalog (modifications go through pg_auth_members)
- Validates that column specifications are not used with role grants (invalid syntax)
- Role membership changes are committed atomically as part of the surrounding transaction
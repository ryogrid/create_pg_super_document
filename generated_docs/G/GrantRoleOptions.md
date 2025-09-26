# GrantRoleOptions

## Location
[src/backend/commands/user.c:78-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L78-L79)

## Overview
A structure that encapsulates the various options that can be specified when granting roles, including admin, inherit, and set privileges along with flags indicating which options were explicitly specified.

## Definition

```c
typedef struct
{
	unsigned	specified;
	bool		admin;
	bool		inherit;
	bool		set;
} GrantRoleOptions;
```
## Detailed Description
This structure is used throughout PostgreSQL's role management system to track the options associated with role grants. It maintains both the actual boolean values for each grant option (admin, inherit, set) and a bitmask indicating which options were explicitly specified by the user. This distinction is important because unspecified options may have different default behaviors compared to explicitly set options.

## Parameters / Member Variables
- `specified`: A bitmask indicating which options were explicitly specified using flags like GRANT_ROLE_SPECIFIED_ADMIN, GRANT_ROLE_SPECIFIED_INHERIT, and GRANT_ROLE_SPECIFIED_SET
- `admin`: Boolean indicating whether the grantee has admin option (can grant the role to others)
- `inherit`: Boolean indicating whether the grantee inherits the privileges of the granted role
- `set`: Boolean indicating whether the grantee can set the role (switch to it)

## Dependencies
- Functions called/Symbols referenced:
  - GRANT_ROLE_SPECIFIED_ADMIN (0x0001)
  - GRANT_ROLE_SPECIFIED_INHERIT (0x0002) 
  - GRANT_ROLE_SPECIFIED_SET (0x0004)
- Called from (representative examples):
  - [CreateRole](../C/CreateRole.md) (src/backend/commands/user.c:171, 543)
  - [AlterRole](../A/AlterRole.md) (src/backend/commands/user.c:649)
  - [GrantRole](GrantRole.md) (src/backend/commands/user.c:1486)
  - [AddRoleMems](../A/AddRoleMems.md) (src/backend/commands/user.c:1683)
  - [DelRoleMems](../D/DelRoleMems.md) (src/backend/commands/user.c:1980)
  - [plan_single_revoke](../p/plan_single_revoke.md) (src/backend/commands/user.c:2320)
  - [InitGrantRoleOptions](../I/InitGrantRoleOptions.md) (src/backend/commands/user.c:2503)

## Notes and Other Information
This structure is essential for maintaining the granular control over role privileges in PostgreSQL. The `specified` field allows the system to distinguish between options that were explicitly set by the user versus those using default values, which is crucial for proper role inheritance and permission management. The structure is used both for creating new role grants and for modifying existing ones.
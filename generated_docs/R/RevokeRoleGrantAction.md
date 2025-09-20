# RevokeRoleGrantAction

## Location
[src/backend/commands/user.c:67-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L67-L77)

## Overview
An enumeration that defines the possible actions that can be taken when revoking role grants, including specific options or the entire grant itself.

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
This enumeration is used internally by PostgreSQL's role management system to determine what action needs to be taken when revoking role grants or specific grant options. It supports the cascading revocation logic where removing a role grant or admin option might require recursive changes to dependent grants. The enum provides fine-grained control over which aspects of a role grant should be modified or removed entirely.

## Parameters / Member Variables
- : Indicates a grant that would not need to be altered by the revocation operation
- : Indicates a grant that would need to have admin_option set to false by the operation
- : Indicates a grant that would need to have inherit option set to false
- : Indicates a grant that would need to have set option set to false  
- : Indicates a grant that would need to be removed entirely by the operation

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no direct function calls)
- Called from (representative examples):
  - [AddRoleMems](../A/AddRoleMems.md) (src/backend/commands/user.c:1767)
  - [DelRoleMems](../D/DelRoleMems.md) (src/backend/commands/user.c:1987)
  - [check_role_grantor](../c/check_role_grantor.md) (src/backend/commands/user.c:2287)
  - [initialize_revoke_actions](../i/initialize_revoke_actions.md) (src/backend/commands/user.c:2290)
  - plan_single_revoke (src/backend/commands/user.c:2319)
  - plan_member_revoke (src/backend/commands/user.c:2389)
  - plan_recursive_revoke (src/backend/commands/user.c:2413)

## Notes and Other Information
This enumeration is central to PostgreSQL's role grant revocation logic, particularly for handling cascading revocations where removing a grant or admin option from one role might require changes to dependent grants. The different enum values allow the system to precisely specify what type of modification is needed for each affected grant during a revocation operation.
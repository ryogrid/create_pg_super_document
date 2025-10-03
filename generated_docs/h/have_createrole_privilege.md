# have_createrole_privilege

## Location
[src/backend/commands/user.c:122-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L122-L131)

## Overview
A utility function that checks whether the current user has the CREATEROLE privilege required to manage database roles.

## Definition

```c
static bool
have_createrole_privilege(void)
```
## Detailed Description
This is a simple wrapper function that determines if the currently connected user has the CREATEROLE privilege. It serves as a convenience function used throughout the role management subsystem to enforce access control. The function internally calls  with the current user's ID obtained via . This privilege check is essential for operations like creating, altering, or dropping database roles, ensuring that only authorized users can perform these administrative tasks.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [has_createrole_privilege](has_createrole_privilege.md)
  - [GetUserId](../G/GetUserId.md)
- Called from (representative examples):
  - [AlterRole](../A/AlterRole.md)
  - [AlterRoleSet](../A/AlterRoleSet.md)
  - [DropRole](../D/DropRole.md)
  - [RenameRole](../R/RenameRole.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the user.c source file
- The function is a simple one-liner that abstracts the privilege checking logic
- CREATEROLE is one of the fundamental superuser-like privileges in PostgreSQL
- Used as a security gate before allowing role management operations

## Simplified Source

```c
static bool
have_createrole_privilege(void)
{
    return has_createrole_privilege(GetUserId());
}
```
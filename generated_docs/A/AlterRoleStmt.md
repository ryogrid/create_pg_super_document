# AlterRoleStmt

## Location
[src/include/nodes/parsenodes.h:3089-3095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3089-L3095)

## Overview
AlterRoleStmt is a parse tree node structure that represents ALTER ROLE SQL statements used to modify existing database roles and their properties or memberships.

## Definition

```c
typedef struct AlterRoleStmt
{
	NodeTag		type;
	RoleSpec   *role;			/* role */
	List	   *options;		/* List of DefElem nodes */
	int			action;			/* +1 = add members, -1 = drop members */
} AlterRoleStmt;
```
## Detailed Description
AlterRoleStmt is a parser node structure that encapsulates information needed to alter existing database roles in PostgreSQL. This structure handles various ALTER ROLE operations including modifying role attributes (like password, privileges) and managing role membership (adding or removing members from roles).

The structure supports both attribute modifications and membership operations through the same node type, with the action field distinguishing between adding and removing members when dealing with role membership operations.

## Parameters / Member Variables
- : Standard NodeTag identifying this as an AlterRoleStmt node
- : RoleSpec pointer specifying the target role to be altered (can reference role by name or special keywords)
- : List of DefElem nodes containing the role options to be modified (password, privileges, etc.)
- : Integer indicating the type of membership operation (+1 for adding members, -1 for dropping members, 0 for attribute changes)

## Dependencies
- Functions called/Symbols referenced:
  - RoleSpec (for role specification)
  - NodeTag (for type identification)
  - List (for storing role options)
  - DefElem (for individual option specifications)
- Called from (representative examples):
  - AlterRole (role alteration command execution)
  - standard_ProcessUtility (utility command processing)

## Notes and Other Information
- This structure handles multiple types of ALTER ROLE operations: attribute changes, membership additions, and membership removals
- The action field is specifically used for role membership operations (GRANT/REVOKE role membership)
- When modifying role attributes (not membership), the action field is typically 0
- Role options in the list can include PASSWORD, VALID UNTIL, various privilege flags, etc.
- Uses RoleSpec which allows referencing roles by name or special identifiers like CURRENT_ROLE
- Location: src/include/nodes/parsenodes.h:3089-3095
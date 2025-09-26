# DropOwnedStmt

## Location
[src/include/nodes/parsenodes.h:4075-4080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4075-L4080)

## Overview
DropOwnedStmt represents the parsed representation of a DROP OWNED statement, which is used to drop all database objects owned by specified roles.

## Definition

```c
typedef struct DropOwnedStmt
{
	NodeTag		type;
	List	   *roles;
	DropBehavior behavior;
} DropOwnedStmt;
```
## Detailed Description
The DropOwnedStmt structure is a parse node that encapsulates the information needed to execute a DROP OWNED BY statement in PostgreSQL. This statement is used to drop all database objects (tables, functions, types, etc.) that are owned by one or more specified roles. The statement is particularly useful for cleaning up objects before dropping a role, as roles cannot be dropped if they still own database objects.

The structure follows PostgreSQL's standard parse node pattern, inheriting from Node through the NodeTag field, and contains the essential information needed by the command execution phase: the list of roles whose objects should be dropped and the drop behavior (CASCADE or RESTRICT).

## Parameters / Member Variables
- `type`: NodeTag identifying this as a DropOwnedStmt parse node
- `*roles`: List of RoleSpec nodes representing the roles whose owned objects should be dropped
- `behavior`: DropBehavior enum value specifying whether to CASCADE (automatically drop dependent objects) or RESTRICT (fail if dependent objects exist)
## Dependencies
- Functions called/Symbols referenced:
  - DropBehavior (enum type for specifying drop behavior)
  - NodeTag (for parse node identification)
  - [List](../L/List.md) (PostgreSQL's generic list structure)

- Called from (representative examples):
  - [DropOwnedObjects](DropOwnedObjects.md) (executes the DROP OWNED command)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing dispatcher)

## Notes and Other Information
- This statement is typically used as a preparatory step before dropping roles, since PostgreSQL requires that roles own no objects before they can be dropped
- The CASCADE behavior will automatically drop objects that depend on the owned objects, while RESTRICT will cause the command to fail if any dependencies exist
- The roles list can contain multiple role specifications, allowing bulk cleanup of objects owned by multiple roles in a single statement
- Part of PostgreSQL's role and privilege management system, defined in the parsenodes.h header file
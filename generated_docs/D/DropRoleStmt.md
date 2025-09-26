# DropRoleStmt

## Location
[src/include/nodes/parsenodes.h:3105-3110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3105-L3110)

## Overview
DropRoleStmt is a parse tree node structure that represents DROP ROLE, DROP USER, or DROP GROUP SQL statements used to remove one or more database roles from the system.

## Definition

```c
typedef struct DropRoleStmt
{
	NodeTag		type;
	List	   *roles;			/* List of roles to remove */
	bool		missing_ok;		/* skip error if a role is missing? */
} DropRoleStmt;
```
## Detailed Description
DropRoleStmt is a parser node structure that encapsulates information needed to drop database roles in PostgreSQL. This structure handles DROP ROLE, DROP USER, and DROP GROUP statements, which are equivalent operations since users and groups are both types of roles in PostgreSQL.

The structure supports dropping multiple roles in a single statement and includes an option to suppress errors when attempting to drop non-existent roles (IF EXISTS clause).

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a DropRoleStmt node
- `*roles`: List containing the names of roles to be dropped (can contain multiple role names)
- `missing_ok`: Boolean flag indicating whether to skip errors if a specified role does not exist (corresponds to IF EXISTS clause)
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - [List](../L/List.md) (for storing role names)
- Called from (representative examples):
  - [DropRole](DropRole.md) (role deletion command execution)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)

## Notes and Other Information
- This structure handles DROP ROLE, DROP USER, and DROP GROUP statements uniformly since they are equivalent in PostgreSQL
- Multiple roles can be dropped in a single statement by listing them in the roles list
- The missing_ok flag corresponds to the IF EXISTS clause in SQL - [when](../w/when.md) true, no error is raised for non-existent roles
- Role deletion requires appropriate privileges and will fail if the role owns database objects or has active connections
- The roles list contains simple string names, not RoleSpec structures like other role-related statements
- Location: src/include/nodes/parsenodes.h:3105-3110
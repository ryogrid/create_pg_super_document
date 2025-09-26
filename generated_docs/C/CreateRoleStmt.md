# CreateRoleStmt

## Location
[src/include/nodes/parsenodes.h:3081-3087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3081-L3087)

## Overview
CreateRoleStmt is a parse tree node structure that represents CREATE ROLE, CREATE USER, or CREATE GROUP SQL statements used to create database roles with specified options.

## Definition

```c
typedef struct CreateRoleStmt
{
	NodeTag		type;
	RoleStmtType stmt_type;		/* ROLE/USER/GROUP */
	char	   *role;			/* role name */
	List	   *options;		/* List of DefElem nodes */
} CreateRoleStmt;
```
## Detailed Description
CreateRoleStmt is a parser node structure that encapsulates the information needed to create database roles in PostgreSQL. This structure handles the CREATE ROLE, CREATE USER, and CREATE GROUP statements, which are all variations of the same underlying functionality in PostgreSQL (since users and groups are both types of roles).

The structure contains the role name and a list of options that specify the role's properties and privileges. The stmt_type field distinguishes between the different SQL statement variants, which affects the default values applied during role creation.

## Parameters / Member Variables
- : Standard NodeTag identifying this as a CreateRoleStmt node
- : Enumeration value indicating whether this was CREATE ROLE, CREATE USER, or CREATE GROUP (affects default privileges)
- : String containing the name of the role to be created
- : List of DefElem nodes containing role options such as PASSWORD, SUPERUSER, CREATEDB, etc.

## Dependencies
- Functions called/Symbols referenced:
  - [RoleStmtType](../R/RoleStmtType.md) (enumeration for statement type)
  - NodeTag (for type identification)
  - [List](../L/List.md) (for storing role options)
  - [DefElem](../D/DefElem.md) (for individual option specifications)
- Called from (representative examples):
  - [CreateRole](CreateRole.md) (role creation command execution)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)

## Notes and Other Information
- This structure unifies the handling of CREATE ROLE, CREATE USER, and CREATE GROUP statements, which are all equivalent in PostgreSQL
- The options list contains DefElem nodes that specify role attributes like password, superuser status, database creation privileges, etc.
- The stmt_type field helps determine appropriate default values - for example, CREATE USER typically defaults to LOGIN privilege while CREATE ROLE does not
- Part of the role management system that provides PostgreSQL's security and access control framework
- Location: src/include/nodes/parsenodes.h:3081-3087
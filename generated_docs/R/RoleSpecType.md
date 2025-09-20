# RoleSpecType

## Location
[src/include/nodes/parsenodes.h:399-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L399-L400)

## Overview
RoleSpecType is an enumeration that defines the different types of role specifications that can be used in PostgreSQL SQL commands, including explicit role names and special built-in role identifiers.

## Definition

```c
typedef struct RoleSpec
{
	NodeTag		type;
	RoleSpecType roletype;		/* Type of this rolespec */
	char	   *rolename;		/* filled only for ROLESPEC_CSTRING */
	ParseLoc	location;		/* token location, or -1 if unknown */
} RoleSpec;
```
## Detailed Description
RoleSpecType provides a classification system for different ways roles can be specified in PostgreSQL SQL statements. It distinguishes between explicit role names and special role identifiers that are resolved at runtime. This enum is used by the parser to handle role specifications in commands like GRANT, REVOKE, ALTER DEFAULT PRIVILEGES, and other role-related SQL statements. The different types allow the system to handle both static role names and dynamic role resolution based on the current session context.

## Parameters / Member Variables
- : An explicit role name stored as a C string in the rolename field
- : References the CURRENT_ROLE special identifier (current effective role)
- : References the CURRENT_USER special identifier (current user name)
- : References the SESSION_USER special identifier (session authentication identity)
- : References the special "public" role that represents all users

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [RoleSpec](RoleSpec.md) (in roletype field)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:392-399
- Used in conjunction with RoleSpec struct which contains the actual role specification data
- CURRENT_ROLE, CURRENT_USER, and SESSION_USER are SQL standard role identifiers that resolve dynamically
- The "public" role is a special PostgreSQL role that represents all database users
- Only ROLESPEC_CSTRING type uses the rolename field in RoleSpec; other types rely on their type identification
- Essential for proper authorization and privilege management in PostgreSQL's security model
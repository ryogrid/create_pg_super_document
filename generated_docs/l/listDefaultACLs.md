# listDefaultACLs

## Location
[src/bin/psql/describe.c:1175-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1175-L1251)

## Overview
Implements the \ddp psql command to display default access control lists (ACLs) that specify the privileges automatically granted to new objects created in specified schemas by particular roles.

## Definition


## Detailed Description
This function queries the pg_default_acl system catalog to retrieve and display default access privileges that are automatically applied to newly created objects. Default ACLs allow database administrators to specify privileges that should be automatically granted when objects of certain types (tables, sequences, functions, types, schemas) are created within specific schemas by specific roles.

The function constructs a SQL query that joins pg_default_acl with pg_namespace to provide comprehensive information including the role that owns the default ACL, the target schema (if any), the object type affected, and the specific privileges granted. The output shows how privileges are structured for different object types and helps administrators understand the privilege inheritance system.

The pattern matching supports filtering by either schema name or role name, making it easy to examine default privileges for specific contexts. Object types are displayed with localized names for better user experience.

## Parameters / Member Variables
- : SQL pattern to match either schema names or role names (can be NULL to show all default ACLs)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer (initialize query buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query with object type mapping)
  - [printACLColumn](../p/printACLColumn.md) (format the default access control list)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and apply name pattern filtering)
  - [PSQLexec](../P/PSQLexec.md) (execute the constructed SQL query)
  - [printQuery](../p/printQuery.md) (display formatted results with translation support)
  - termPQExpBuffer (cleanup query buffer)
  - DEFACLOBJ constants (DEFACLOBJ_RELATION, DEFACLOBJ_SEQUENCE, DEFACLOBJ_FUNCTION, DEFACLOBJ_TYPE, DEFACLOBJ_NAMESPACE)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (src/bin/psql/command.c:856) - handles \ddp command
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:47)

## Notes and Other Information
- Default ACLs apply only to objects created in the future, not to existing objects
- The schema column can be NULL, indicating the default ACL applies to all schemas where the role has CREATE privilege
- Object types supported: tables, sequences, functions, types, and schemas (represented by DEFACLOBJ_* constants)
- Pattern matching works against both schema names and role names for flexible filtering
- Uses translate_columns array to control internationalization of the "Type" column
- Results are ordered by owner, schema, and object type for consistent presentation  
- The ACL format follows the same conventions as other privilege displays in PostgreSQL
- Returns boolean status indicating success/failure of the operation
- Helpful for understanding privilege inheritance and automated privilege assignment in PostgreSQL databases
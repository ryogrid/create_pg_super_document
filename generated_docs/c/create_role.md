# create_role

## Location
[src/test/regress/pg_regress.c:2000-2013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L2000-L2013)

## Overview
A utility function in the PostgreSQL regression test framework that creates a database role with login privileges and grants permissions to specified databases.

## Definition

```c
static void
create_role(const char *rolename, const _stringlist *granted_dbs)
```
## Detailed Description
The  function is part of the PostgreSQL regression testing infrastructure (). It constructs and executes SQL commands to create a new database role with login capabilities and optionally grants ALL privileges on specified databases to that role. The function uses the psql command interface to execute the SQL statements against the PostgreSQL server.

The function first creates a role with LOGIN privilege, then iterates through the provided list of database names, granting ALL privileges on each database to the newly created role. This is typically used during test setup to create roles with appropriate permissions for regression testing scenarios.

## Parameters / Member Variables
- `*rolename`: The name of the database role to be created
- `*granted_dbs`: A linked list of database names (_stringlist) on which the role should be granted ALL privileges; can be NULL if no database grants are needed
## Dependencies
- Functions called/Symbols referenced:
  - [psql_start_command](../p/psql_start_command.md)
  - [psql_add_command](../p/psql_add_command.md)  
  - [psql_end_command](../p/psql_end_command.md)
  - [_stringlist](../s/_stringlist.md)
- Called from (representative examples):
  - No references found (appears to be an internal utility function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit (pg_regress.c)
- The function connects to the 'postgres' database to execute the role creation and grant commands
- Uses StringInfo buffer to build the SQL command sequence before execution
- Part of the PostgreSQL regression testing framework, not the core database engine
- The role is created with LOGIN privilege, making it suitable for database connections during tests
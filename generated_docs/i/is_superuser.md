# is_superuser

## Location
[src/bin/psql/common.c:2112-2131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L2112-L2131)

## Overview
This function checks whether the currently connected user in a pg_dump session has superuser privileges by querying the server's parameter status.

## Definition


## Detailed Description
The  function is a utility function in pg_dump that determines if the current database connection is established by a superuser. It queries the PostgreSQL server's parameter status using  to retrieve the "is_superuser" parameter value. This information is crucial for pg_dump operations as certain database objects and operations require superuser privileges to access or dump properly.

The function returns  if the connected user is a superuser (when the parameter value is "on"), and  otherwise. This check allows pg_dump to conditionally handle operations that require elevated privileges.

## Parameters / Member Variables
- : Archive pointer representing the pg_dump output archive handle, which contains the database connection information

## Dependencies
- Functions called/Symbols referenced:
  - PQparameterStatus (libpq function to query server parameters)
- Called from (representative examples):
  - [getSubscriptions](../g/getSubscriptions.md) (for dumping subscription information)
  - Various functions in variable.c for session authorization checks
  - User management functions in miscinit.c

## Notes and Other Information
- This is a static function specific to pg_dump, not part of the general PostgreSQL backend
- The function relies on the server's built-in parameter "is_superuser" which is automatically set based on the connecting user's privileges
- Used primarily in pg_dump to determine whether certain privileged database objects can be accessed and dumped
- The superuser status affects what database objects pg_dump can access, particularly system catalogs and certain metadata
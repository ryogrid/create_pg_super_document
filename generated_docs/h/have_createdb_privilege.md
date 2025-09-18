# have_createdb_privilege

## Location
[src/backend/commands/dbcommands.c:2939-2963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2939-L2963)

## Overview
Checks if the current user has the privilege to create databases in PostgreSQL.

## Definition


## Detailed Description
This function determines whether the currently connected user has the necessary privileges to create databases. The function implements PostgreSQL's hierarchical privilege system where superusers automatically have all privileges, including database creation. For non-superuser roles, it checks the  attribute in the  system catalog to determine if the role has been granted database creation privileges.

The function performs a system catalog lookup to retrieve the user's role information and examines the  boolean field to make the privilege determination.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if current user is a superuser
  -  - Get the OID of the current user
  -  - Search system cache for role information
  -  - Validate heap tuple
  -  - Extract structure from heap tuple
  -  - Release system cache entry
  -  - Convert OID to Datum
  -  - Structure type for pg_authid catalog
- Called from (representative examples):
  -  - Database creation command
  -  - Database rename operation
  -  - Database owner change operation
  -  - Role creation command
  -  - Role alteration command

## Notes and Other Information
- Returns  immediately if the current user is a superuser, as superusers have all privileges
- For regular users, performs a system catalog lookup in  to check the  attribute
- Uses the system cache (syscache) for efficient access to role information
- This privilege check is typically performed before attempting database creation operations to ensure proper authorization
- The function is defined in 
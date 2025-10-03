# dumpRoleGUCPrivs

## Location
[src/bin/pg_dump/pg_dumpall.c:1245-1299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1245-L1299)

## Overview
Dumps role configuration parameter privileges for PostgreSQL 15.0 and later servers, generating SQL commands to recreate ACL permissions on configuration parameters.

## Definition

```c
static void
dumpRoleGUCPrivs(PGconn *conn)
```
## Detailed Description
This function handles the dumping of role privileges on configuration parameters (GUC - Grand Unified Configuration). It's specifically designed for PostgreSQL version 15.0 and later where per-role configuration parameter privileges are supported. The function queries the pg_parameter_acl system catalog to retrieve all parameters that have non-default access control lists (ACLs) defined, then generates the appropriate GRANT/REVOKE commands to recreate these privileges.

The function assumes that all roles have already been created by the dumpRoles function, but the per-role configuration parameter privileges have not yet been applied. It processes each parameter with custom ACLs and uses the buildACLCommands utility to generate the SQL commands needed to restore the privilege state.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle used to execute queries against the database
## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md): Executes SQL query to retrieve parameter ACL information
  - CppAsString2: Macro for converting values to string literals
  - [buildACLCommands](../b/buildACLCommands.md): Generates GRANT/REVOKE SQL commands from ACL data
  - [fmtId](../f/fmtId.md): Formats identifier names for SQL output
  - [PQfinish](../P/PQfinish.md): Closes database connection on error
  - [exit_nicely](../e/exit_nicely.md): Performs clean exit with error status
- Called from (representative examples):
  - [main](../m/main.md): Primary entry point in pg_dumpall utility

## Notes and Other Information
- Only available for PostgreSQL 15.0+ servers where parameter ACLs are supported
- Queries pg_parameter_acl system catalog to find parameters with custom privileges
- Uses BOOTSTRAP_SUPERUSERID as the default owner for parameter privileges
- Outputs descriptive header comments in the dump file for clarity
- Error handling includes proper cleanup and informative error messages
- Part of the pg_dumpall utility's comprehensive database cluster backup functionality
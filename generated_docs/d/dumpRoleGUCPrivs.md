# dumpRoleGUCPrivs

## Location
src/bin/pg_dump/pg_dumpall.c: 1245 - 1299

## Overview
Dumps role configuration parameter privileges for PostgreSQL 15.0 and later servers, generating SQL commands to recreate ACL permissions on configuration parameters.

## Definition


## Detailed Description
This function handles the dumping of role privileges on configuration parameters (GUC - Grand Unified Configuration). It's specifically designed for PostgreSQL version 15.0 and later where per-role configuration parameter privileges are supported. The function queries the pg_parameter_acl system catalog to retrieve all parameters that have non-default access control lists (ACLs) defined, then generates the appropriate GRANT/REVOKE commands to recreate these privileges.

The function assumes that all roles have already been created by the dumpRoles function, but the per-role configuration parameter privileges have not yet been applied. It processes each parameter with custom ACLs and uses the buildACLCommands utility to generate the SQL commands needed to restore the privilege state.

## Parameters / Member Variables
- : PostgreSQL connection handle used to execute queries against the database

## Dependencies
- Functions called/Symbols referenced:
  - executeQuery: Executes SQL query to retrieve parameter ACL information
  - CppAsString2: Macro for converting values to string literals
  - buildACLCommands: Generates GRANT/REVOKE SQL commands from ACL data
  - fmtId: Formats identifier names for SQL output
  - PQfinish: Closes database connection on error
  - exit_nicely: Performs clean exit with error status
- Called from (representative examples):
  - main: Primary entry point in pg_dumpall utility

## Notes and Other Information
- Only available for PostgreSQL 15.0+ servers where parameter ACLs are supported
- Queries pg_parameter_acl system catalog to find parameters with custom privileges
- Uses BOOTSTRAP_SUPERUSERID as the default owner for parameter privileges
- Outputs descriptive header comments in the dump file for clarity
- Error handling includes proper cleanup and informative error messages
- Part of the pg_dumpall utility's comprehensive database cluster backup functionality
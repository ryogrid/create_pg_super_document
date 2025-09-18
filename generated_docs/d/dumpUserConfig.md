# dumpUserConfig

## Location
src/bin/pg_dump/pg_dumpall.c: 1486 - 1527

## Overview
Generates ALTER ROLE statements to restore user-specific configuration parameters (GUC settings) that are set at the role level across all databases.

## Definition
static void dumpUserConfig(PGconn *conn, const char *username)

## Detailed Description
This function extracts and dumps user-specific configuration settings that have been set at the role level using ALTER ROLE commands. It queries the pg_db_role_setting system catalog to find configuration parameters that are set for a specific role across all databases (where setdatabase = 0). These are typically parameters that have been set using commands like 'ALTER ROLE username SET parameter = value'.

The function builds a dynamic query that searches for the role's OID using the role_catalog (which varies depending on server version), then retrieves all configuration settings associated with that role. For each setting found, it generates the appropriate ALTER ROLE SET command to restore the configuration during database restoration.

The function includes proper sanitization of the username for safe display in comments and uses the makeAlterConfigCommand utility to generate syntactically correct ALTER statements.

## Parameters / Member Variables
- conn: PostgreSQL connection handle used to execute queries against the database
- username: The name of the role whose configuration settings should be dumped

## Dependencies
- Functions called/Symbols referenced:
  - printfPQExpBuffer: Formats the SQL query string
  - appendStringLiteralConn: Safely appends username as string literal to query
  - appendPQExpBufferChar: Adds closing parenthesis to query
  - executeQuery: Executes the constructed query
  - sanitize_line: Sanitizes username for safe display in comments
  - resetPQExpBuffer: Clears buffer for reuse
  - makeAlterConfigCommand: Generates ALTER ROLE SET command from configuration data
- Called from (representative examples):
  - dumpRoles: Called for each role during role dumping process

## Notes and Other Information
- Only dumps role-level settings that apply across all databases (setdatabase = 0)
- Uses dynamic role_catalog reference to support different PostgreSQL versions
- Includes descriptive header comments when configuration settings are found
- Part of the comprehensive role restoration process in pg_dumpall
- Essential for preserving user-specific default settings during cluster migration
- Coordinates with dumpRoles function to provide complete role restoration
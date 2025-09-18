# listForeignServers

## Location
src/bin/psql/describe.c: 5799 - 5874

## Overview
Lists foreign servers in the PostgreSQL database, showing their names, owners, associated foreign data wrappers, and optionally detailed configuration information.

## Definition
bool listForeignServers(const char *pattern, bool verbose)

## Detailed Description
This function queries the pg_foreign_server and related system catalogs to display information about foreign servers configured in the database. It shows basic information including server name, owner, and the foreign data wrapper used. In verbose mode, it additionally displays access control lists (ACLs), server type, version, server-specific options formatted as key-value pairs, and descriptions. The function joins pg_foreign_server with pg_foreign_data_wrapper to show the relationship between servers and their underlying FDWs. This implements the \des psql command functionality.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter foreign server names (can be NULL to show all servers)
- `verbose`: Boolean flag to control whether to show additional detailed information (ACLs, type, version, options, descriptions)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - printfPQExpBuffer
  - appendPQExpBufferStr
  - printACLColumn
  - appendPQExpBuffer
  - validateSQLNamePattern
  - termPQExpBuffer
  - PSQLexec
  - printQuery
  - PQclear
  - gettext_noop
- Called from (representative examples):
  - exec_command_d (psql command dispatcher)

## Notes and Other Information
- Returns false if pattern validation fails or query execution fails
- Performs JOIN between pg_foreign_server and pg_foreign_data_wrapper tables to show FDW relationships
- In verbose mode, displays server options using pg_options_to_table() for proper formatting
- Includes left join with pg_description for object descriptions in verbose mode
- Orders results alphabetically by server name for consistent output
- Uses internationalization support for all column headers and titles
- The server type and version fields are optional and may be NULL
- This function corresponds to the \des command in psql for listing foreign servers
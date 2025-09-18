# describeRoles

## Location
src/bin/psql/describe.c: 3614 - 3748

## Overview
Implements the \du and \dg commands in psql to display information about database roles (users and groups).

## Definition


## Detailed Description
The  function implements psql's \du (describe users) and \dg (describe groups) commands, which are functionally identical since PostgreSQL treats users and groups as the same entity (roles). The function queries the pg_roles system view to retrieve comprehensive information about database roles and formats it into a readable table.

The function displays role names along with their attributes in a human-readable format. Attributes include superuser status, inheritance capabilities, role creation privileges, database creation privileges, login capabilities, replication permissions, and row-level security bypass permissions (for PostgreSQL 9.5+). It also shows connection limits and password expiration dates when applicable.

In verbose mode, the function includes role descriptions from the system comments. The output is formatted as a table with role names in the first column and a consolidated attributes column that lists all relevant permissions and restrictions for each role.

## Parameters / Member Variables
- : SQL pattern to filter role names (supports wildcards). Schema portions are ignored since roles are cluster-wide objects.
- : Boolean flag for verbose mode (\du+ vs \du) - when true, includes role descriptions from pg_description
- : Boolean flag to include system roles (roles starting with 'pg_'). If false, only user-defined roles are shown.

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer: Initialize query buffer for SQL construction
  - printfPQExpBuffer: Format SQL query with role attributes
  - validateSQLNamePattern: Process and validate the role name pattern
  - PSQLexec: Execute the role information query
  - printTableInit: Initialize table formatting structure
  - printTableAddHeader: Add column headers for the role table
  - printTableAddCell: Add role data to table cells
  - add_role_attribute: Helper function to format individual role attributes
  - resetPQExpBuffer: Clear buffer for reuse in attribute formatting
  - printTable: Display the formatted role table
  - printTableCleanup: Clean up table formatting resources
  - pg_malloc0: Allocate memory for attribute strings
- Called from (representative examples):
  - exec_command_d: Command dispatcher for both \du and \dg commands in psql

## Notes and Other Information
- The function treats \du and \dg identically since PostgreSQL unified users and groups into roles
- Handles version-specific features like row-level security bypass (PostgreSQL 9.5+)
- Uses internationalization through gettext for translatable attribute names and descriptions
- Implements smart attribute display - only shows relevant attributes for each role (e.g., "Cannot login" instead of showing login capability for non-login roles)
- Connection limits are displayed with proper pluralization using ngettext
- Password expiration dates are shown in a user-friendly format when present
- Memory management includes proper cleanup of dynamically allocated attribute strings
- The function filters out system roles by default unless showSystem is true
- Returns false on SQL errors or validation failures for proper error propagation
- Role descriptions in verbose mode come from the shared object description system (shobj_description)
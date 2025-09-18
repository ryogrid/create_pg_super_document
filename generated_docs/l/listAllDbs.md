# listAllDbs

## Location
src/bin/psql/describe.c: 911 - 1010

## Overview
Implements the \l, \list psql commands and the -l command-line switch to display a comprehensive list of all databases in the PostgreSQL cluster with their properties and metadata.

## Definition


## Detailed Description
This function constructs and executes a SQL query against system catalogs to retrieve detailed information about all databases in the PostgreSQL cluster. It displays essential database properties including name, owner, encoding, locale settings, access privileges, and optionally size and tablespace information.

The function adapts its query based on the PostgreSQL server version to handle evolving database locale features. For PostgreSQL 15+, it displays the actual locale provider (builtin, libc, icu). For PostgreSQL 17+, it shows the unified locale field, while for 15-16 it shows ICU-specific locale, and for older versions it shows NULL. Similarly, ICU rules are displayed for PostgreSQL 16+.

When verbose mode is enabled, the function includes additional columns for database size (with access control - shows 'No Access' if the user lacks CONNECT privilege) and the tablespace name. The query joins with pg_tablespace in verbose mode to retrieve tablespace information.

## Parameters / Member Variables  
- : SQL pattern to filter database names (can be NULL to list all databases)
- : If true, includes size, tablespace, and description columns in the output

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer (initialize query buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format base SQL query)
  - [printACLColumn](../p/printACLColumn.md) (add access privileges column)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and apply name pattern filtering)
  - [PSQLexec](../P/PSQLexec.md) (execute the constructed SQL query)
  - [printQuery](../p/printQuery.md) (display formatted results)
  - termPQExpBuffer (cleanup query buffer)
- Called from (representative examples):
  - [exec_command_list](../e/exec_command_list.md) (src/bin/psql/command.c:1984) - handles \l and \list commands
  - PARAMS_ARRAY_SIZE (src/bin/psql/startup.c:334) - handles -l command line option
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:68)

## Notes and Other Information
- Adapts query structure based on server version (pset.sversion) to handle locale provider and ICU features
- In verbose mode, database size calculation respects CONNECT privileges - displays 'No Access' for inaccessible databases
- Results are ordered by database name for consistent presentation
- Uses internationalization (gettext_noop) for column headers to support multiple languages
- Handles backward compatibility across PostgreSQL versions 15-17+ for locale-related columns
- Access control list (ACL) information is displayed using the standard printACLColumn formatter
- Returns boolean status indicating success/failure of the operation
- The query includes a safety check using has_database_privilege() to prevent errors when accessing database size information
# listDomains

## Location
src/bin/psql/describe.c: 4383 - 4465

## Overview
Lists and displays information about user-defined domains in the PostgreSQL database, corresponding to the psql \dD command.

## Definition
bool listDomains(const char *pattern, bool verbose, bool showSystem)

## Detailed Description
The listDomains function generates and executes a SQL query to retrieve information about domains from the pg_catalog.pg_type system catalog. It displays domain details including schema, name, underlying base type, collation, nullable constraints, default values, and check constraints. When verbose mode is enabled, it includes access control lists (ACLs) and descriptions. The function specifically filters for domain types (typtype = 'd') and can optionally exclude system schemas based on the showSystem parameter. The query uses complex subqueries to extract collation information and aggregate check constraints into a readable format.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter domain names by schema and/or name (can be NULL for no filtering)
- `verbose`: Boolean flag to include extended information (ACLs, descriptions) in the output
- `showSystem`: Boolean flag to include domains from system schemas (pg_catalog, information_schema)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - printfPQExpBuffer  
  - printACLColumn
  - validateSQLNamePattern
  - termPQExpBuffer
  - PSQLexec
  - printQuery
- Called from (representative examples):
  - exec_command_d (psql command dispatcher)

## Notes and Other Information
- Implements the \dD psql meta-command functionality
- Queries pg_catalog.pg_type with typtype = 'd' to find domain types
- Joins with pg_catalog.pg_namespace for schema information
- Uses complex subqueries to resolve collation names and aggregate check constraints
- When showSystem is false, excludes pg_catalog and information_schema
- Returns false on query validation or execution failure, true on success
- Output is ordered by schema name then domain name for consistent presentation
- Uses pg_catalog.pg_type_is_visible() for visibility checks when pattern matching
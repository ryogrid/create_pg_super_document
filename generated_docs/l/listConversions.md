# listConversions

## Location
src/bin/psql/describe.c: 4466 - 4545

## Overview
Lists and displays information about character set conversions in the PostgreSQL database, corresponding to the psql \dc command.

## Definition
bool listConversions(const char *pattern, bool verbose, bool showSystem)

## Detailed Description
The listConversions function generates and executes a SQL query to retrieve information about character encoding conversions from the pg_catalog.pg_conversion system catalog. It displays conversion details including schema, name, source encoding, destination encoding, and whether the conversion is a default conversion. When verbose mode is enabled, it includes descriptions. The function uses PostgreSQL's built-in pg_encoding_to_char() function to convert encoding IDs to readable names and formats the default status as localized yes/no values. The query can optionally exclude system schemas and supports pattern-based filtering.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter conversion names by schema and/or name (can be NULL for no filtering)
- `verbose`: Boolean flag to include extended information (descriptions) in the output
- `showSystem`: Boolean flag to include conversions from system schemas (pg_catalog, information_schema)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - printfPQExpBuffer
  - validateSQLNamePattern
  - termPQExpBuffer
  - PSQLexec
  - printQuery
  - lengthof
- Called from (representative examples):
  - exec_command_d (psql command dispatcher)

## Notes and Other Information
- Implements the \dc psql meta-command functionality
- Queries the pg_catalog.pg_conversion system catalog
- Uses pg_catalog.pg_encoding_to_char() to convert encoding IDs to names
- Joins with pg_catalog.pg_namespace for schema information
- Uses static translate_columns array to specify which columns should be translated
- When showSystem is false, excludes pg_catalog and information_schema
- Returns false on query validation or execution failure, true on success
- Output is ordered by schema name then conversion name for consistent presentation
- Uses pg_catalog.pg_conversion_is_visible() for visibility checks when pattern matching
- The 'Default?' column shows localized yes/no values based on the condefault field
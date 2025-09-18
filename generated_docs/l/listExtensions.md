# listExtensions

## Location
src/bin/psql/describe.c: 6002 - 6052

## Overview
Implements the  command in psql to display a brief list of installed PostgreSQL extensions with their names, versions, schemas, and descriptions.

## Definition


## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve information about installed extensions. It constructs a SQL query that joins the pg_extension catalog with pg_namespace and pg_description to provide comprehensive extension information. The function supports pattern matching for selective display of extensions and presents the results in a formatted table showing extension name, version, schema, and description.

The query retrieves:
- Extension name
- Extension version
- Schema name where the extension is installed
- Extension description (from pg_description)

## Parameters / Member Variables
- : SQL name pattern for filtering extensions (can be NULL for all extensions)

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [printQueryOpt](../p/printQueryOpt.md) (data structure)
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in src/bin/psql/command.c:1014)

## Notes and Other Information
- This function is part of psql's describe commands (\d family)
- Uses internationalization with gettext_noop for column headers
- Implements proper error handling by returning false on failures
- The query uses LEFT JOINs to handle extensions that might not have descriptions or might be in unusual schemas
- Pattern validation is handled by validateSQLNamePattern to ensure SQL injection safety
- Results are ordered by extension name for consistent presentation
- Unlike the more detailed listExtensionContents, this provides a summary view of all extensions
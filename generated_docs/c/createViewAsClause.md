# createViewAsClause

## Location
[src/bin/pg_dump/pg_dump.c:15857-15905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15857-L15905)

## Overview
Retrieves and formats the AS clause definition for a view or materialized view, stripping the trailing semicolon to allow additional clauses to be appended.

## Definition

```c
static PQExpBuffer
createViewAsClause(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
This utility function extracts the complete view definition from the database using the pg_get_viewdef() system function and formats it for use in CREATE VIEW or CREATE MATERIALIZED VIEW statements. The function specifically removes the trailing semicolon from the view definition to enable additional SQL clauses to be appended, such as WITH NO DATA for materialized views or other modifiers.

The function performs robust error checking to ensure the view definition is retrieved successfully and is not empty. It uses PostgreSQL's built-in pg_get_viewdef() function which returns the properly formatted SQL query that defines the view, including all necessary parentheses, aliases, and formatting. The returned buffer contains just the AS clause portion without the leading CREATE VIEW statement or trailing semicolon.

This function is essential for generating syntactically correct view creation statements during the dump process, particularly when the view definition needs to be combined with additional clauses or modified for different PostgreSQL versions.

## Parameters / Member Variables
- : Archive structure containing database connection for executing queries
- : TableInfo structure containing metadata about the view including its catalog ID and name

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer: Creates buffers for query construction and result storage
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md): Constructs parameterized query to get view definition
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the pg_get_viewdef query against the database
  - [PQntuples](../P/PQntuples.md): Checks that exactly one result row was returned
  - [PQgetlength](../P/PQgetlength.md): Gets the length of the view definition string
  - [PQgetvalue](../P/PQgetvalue.md): Extracts the view definition from query results
  - appendBinaryPQExpBuffer: Copies view definition minus semicolon to result buffer
  - [PQclear](../P/PQclear.md): Frees query result memory
  - destroyPQExpBuffer: Cleans up query buffer
  - [pg_fatal](../p/pg_fatal.md): Reports fatal errors if view definition is missing or invalid
- Called from:
  - [dumpTableSchema](../d/dumpTableSchema.md): Used when dumping view and materialized view schema definitions
  - [dumpRule](../d/dumpRule.md): Used when dumping rules that involve view-like constructs

## Notes and Other Information
- Returns a new PQExpBuffer that must be freed by the caller
- Uses PostgreSQL's pg_get_viewdef() function which handles all formatting and escaping
- Performs strict validation that exactly one non-empty view definition is returned
- The semicolon stripping is essential for materialized views which need WITH NO DATA appended
- Error messages include the view name for easier debugging
- Handles both regular views and materialized views transparently
- The Assert() statement ensures the assumption about semicolon presence is verified in debug builds
- Part of the view dumping infrastructure that enables proper restoration of complex view definitions
- Works across different PostgreSQL versions as pg_get_viewdef() is a stable system function
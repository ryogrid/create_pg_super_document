# PQftablecol

## Location
[src/interfaces/libpq/fe-exec.c:3697-3707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3697-L3707)

## Overview
Returns the column number of the source table column that corresponds to the given field in a query result.

## Definition
int PQftablecol(const PGresult *res, int field_num)

## Detailed Description
PQftablecol retrieves the column number within the source table that corresponds to the specified field in a query result. This function is part of PostgreSQL's libpq client library and provides metadata about result columns. The column number refers to the position of the column in the source table, starting from 1. This information is only available when the query result includes metadata about the source tables and columns, which typically occurs with SELECT statements that reference specific table columns. If the field does not originate from a table column (e.g., computed expressions, constants) or if the source column information is not available, the function returns 0.

## Parameters / Member Variables
- res: Pointer to a PGresult structure containing the query result
- field_num: Zero-based index of the field (column) for which to retrieve the source column number

## Dependencies
- Functions called/Symbols referenced:
  - [check_field_number](../c/check_field_number.md): Validates that field_num is within valid range
- Called from (representative examples):
  - Client applications querying metadata about result columns
  - Tools that need to map result columns to their source table columns

## Notes and Other Information
- Returns 0 if the field number is out of range or if no source column information is available
- The function accesses the columnid member of the PGresAttDesc structure stored in res->attDescs
- Column numbers in PostgreSQL are 1-based (first column is 1, not 0)
- Source column information is populated by the server when available and depends on the nature of the SQL query
- This function is thread-safe as it only reads from the PGresult structure
- Defined in src/interfaces/libpq/fe-exec.c:3697-3707
# append_db_pattern_cte

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1537 - 1582

## Overview
Constructs a Common Table Expression (CTE) containing database name patterns extracted from a pattern array for SQL query generation in pg_amcheck.

## Definition


## Detailed Description
This function generates the body of a SQL CTE (Common Table Expression) that contains database patterns filtered from the input pattern array. The CTE produces two columns: `pattern_id` (index in the pattern array) and `rgx` (the database regular expression). The function provides flexibility in pattern inclusion based on the `inclusive` parameter - when false, it only includes patterns that specify only a database name, when true, it includes patterns that may also have schema and/or relation components.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the CTE SQL content to
- `pia`: Pointer to PatternInfoArray containing the patterns to process
- `conn`: PostgreSQL connection handle used for proper string literal escaping
- `inclusive`: Boolean flag controlling whether to include patterns with schema/relation parts

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendPQExpBuffer](appendPQExpBuffer.md)
  - [appendStringLiteralConn](appendStringLiteralConn.md)
  - appendPQExpBufferChar
  - [PatternInfoArray](../P/PatternInfoArray.md)
  - [PatternInfo](../P/PatternInfo.md)
- Called from (representative examples):
  - [compile_database_list](../c/compile_database_list.md) (at src/bin/pg_amcheck/pg_amcheck.c:1608)
  - [compile_database_list](../c/compile_database_list.md) (at src/bin/pg_amcheck/pg_amcheck.c:1625)

## Notes and Other Information
- Returns true if any database patterns were found and appended, false otherwise
- When no patterns are found, appends a dummy SELECT that returns no rows to maintain valid SQL syntax
- Uses proper SQL string literal escaping via appendStringLiteralConn to prevent injection issues
- Part of pg_amcheck's database discovery mechanism for pattern-based object selection
- The generated CTE is typically used in larger SQL queries to match database names against user-specified patterns
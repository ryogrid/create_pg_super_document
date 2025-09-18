# append_rel_pattern_raw_cte

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1775 - 1843

## Overview
Constructs a Common Table Expression (CTE) containing complete pattern information for relation matching in pg_amcheck SQL queries.

## Definition


## Detailed Description
This function generates the body of a SQL CTE that transforms pattern information from the PatternInfoArray into a structured format suitable for SQL queries. The CTE produces six columns that capture all aspects of each pattern: pattern ID, database regex, namespace regex, relation regex, and boolean flags indicating whether the pattern applies only to heap tables or btree indexes. Each pattern entry is converted into a SQL VALUES clause with proper type casting and NULL handling for missing pattern components.

## Parameters / Member Variables
- `buf`: PQExpBuffer to which the CTE SQL content will be appended
- `pia`: Pointer to PatternInfoArray containing the patterns to transform into SQL format  
- `conn`: PostgreSQL connection handle used for proper SQL string literal escaping

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendPQExpBuffer](appendPQExpBuffer.md)  
  - [appendStringLiteralConn](appendStringLiteralConn.md)
  - appendPQExpBufferChar
  - [PatternInfoArray](../P/PatternInfoArray.md)
  - [PatternInfo](../P/PatternInfo.md)
- Called from (representative examples):
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (at src/bin/pg_amcheck/pg_amcheck.c:1900)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (at src/bin/pg_amcheck/pg_amcheck.c:1910)

## Notes and Other Information
- Generates six-column CTE: pattern_id, db_regex, nsp_regex, rel_regex, heap_only, btree_only
- Uses explicit type casting (::INTEGER, ::TEXT, ::BOOLEAN) to ensure proper SQL data types
- Handles NULL values appropriately when pattern components are missing (db_regex, nsp_regex, or rel_regex)
- When no patterns exist, generates a dummy SELECT that returns no rows with correct column types
- Properly escapes string literals using appendStringLiteralConn to prevent SQL injection
- Part of pg_amcheck's relation discovery mechanism, typically used in larger SQL queries for pattern matching
- Essential component for translating user-specified patterns into database-queryable format
# pg_get_indexdef

## Location
[src/backend/utils/adt/ruleutils.c:1158-1177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1158-L1177)

## Overview
A SQL-callable function that returns the complete CREATE INDEX statement for a given index OID using default formatting.

## Definition

```c
Datum
pg_get_indexdef(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the basic SQL interface for retrieving index definitions from PostgreSQL's system catalogs. It extracts an index OID from the function arguments and calls the worker function  with default parameters to generate a complete CREATE INDEX statement. The function uses indented formatting and excludes tablespace information (intentionally for pg_dump compatibility). It delegates all the complex work of reconstructing the index definition to the worker function and handles the return value conversion to PostgreSQL's text type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The OID of the index to retrieve the definition for
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts OID argument from function call
  - : Constant for formatting flags
  - : Core function that builds the index definition string
  - : Converts C string to PostgreSQL text type
  - : Returns text value to SQL caller
  - : Returns NULL to SQL caller
- Called from (representative examples):
  - SQL queries using pg_get_indexdef(oid) function
  - System administration and introspection tools
  - pg_dump utility for database backup

## Notes and Other Information
- This is the simplest interface to index definition retrieval, using default parameters
- Deliberately omits tablespace information for pg_dump compatibility
- Uses indented formatting for readability
- Returns complete index definition (colno=0 to worker function)
- Part of PostgreSQL's rule utilities system for reconstructing DDL statements
- Located in src/backend/utils/adt/ruleutils.c:1158-1177
- For more control over output format, use pg_get_indexdef_ext instead
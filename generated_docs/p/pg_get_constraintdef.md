# pg_get_constraintdef

## Location
[src/backend/utils/adt/ruleutils.c:2126-2142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2126-L2142)

## Overview
PostgreSQL function that returns the complete definition for a constraint, formatted as the SQL text that would appear after "ALTER TABLE ... ADD CONSTRAINT <constraintname>".

## Definition

```c
Datum
pg_get_constraintdef(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the SQL-callable interface for retrieving constraint definitions from the PostgreSQL system catalogs. It takes a constraint OID as input and returns the complete constraint definition as formatted SQL text. The function acts as a wrapper around , providing default parameters for pretty-printing with indentation. The returned definition includes all the necessary SQL syntax that would be required to recreate the constraint using an ALTER TABLE ADD CONSTRAINT statement, making it useful for schema documentation, backup generation, and administrative queries.

## Parameters / Member Variables
- Function expects one argument via :
  -  (Oid): Object identifier of the constraint whose definition should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID, PG_RETURN_NULL, PG_RETURN_TEXT_P (PostgreSQL function interface macros)
  - [pg_get_constraintdef_worker](pg_get_constraintdef_worker.md) (core implementation function that handles constraint definition generation)
  - [string_to_text](../s/string_to_text.md) (converts C string to PostgreSQL text type)
  - PRETTYFLAG_INDENT (formatting constant for indented output)
- Called from (representative examples):
  - No direct references found in the codebase (likely called from SQL queries and system views)

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL, commonly used in system information views
- Returns NULL if the constraint is not found or if the worker function cannot generate a definition
- Uses pretty-printing with indentation for readable output formatting
- The function calls the worker with parameters  where the boolean parameters control various formatting and validation options
- Supports all constraint types: CHECK, FOREIGN KEY, PRIMARY KEY, UNIQUE, EXCLUSION constraints
- The returned definition is suitable for use in DDL statements to recreate the constraint
- Part of PostgreSQL's rule utilities system for reconstructing DDL from system catalogs
- Often used in pg_dump and other backup utilities to generate constraint definitions
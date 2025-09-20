# pg_get_statisticsobjdef

## Location
[src/backend/utils/adt/ruleutils.c:1588-1606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1588-L1606)

## Overview
PostgreSQL function that retrieves the definition of an extended statistics object as a formatted SQL CREATE STATISTICS statement.

## Definition
```c
Datum pg_get_statisticsobjdef(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a SQL-callable interface for obtaining the definition of extended statistics objects in PostgreSQL. Extended statistics objects are database objects that store multi-column statistics to help the query planner make better estimates for complex queries involving correlated columns.

The function extracts the OID of the statistics object from its argument, delegates the actual work to pg_get_statisticsobj_worker, and handles the conversion from internal C string representation to PostgreSQL's text data type for return to SQL callers.

As a PostgreSQL function following the PG_FUNCTION_ARGS convention, it can be called directly from SQL queries to retrieve human-readable CREATE STATISTICS statements for existing statistics objects, which is useful for documentation, backup scripts, or understanding database schema.

## Parameters / Member Variables
- `statextid`: OID of the extended statistics object (extracted via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro to extract OID argument from function call)
  - [pg_get_statisticsobj_worker](pg_get_statisticsobj_worker.md) (worker function that generates the statistics definition)
  - string_to_text (converts C string to PostgreSQL text type)
  - PG_RETURN_TEXT_P (macro to return text value to SQL caller)
  - PG_RETURN_NULL (macro to return NULL to SQL caller)
- Called from (representative examples):
  - No direct references found (likely called from SQL queries or system functions)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called from SQL
- Returns NULL if the statistics object does not exist or cannot be accessed
- The function uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of PostgreSQL's extended statistics system introduced for better query planning with correlated columns
- The returned text contains a complete CREATE STATISTICS statement that could be used to recreate the statistics object
- Handles memory management automatically through PostgreSQL's palloc/pfree system
- The function signature follows PostgreSQL's convention for system catalog functions
# pg_get_statisticsobjdef_columns

## Location
[src/backend/utils/adt/ruleutils.c:1617-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1617-L1633)

## Overview
A PostgreSQL system function that retrieves the column list and expressions for an extended statistics object and returns them as formatted text.

## Definition
```c
Datum pg_get_statisticsobjdef_columns(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function (accessible via SQL) that extracts and formats the columns and expressions associated with an extended statistics object. It serves as a user-facing interface to inspect the structure of statistics objects, showing which columns and expressions are included in the statistics collection. The function handles the conversion from internal representation to a user-readable text format and properly handles cases where the statistics object might not exist.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides:
  - `statextid`: The OID of the statistics object to examine (retrieved via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_statisticsobj_worker](pg_get_statisticsobj_worker.md)
  - [string_to_text](../s/string_to_text.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL queries
- Returns NULL if the statistics object does not exist
- Uses pretty-printing format (true, true parameters to worker function)
- The function follows standard PostgreSQL function conventions with Datum return type
- Converts the result to PostgreSQL text type for SQL compatibility
- Part of the ruleutils module which handles object definition formatting
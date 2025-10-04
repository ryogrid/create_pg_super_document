# pg_replication_origin_drop

## Location
[src/backend/replication/logical/origin.c:1310-1328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1310-L1328)

## Overview
SQL-callable function that drops a replication origin by name, providing a user interface to remove replication origin entries from the system catalog.

## Definition

```c
Datum
pg_replication_origin_drop(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL SQL function wrapper for dropping replication origins. It takes a text parameter containing the origin name, validates prerequisites, converts the text parameter to a C string, and calls the internal  function to perform the actual deletion. The function is designed to be called from SQL as .

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0:  - Name of the replication origin to drop (converted internally to C string)

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that replication origins can be manipulated (not in recovery, proper configuration)
  -  - Converts PostgreSQL text datum to null-terminated C string
  -  - Performs the actual origin deletion from system catalog
  -  - Frees allocated memory for the converted string
  -  - Returns void result to PostgreSQL function call framework
- Called from (representative examples):
  - SQL interface (no direct C callers found)

## Notes and Other Information
- Must be called within a transaction context (enforced by )
- Performs prerequisite checks to ensure replication origins can be manipulated
- Uses  and  when calling the internal drop function
- Automatically handles memory management for the converted origin name string
- Part of PostgreSQL's logical replication origin management system
- Located in

## Simplified Source

```c
Datum
pg_replication_origin_drop(PG_FUNCTION_ARGS)
{
    char *name;

    // Check prerequisites (not in recovery, proper configuration)
    replorigin_check_prerequisites(false, false);

    // Convert text argument to C string
    name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));

    // Drop the replication origin by name
    replorigin_drop_by_name(name, false, true);

    pfree(name);

    PG_RETURN_VOID();
}
```
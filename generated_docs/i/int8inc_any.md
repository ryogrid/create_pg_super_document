# int8inc_any

## Location
[src/backend/utils/adt/int8.c:804-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L804-L809)

## Overview
A wrapper function for int8inc specifically designed for aggregates that count only non-null values.

## Definition
```c
Datum int8inc_any(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a simple wrapper around int8inc, designed specifically for aggregate operations that count only non-null values. Since the function is declared as strict in PostgreSQL's system catalog, null checks are performed automatically before the function is called, eliminating the need for explicit null handling within the function itself.

The function exists as a separate entry to maintain proper pg_proc catalog organization and to avoid regression test complaints about mismatched entries for built-in functions, even though it could theoretically point directly to int8inc.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:

## Dependencies
- Functions called/Symbols referenced:
  - [int8inc](int8inc.md) (the underlying increment function)
- Called from (representative examples):
  - No direct references found in codebase (likely used through PostgreSQL's aggregate system)

## Notes and Other Information
- Declared as a strict function, ensuring automatic null value handling by PostgreSQL
- Exists primarily for proper catalog organization and regression test compatibility
- Simply delegates all functionality to int8inc without additional processing
- Used specifically for aggregates counting non-null values rather than all values
- Located in src/backend/utils/adt/int8.c:804-809
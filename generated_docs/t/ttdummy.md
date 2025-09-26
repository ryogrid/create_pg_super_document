# ttdummy

## Location
[src/test/regress/regress.c:275-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L275-L463)

## Overview
This function implements a PostgreSQL trigger that manages temporal database functionality by automatically handling start and stop date columns to maintain historical record versioning.

## Definition
```c
Datum ttdummy(PG_FUNCTION_ARGS)
```

## Detailed Description
ttdummy is a sophisticated PostgreSQL trigger function that implements temporal table functionality, allowing tables to maintain historical versions of rows by managing start and stop date columns. The function validates that it's called as a BEFORE ROW trigger (not for INSERT operations), then processes UPDATE and DELETE operations by creating new historical records. For UPDATE operations, it ensures the temporal columns cannot be manually modified and creates a new row with updated temporal values. For DELETE operations, it sets the stop date to mark the record as ended. The function uses SPI (Server Programming Interface) to insert historical records and maintains consistency through proper validation and error handling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - `fcinfo->context`: Contains TriggerData with trigger-specific information
  - TriggerData fields used:
    - `tg_trigtuple`: The old tuple being modified
    - `tg_newtuple`: The new tuple (for UPDATE operations)
    - `tg_relation`: The relation being modified
    - `tg_trigger`: Trigger definition with arguments
    - `tg_event`: Event type information
- Trigger arguments (expected 2):
  - `args[0]`: Name of start date column (must be integer type)
  - `args[1]`: Name of stop date column (must be integer type)

## Dependencies
- Functions called/Symbols referenced:
  - CALLED_AS_TRIGGER (validates trigger context)
  - TRIGGER_FIRED_FOR_ROW, TRIGGER_FIRED_BEFORE, TRIGGER_FIRED_BY_INSERT, TRIGGER_FIRED_BY_UPDATE (trigger event validation)
  - [SPI_getrelname](../S/SPI_getrelname.md), SPI_fnumber, SPI_gettypeid, SPI_getbinval (SPI data access functions)
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_keepplan, SPI_execp, SPI_modifytuple, SPI_finish (SPI execution functions)
  - DirectFunctionCall1, nextval (sequence value generation)
  - TTDUMMY_INFINITY (constant for infinite date values)
  - [palloc](../p/palloc.md), pfree (PostgreSQL memory management)
  - elog, ereport (error reporting)
- Called from (representative examples):
  - Referenced by TTDUMMY_INFINITY constant

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite demonstrating temporal table implementation
- Requires exactly 2 arguments specifying the start and stop date column names
- Only works with BEFORE ROW triggers and prohibits INSERT operations
- Maintains historical integrity by preventing manual modification of temporal columns
- Uses a sequence ('ttdummy_seq') to generate timestamp values for temporal columns
- Creates audit trail by inserting historical records before modifying current data
- The global variable 'ttoff' can disable the temporal functionality when set
- Implements sophisticated validation to ensure data consistency and proper temporal semantics
- Located in src/test/regress/regress.c, primarily used for testing temporal database patterns
- Demonstrates advanced PostgreSQL trigger programming including SPI usage and plan caching
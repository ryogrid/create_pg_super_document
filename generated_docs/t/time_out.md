# time_out

## Location
[src/backend/utils/adt/date.c:1501-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1501-L1520)

## Overview
PostgreSQL function that converts a TimeADT value to its string representation for output, implementing the time data type's output function.

## Definition
```c
Datum time_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The time_out function serves as the output function for PostgreSQL's time data type. It takes a TimeADT value as input, converts it to broken-down time components using time2tm, then formats it as a human-readable string using EncodeTimeOnly. The function follows PostgreSQL's function call conventions, using PG_FUNCTION_ARGS for parameter handling and returning a Datum. The resulting string format depends on the current DateStyle setting.

## Parameters / Member Variables
- Input: TimeADT value accessed via PG_GETARG_TIMEADT(0)
- Returns: C-string representation of the time value via PG_RETURN_CSTRING

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT (macro for parameter extraction)
  - [time2tm](time2tm.md) (time decomposition function)
  - [EncodeTimeOnly](../E/EncodeTimeOnly.md) (time formatting function)
  - [pstrdup](../p/pstrdup.md) (string duplication function)
  - PG_RETURN_CSTRING (return value macro)
- Types used:
  - TimeADT (time abstract data type)
  - [pg_tm](../p/pg_tm.md) (time structure)
  - fsec_t (fractional seconds type)
  - Datum (PostgreSQL function return type)
- Constants used:
  - MAXDATELEN (maximum date string length)
  - DateStyle (global formatting setting)
- Called from (representative examples):
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md)

## Notes and Other Information
- Registered as the output function for the time data type in PostgreSQL's type system
- Uses a local buffer of MAXDATELEN + 1 characters for formatting
- Returns a palloc'd string that will be automatically freed by PostgreSQL's memory management
- Respects the current DateStyle setting for output formatting
- Part of PostgreSQL's type input/output infrastructure in src/backend/utils/adt/date.c
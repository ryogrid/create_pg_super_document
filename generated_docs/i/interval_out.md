# interval_out

## Location
[src/backend/utils/adt/timestamp.c:982-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L982-L1005)

## Overview
Converts PostgreSQL's internal Interval data type to its external string representation, handling both finite intervals and special infinite values.

## Definition
```c
Datum interval_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_out` function is the output conversion function for PostgreSQL's interval data type. It takes an internal Interval structure and converts it to a human-readable string representation. The function handles two main cases: finite intervals (which are converted through the standard encoding pipeline) and special infinite intervals (which use special encoding). The output format is controlled by the global `IntervalStyle` setting, allowing for different presentation styles (PostgreSQL, ISO8601, etc.).

## Parameters / Member Variables
- `span` (PG_GETARG_INTERVAL_P(0)): The input Interval structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_NOT_FINITE (macro to check for infinite intervals)
  - [EncodeSpecialInterval](../E/EncodeSpecialInterval.md) (handles infinite interval encoding)
  - [interval2itm](interval2itm.md) (converts Interval to internal time structure)
  - [EncodeInterval](../E/EncodeInterval.md) (encodes finite intervals to string)
  - IntervalStyle (global variable controlling output format)
  - [pstrdup](../p/pstrdup.md) (duplicates string in PostgreSQL memory context)
  - PG_RETURN_CSTRING (macro for returning C string result)
- Called from (representative examples):
  - [timetz_izone](../t/timetz_izone.md) (src/backend/utils/adt/date.c:3133, 3140)
  - [timestamp_izone](../t/timestamp_izone.md) (src/backend/utils/adt/timestamp.c:6243, 6250)
  - [timestamptz_izone](../t/timestamptz_izone.md) (src/backend/utils/adt/timestamp.c:6480, 6487)
  - [flatten_set_variable_args](../f/flatten_set_variable_args.md) (src/backend/utils/misc/guc_funcs.c:282)

## Notes and Other Information
- Uses a branching approach to handle finite vs infinite intervals differently
- Output format depends on the global `IntervalStyle` setting (PostgreSQL, ISO8601, SQL standard, etc.)
- The function allocates memory for the result string using `pstrdup` to ensure proper memory management
- Supports special interval values representing positive and negative infinity
- The internal `pg_itm` structure is used as an intermediate representation for encoding
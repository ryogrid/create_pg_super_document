# interval_in

## Location
[src/backend/utils/adt/timestamp.c:900-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L900-L981)

## Overview
Converts a string representation to PostgreSQL's internal Interval data type, supporting multiple input formats including standard SQL interval syntax and ISO8601 format.

## Definition
```c
Datum interval_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_in` function is the input conversion function for PostgreSQL's interval data type. It parses string representations of time intervals and converts them to PostgreSQL's internal Interval structure. The function supports multiple input formats through a sophisticated parsing pipeline that first attempts standard PostgreSQL datetime parsing, then falls back to ISO8601 interval format if the initial parsing fails. It handles special interval values like infinity (early/late) and applies type modifiers for precision control.

## Parameters / Member Variables
- `str` (PG_GETARG_CSTRING(0)): Input string representation of the interval
- `typelem` (unused): Type element OID (not currently used)
- `typmod` (PG_GETARG_INT32(2)): Type modifier specifying interval precision and range restrictions
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](../P/ParseDateTime.md) (initial parsing attempt)
  - [DecodeInterval](../D/DecodeInterval.md) (decode parsed fields into interval)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (fallback ISO8601 parsing)
  - [itmin2interval](itmin2interval.md) (convert internal time structure to Interval)
  - [AdjustIntervalForTypmod](../A/AdjustIntervalForTypmod.md) (apply type modifier constraints)
  - [DateTimeParseError](../D/DateTimeParseError.md) (error reporting)
  - INTERVAL_RANGE, INTERVAL_FULL_RANGE (typmod handling)
  - INTERVAL_NOEND, INTERVAL_NOBEGIN (special infinity values)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md) (src/backend/commands/variable.c:299)
  - [flatten_set_variable_args](../f/flatten_set_variable_args.md) (src/backend/utils/misc/guc_funcs.c:276)

## Notes and Other Information
- Supports multiple input formats: standard PostgreSQL syntax and ISO8601 intervals
- Handles special values for infinite intervals (DTK_LATE, DTK_EARLY)
- Uses a two-stage parsing approach with fallback for better format compatibility
- Applies precision and range constraints through typmod processing
- Uses soft error handling through escontext for better error reporting
- The function initializes a pg_itm_in structure to zero before parsing to ensure clean state
# to_timestamp

## Location
src/backend/utils/adt/formatting.c: 4368 - 4406

## Overview
SQL callable function that parses a formatted date/time string into a TIMESTAMP value according to a specified format template, implementing the `to_timestamp(text, format)` SQL function.

## Definition  
```c
Datum to_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for parsing formatted date/time strings into TIMESTAMP values, serving as the reverse operation of `to_char()` for timestamps. It implements PostgreSQL's `to_timestamp()` SQL function, which converts human-readable date/time strings into internal timestamp representation.

The function delegates the core parsing work to `do_to_timestamp()`, which handles the complex logic of interpreting format codes and extracting date/time components from the input string. The result includes broken-down time components, fractional seconds, timezone information, and precision specifications.

After parsing, the function handles timezone resolution. If the input string contained timezone information (indicated by `ftz.has_tz`), it uses the parsed timezone offset. Otherwise, it determines the appropriate timezone using the session's timezone setting via `DetermineTimeZoneOffset()`.

The final step converts the broken-down components into PostgreSQL's internal timestamp format using `tm2timestamp()`. If a specific fractional precision was specified in the format string, `AdjustTimestampForTypmod()` applies the precision adjustment to match the expected output format.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `text *date_txt` - The formatted date/time string to parse
  - Argument 1: `text *fmt` - The format string template specifying the input format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GET_COLLATION  
  - [do_to_timestamp](../d/do_to_timestamp.md) (core parsing engine)
  - DetermineTimeZoneOffset
  - [tm2timestamp](tm2timestamp.md) (converts broken-down time to timestamp)
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md) (applies fractional precision)
  - PG_RETURN_TIMESTAMP
- Called from (representative examples):
  - SQL queries using to_timestamp() function
  - Direct SQL function calls

## Notes and Other Information
- Reverse operation of to_char() for timestamps - parses formatted strings back to timestamps
- Handles timezone information: uses parsed timezone if present, otherwise session timezone  
- Supports fractional second precision control through format specifications
- Error handling for timestamp values outside representable range with ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
- Part of PostgreSQL's public SQL function interface, accessible via SQL to_timestamp() calls
- Uses session_timezone as fallback when no timezone specified in input
- Precision adjustment ensures output matches expected timestamp typmod specifications
- Complex parsing logic handled by do_to_timestamp() enables flexible input format support
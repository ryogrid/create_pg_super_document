# parse_datetime

## Location
src/backend/utils/adt/formatting.c: 4455 - 4617

## Overview
A comprehensive datetime parsing function that converts a text string to various PostgreSQL datetime types based on the format string's components, automatically determining the appropriate return type.

## Definition
```c
Datum parse_datetime(text *date_txt, text *fmt, Oid collid, bool strict,
                    Oid *typid, int32 *typmod, int *tz,
                    Node *escontext)
```

## Detailed Description
The `parse_datetime` function is a versatile datetime parsing utility that analyzes both the format string and input string to determine the most appropriate PostgreSQL datetime type to return. Unlike fixed-type parsing functions, this function dynamically selects the output type based on the presence of date, time, and timezone components in the format string.

The function supports multiple output types:
- **DateADT** (date only): When only date components are present
- **TimeADT** (time only): When only time components are present  
- **TimeTzADT** (time with timezone): When time and timezone components are present
- **Timestamp** (timestamp without timezone): When date and time components are present
- **TimestampTz** (timestamp with timezone): When date, time, and timezone components are present

The function uses `do_to_timestamp` for the core parsing logic and then converts the results to the appropriate type based on detected flags. It includes comprehensive error handling through the escontext mechanism, allowing for both exception-based and soft error reporting.

## Parameters / Member Variables
- `date_txt` (text*): The input date/time string to be parsed
- `fmt` (text*): The format string specifying how to interpret the input string
- `collid` (Oid): The collation ID for case-folding rules in string operations
- `strict` (bool): Whether to use standard (strict) parsing mode
- `typid` (Oid*): Output parameter receiving the OID of the determined data type
- `typmod` (int32*): Output parameter receiving the type modifier (precision for fractional seconds)
- `tz` (int*): Output parameter receiving timezone offset when applicable
- `escontext` (Node*): Error context for soft error handling (NULL for exception throwing)

## Dependencies
- Functions called/Symbols referenced:
  - `[do_to_timestamp](../d/do_to_timestamp.md)` - Core parsing logic
  - `[tm2timestamp](../t/tm2timestamp.md)` - Convert tm struct to timestamp
  - `[tm2timetz](../t/tm2timetz.md)` - Convert tm struct to time with timezone
  - `[tm2time](../t/tm2time.md)` - Convert tm struct to time
  - `[date2j](../d/date2j.md)` - Convert date to Julian day
  - `IS_VALID_JULIAN` - Validate Julian date range
  - `IS_VALID_DATE` - Validate date range
  - `[AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)` - Apply precision constraints
  - `[AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)` - Apply time precision constraints
  - Various datum conversion functions (TimestampTzGetDatum, DateADTGetDatum, etc.)
- Called from (representative examples):
  - `[executeDateTimeMethod](../e/executeDateTimeMethod.md)` - JSON path datetime operations
  - Header definitions in formatting.h

## Notes and Other Information
- The function performs intelligent type detection using DCH_DATED, DCH_TIMED, and DCH_ZONED flags from the format analysis
- Supports both strict and non-strict parsing modes, with different error handling behaviors
- Includes comprehensive validation for date ranges and timestamp overflow conditions
- Uses the escontext mechanism for modern PostgreSQL error handling, allowing callers to choose between exceptions and soft error returns
- The typmod parameter captures fractional second precision for temporal types
- Timezone information is extracted and returned separately when timezone components are present
- Validates format string consistency (e.g., prevents zoned but not timed formats)
- Part of PostgreSQL's advanced formatting system supporting dynamic type inference
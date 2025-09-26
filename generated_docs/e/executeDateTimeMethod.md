# executeDateTimeMethod

## Location
[src/backend/utils/adt/jsonpath_exec.c:2339-2819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2339-L2819)

## Overview
Implements JSON path datetime methods (.datetime(), .date(), .time(), .time_tz(), .timestamp(), .timestamp_tz()) that convert string values to PostgreSQL datetime types.

## Definition
```c
static JsonPathExecResult executeDateTimeMethod(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
```

## Detailed Description
The `executeDateTimeMethod` function provides comprehensive datetime conversion functionality for JSON path expressions in PostgreSQL. It converts string representations of dates and times into appropriate PostgreSQL datetime types based on the specific method called. The function supports both template-based parsing (for .datetime()) and ISO format parsing (for other methods). It handles type conversions between different datetime types, validates input formats, applies optional time precision parameters, and manages timezone information appropriately.

## Parameters / Member Variables  
- `cxt`: JsonPathExecContext pointer providing execution context and timezone usage settings
- `jsp`: JsonPathItem pointer representing the datetime method being executed
- `jb`: JsonbValue pointer to the input string value to convert
- `found`: JsonValueList pointer for collecting matching datetime values

## Dependencies
- Functions called/Symbols referenced:
  - [getScalar](../g/getScalar.md): Converts input to string scalar
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md): Converts C strings to PostgreSQL text
  - [parse_datetime](../p/parse_datetime.md): Core datetime parsing function with template support
  - [jspGetArg](../j/jspGetArg.md)/jspGetString/jspGetNumeric: Extract arguments from JSON path items
  - DirectFunctionCall1: Execute PostgreSQL type conversion functions
  - [checkTimezoneIsUsedForCast](../c/checkTimezoneIsUsedForCast.md): Validate timezone usage in conversions
  - Various datetime conversion functions (timestamp_date, timetz_time, etc.)
  - [anytime_typmod_check](../a/anytime_typmod_check.md)/anytimestamp_typmod_check: Validate time precision
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)/AdjustTimestampForTypmod: Apply precision to datetime values
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md): Calculate timezone offsets
  - [executeNextItem](executeNextItem.md): Continue JSON path evaluation
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md): Main item execution dispatcher
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathExecResult (jperOk on success, jperError on failure, jperNotFound if no format matches)
- Supports multiple ISO formats for automatic format detection when no template is provided
- .datetime() method accepts optional format template; other methods use predefined ISO formats
- Methods except .datetime() and .date() support optional time precision arguments
- Handles comprehensive type conversions between all PostgreSQL datetime types (date, time, timetz, timestamp, timestamptz)
- Manages timezone information separately in JsonbValue structure for proper JSON representation
- Caches compiled format templates in static array for performance optimization
- Validates input strings must be convertible to string scalars; non-string inputs cause errors
- Part of PostgreSQL's JSON path expression evaluation system for datetime operations
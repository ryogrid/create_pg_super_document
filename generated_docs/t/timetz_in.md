# timetz_in

## Location
[src/backend/utils/adt/date.c:2273-2313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2273-L2313)

## Overview
The timetz_in function is PostgreSQL's input function for the TIME WITH TIME ZONE data type, responsible for parsing string representations of time values with timezone information into the internal TimeTzADT format.

## Definition
```c
Datum timetz_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the primary input parser for TIME WITH TIME ZONE values in PostgreSQL. It takes a string representation of a time with timezone (e.g., "14:30:00+05:30") and converts it to the internal TimeTzADT structure.

The parsing process involves several steps:
1. Parse the input string using ParseDateTime to break it into components
2. Decode the time-only components using DecodeTimeOnly to extract hours, minutes, seconds, fractional seconds, and timezone
3. Handle any parsing errors by reporting them appropriately
4. Convert the parsed components to TimeTzADT format using tm2timetz
5. Apply any type modifier constraints (precision limits) using AdjustTimeForTypmod
6. Return the resulting TimeTzADT value

The function handles various time formats and timezone representations, making it flexible for different input styles while maintaining strict validation.

## Parameters / Member Variables
- `str`: Input string containing the time with timezone representation to parse
- `typelem`: Type element OID (currently unused, marked with NOT_USED)
- `typmod`: Type modifier specifying precision constraints for the time value
- `escontext`: Error context for proper error handling and reporting

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](../P/ParseDateTime.md)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md)
  - DateTimeParseError
  - [tm2timetz](tm2timetz.md)
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)
  - PG_RETURN_TIMETZADT_P
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL's type system during SQL parsing)

## Notes and Other Information
- This is the primary input function for the TIME WITH TIME ZONE data type in PostgreSQL
- Located in src/backend/utils/adt/date.c as part of the date/time ADT implementations
- Handles comprehensive error reporting through DateTimeParseError when parsing fails
- Supports various time and timezone formats through the underlying parsing infrastructure
- The function allocates memory for the result using palloc, which is automatically managed by PostgreSQL's memory context system
- Type modifiers are applied to enforce precision constraints on fractional seconds
- Returns NULL on parsing errors after proper error reporting
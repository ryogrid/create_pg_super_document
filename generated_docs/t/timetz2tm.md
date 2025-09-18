# timetz2tm

## Location
[src/backend/utils/adt/date.c:2403-2424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2403-L2424)

## Overview
Converts a PostgreSQL TIMETZ (time with time zone) value to a POSIX-style time structure, decomposing it into hours, minutes, seconds, fractional seconds, and timezone offset.

## Definition


## Detailed Description
The `timetz2tm` function is a utility function that breaks down a PostgreSQL TIMETZ value into its constituent components for easier manipulation and formatting. It takes a compact TIMETZ representation (microseconds since midnight plus timezone offset) and decomposes it into a POSIX-style `pg_tm` structure with separate fields for hours, minutes, and seconds, plus fractional seconds and timezone information.

The function performs integer arithmetic to extract each time component by successively dividing by the appropriate time unit constants (USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC) and calculating remainders. This approach avoids floating-point arithmetic and maintains precision.

## Parameters / Member Variables
- `time`: Pointer to the input TIMETZ value containing time (in microseconds) and zone offset
- `tm`: Pointer to output POSIX time structure to be filled with hour, minute, second components  
- `fsec`: Pointer to output fractional seconds (in microseconds)
- `tzp`: Pointer to output timezone offset in seconds (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - TimeTzADT: Input structure type containing time and zone components
  - [pg_tm](../p/pg_tm.md): Standard PostgreSQL time structure
  - TimeOffset: Type for time offset calculations
  - USECS_PER_HOUR: Conversion constant (3600000000 microseconds)
  - USECS_PER_MINUTE: Conversion constant (60000000 microseconds) 
  - USECS_PER_SEC: Conversion constant (1000000 microseconds)
- Called from (representative examples):
  - [timetz_out](timetz_out.md): For text output formatting
  - [timetz_part_common](timetz_part_common.md): For EXTRACT() function implementation
  - JsonEncodeDateTime: For JSON serialization

## Notes and Other Information
- Returns 0 on success (currently no failure conditions exist)
- The timezone offset is stored in seconds and can be negative for timezones west of UTC
- Fractional seconds are returned in microseconds for maximum precision
- The function performs no validation - it assumes the input TIMETZ value is valid
- Part of the core time/date ADT implementation and widely used throughout PostgreSQL's datetime handling
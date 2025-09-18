# check_timezone

## Location
[src/backend/commands/variable.c:261-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L261-L380)

## Overview
A GUC (Grand Unified Configuration) validation hook function that parses and validates timezone configuration strings, supporting multiple formats including timezone names, numeric offsets, and SQL INTERVAL syntax.

## Definition


## Detailed Description
The  function serves as a GUC check hook that validates and processes the  configuration parameter in PostgreSQL. It supports three different input formats:

1. **INTERVAL format**: SQL-compliant syntax like  or 
2. **Numeric format**: Simple numeric hours offset like  or   
3. **Timezone name format**: Named timezones like , , or 

The function performs comprehensive validation including:
- Parsing INTERVAL strings and ensuring they contain only time components (no months or days)
- Converting numeric offsets to valid timezone objects
- Loading and validating named timezone definitions
- Checking that timezones don't use leap seconds (which PostgreSQL doesn't support)
- Verifying UTC offsets are within acceptable ranges

Upon successful validation, the function creates a pg_tz timezone object and stores it in the extra data for use by the assignment function.

## Parameters / Member Variables
- : Double pointer to the input timezone string to be validated
- : Double pointer that will contain a pg_tz pointer for the validated timezone object
- : The source of the GUC setting (file, command line, etc.) - used for logging and validation context

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strncasecmp](../p/pg_strncasecmp.md): Case-insensitive string comparison for parsing INTERVAL keyword
  - DatumGetIntervalP, DirectFunctionCall3, interval_in: PostgreSQL interval parsing functions
  - [CStringGetDatum](../C/CStringGetDatum.md), ObjectIdGetDatum, Int32GetDatum: PostgreSQL datum conversion functions
  - [pg_tzset_offset](../p/pg_tzset_offset.md): Creates timezone object from GMT offset in seconds
  - [pg_tzset](../p/pg_tzset.md): Loads timezone by name from system timezone database
  - [pg_tz_acceptable](../p/pg_tz_acceptable.md): Validates that timezone doesn't use leap seconds
  - GUC_check_errdetail, GUC_check_errmsg: GUC error reporting functions
  - [guc_malloc](../g/guc_malloc.md): GUC memory allocation function
  - USECS_PER_SEC, SECS_PER_HOUR: Time conversion constants

- Called from (representative examples):
  - GUC system during timezone configuration validation

## Notes and Other Information
- Supports SQL standard INTERVAL syntax for compliance, though it has limited practical use
- INTERVAL format restricts input to time-only intervals (no months or days allowed)
- Sign convention conversion: SQL uses positive for east of GMT, Unix uses negative
- Named timezone validation includes leap seconds check since PostgreSQL doesn't support them
- UTC offset validation ensures values are within reasonable bounds (typically ±12-14 hours)
- The extra data contains a single pg_tz pointer for use by assign_timezone
- Memory management follows GUC conventions with guc_malloc for persistent allocations
- Error messages provide detailed feedback for different failure modes
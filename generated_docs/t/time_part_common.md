# time_part_common

## Location
src/backend/utils/adt/date.c: 2140 - 2242

## Overview
The  function extracts specified time components (hour, minute, second, etc.) from a TimeADT value, with support for both numeric and floating-point return formats.

## Definition


## Detailed Description
This function is the core implementation for extracting time components from a time data type in PostgreSQL. It processes a text string specifying which time component to extract (e.g., 'hour', 'minute', 'second') and returns the corresponding value from a TimeADT input. The function supports multiple precision levels for seconds (microseconds, milliseconds, seconds) and handles special cases like epoch conversion. It can return results either as numeric values (when retnumeric is true) or as floating-point values (when retnumeric is false).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Text string specifying the time component to extract
  - Argument 1:  - The time value to extract the component from
- : Boolean flag determining return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP, PG_GETARG_TIMEADT
  - downcase_truncate_identifier (text processing)
  - DecodeUnits, DecodeSpecial (time unit parsing)
  - time2tm (time conversion)
  - int64_div_fast_to_numeric, int64_to_numeric (numeric conversion)
  - PG_RETURN_NUMERIC, PG_RETURN_FLOAT8 (return macros)
  - ereport (error reporting)
- Data types used:
  - TimeADT, text, fsec_t, pg_tm
  - Various DTK constants (DTK_HOUR, DTK_MINUTE, etc.)
- Called from (representative examples):
  - time_part (src/backend/utils/adt/date.c:2245)
  - extract_time (src/backend/utils/adt/date.c:2251)

## Notes and Other Information
- The function is static and serves as the common implementation for both time_part() and extract_time()
- Supports extraction of: microseconds, milliseconds, seconds, minutes, hours, and epoch
- Rejects unsupported time units (day, month, year, etc.) with appropriate error messages
- For sub-second precision, uses high-precision arithmetic to maintain accuracy
- The epoch extraction returns seconds since Unix epoch as a fractional value
- Input unit names are case-insensitive due to downcase_truncate_identifier processing
- Error handling includes both ERRCODE_FEATURE_NOT_SUPPORTED and ERRCODE_INVALID_PARAMETER_VALUE
- Located in 
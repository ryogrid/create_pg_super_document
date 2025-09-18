# timetz_part_common

## Location
src/backend/utils/adt/date.c: 2927 - 3043

## Overview
A common implementation function that extracts specified time components (hour, minute, second, timezone, etc.) from a time with timezone (TimeTzADT) value, with support for both numeric and floating-point return types.

## Definition
```c
static Datum timetz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
This static function provides the core implementation for extracting various time components from TimeTzADT values. It handles a wide range of extraction units including timezone information (DTK_TZ, DTK_TZ_MINUTE, DTK_TZ_HOUR), time components (DTK_HOUR, DTK_MINUTE, DTK_SECOND, DTK_MICROSEC, DTK_MILLISEC), and special values like epoch time. The function parses the unit specification string, converts the TimeTzADT to a broken-down time structure, and extracts the requested component. It supports returning results as either numeric values (when retnumeric is true) or floating-point values, with special handling for fractional seconds and milliseconds. The function includes comprehensive error checking for unsupported units and properly handles timezone-specific extractions.

## Parameters / Member Variables
- Input parameter 0 (via PG_GETARG_TEXT_PP(0)): A text value specifying the unit to extract (e.g., 'hour', 'minute', 'timezone')
- Input parameter 1 (via PG_GETARG_TIMETZADT_P(1)): A TimeTzADT pointer representing the time with timezone value
- : Boolean flag determining return type (numeric if true, float8 if false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Macro to extract text argument
  - PG_GETARG_TIMETZADT_P: Macro to extract TimeTzADT argument
  - downcase_truncate_identifier: Function to normalize unit strings
  - DecodeUnits/DecodeSpecial: Functions to parse time unit specifications
  - timetz2tm: Function to convert TimeTzADT to broken-down time structure
  - int64_div_fast_to_numeric/int64_to_numeric: Numeric conversion functions
  - PG_RETURN_NUMERIC/PG_RETURN_FLOAT8: Macros to return results
  - ereport/errcode/errmsg: Error reporting functions
- Constants used:
  - DTK_TZ, DTK_TZ_MINUTE, DTK_TZ_HOUR: Timezone extraction constants
  - DTK_MICROSEC, DTK_MILLISEC, DTK_SECOND, DTK_MINUTE, DTK_HOUR: Time unit constants
  - DTK_EPOCH: Epoch time constant
  - SECS_PER_MINUTE, MINS_PER_HOUR, SECS_PER_HOUR: Time conversion constants
  - INT64CONST: 64-bit integer constant macro
- Types used:
  - TimeTzADT: Time with timezone data type
  - pg_tm: Broken-down time structure
  - fsec_t: Fractional seconds type
- Called from (representative examples):
  - timetz_part: Public function for EXTRACT() with float8 return
  - extract_timetz: Public function for EXTRACT() with numeric return

## Notes and Other Information
- This is a static (internal) function shared by timetz_part and extract_timetz
- Supports extraction of timezone information with proper sign handling (negative for western timezones)
- Handles fractional seconds with microsecond precision
- Returns NULL for unsupported date-related units (day, month, year, etc.) since TimeTzADT contains no date information
- Epoch extraction returns seconds since Unix epoch adjusted for timezone
- The retnumeric parameter allows the same logic to serve both numeric and floating-point EXTRACT functions
- Located in src/backend/utils/adt/date.c with other date/time utility functions
- Comprehensive error handling with appropriate error codes for invalid or unsupported units
- Special handling for millisecond and second extraction to maintain precision when returning numeric values
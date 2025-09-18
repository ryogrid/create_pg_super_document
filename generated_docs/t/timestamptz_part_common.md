# timestamptz_part_common

## Location
src/backend/utils/adt/timestamp.c: 5626 - 5882

## Overview
The core implementation function that extracts specified date/time fields from timestamp with time zone values, handling timezone conversion and supporting both floating-point and numeric return types.

## Definition
```c
static Datum timestamptz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
The `timestamptz_part_common` function is the shared implementation for extracting date/time components from timestamp with time zone (timestamptz) values. Unlike its timestamp counterpart, this function must handle timezone conversions and timezone-specific fields like timezone offset hours/minutes. The function supports extracting a wide variety of temporal components including standard date/time parts (year, month, day, hour, minute, second), timezone components (timezone offset), ISO standards (ISO year, ISO week, ISO day of week), and special values (Julian day, epoch seconds).

The function handles both finite and infinite timestamps, with special logic for infinite values. It supports two return types controlled by the `retnumeric` parameter: floating-point numbers (float8) for compatibility and precision numeric types for higher accuracy, especially important for fractional seconds and epoch calculations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Argument 0 (`units`): Text specifying the field to extract (e.g., 'year', 'month', 'timezone', etc.)
  - Argument 1 (`timestamp`): TimestampTz value to extract from
- `retnumeric`: Boolean flag controlling return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - `downcase_truncate_identifier` - Normalizes field name input
  - `DecodeUnits`, `DecodeSpecial` - Parse field name tokens  
  - `NonFiniteTimestampTzPart` - Handles infinite timestamp values
  - `timestamp2tm` - Converts timestamp to broken-down time structure
  - `date2isoweek`, `date2isoyear`, `date2j`, `j2day` - Date calculation utilities
  - `int64_div_fast_to_numeric`, `numeric_add_opt_error` - Numeric type operations
  - `SetEpochTimestamp` - Gets PostgreSQL epoch reference point
  - Various PostgreSQL datum conversion macros
- Called from (representative examples):
  - `timestamptz_part` - Float8 variant of field extraction
  - `extract_timestamptz` - Numeric variant of field extraction

## Notes and Other Information
- Handles timezone-specific fields: DTK_TZ (timezone offset in seconds), DTK_TZ_MINUTE, DTK_TZ_HOUR
- Supports both standard and ISO date/time standards (ISO year, ISO week, ISO day of week)  
- Special handling for BCE years in decade/century/millennium calculations
- Precision handling for fractional seconds using microsecond internal representation
- Julian day calculations support both integer and fractional parts for precise astronomical use
- Comprehensive error handling for unsupported or unrecognized field names
- Performance optimization for epoch calculations to avoid precision loss with large timestamps
- Located in `src/backend/utils/adt/timestamp.c:5626-5882`
- Static function - only accessible within the timestamp.c compilation unit
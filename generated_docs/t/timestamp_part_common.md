# timestamp_part_common

## Location
src/backend/utils/adt/timestamp.c: 5353 - 5610

## Overview
Core implementation function for extracting specific date/time components from timestamp values, supporting both float8 and numeric return types.

## Definition
```c
static Datum timestamp_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
The `timestamp_part_common` function is the central implementation for extracting date/time parts from timestamp values. It serves as the backend for both `timestamp_part()` and `extract_timestamp()` functions. The function handles:

1. **Unit parsing**: Converts string unit names to internal constants
2. **Infinite timestamp handling**: Uses `NonFiniteTimestampTzPart` for infinite values
3. **Finite timestamp processing**: Breaks down timestamps into component parts
4. **Multiple unit types**: Supports time units (seconds, minutes, hours), date units (days, months, years), and special units (epoch, Julian day)
5. **Dual return modes**: Can return either float8 or numeric values based on the `retnumeric` parameter

The function handles complex calculations for derived units like quarters, decades, centuries, millennia, ISO years, and day-of-week/year calculations. It also manages precision issues when dealing with fractional seconds and large timestamp values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `units`: Text specifying the unit to extract (e.g., 'year', 'month', 'second')
  - `timestamp`: The timestamp value to extract from
- `retnumeric`: Boolean flag determining return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - downcase_truncate_identifier (unit string processing)
  - DecodeUnits, DecodeSpecial (unit parsing)
  - NonFiniteTimestampTzPart (infinite timestamp handling)
  - timestamp2tm (timestamp decomposition)
  - date2isoweek, date2isoyear (ISO date calculations)
  - date2j, j2day (Julian day conversions)
  - SetEpochTimestamp (epoch calculations)
  - Various numeric functions (int64_to_numeric, numeric_div_opt_error, etc.)
- Called from (representative examples):
  - timestamp_part
  - extract_timestamp

## Notes and Other Information
- Static function serving as common implementation for multiple user-facing functions
- Handles both finite and infinite timestamp values appropriately
- Supports extraction of 20+ different temporal units and components
- Implements complex calendar arithmetic for centuries, decades, and millennia
- Provides high precision numeric results when requested to avoid floating-point precision issues
- Includes extensive error handling for invalid or unsupported units
- Central to PostgreSQL's temporal data extraction functionality across multiple SQL functions
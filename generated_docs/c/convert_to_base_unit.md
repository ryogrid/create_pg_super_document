# convert_to_base_unit

## Location
src/backend/utils/misc/guc.c: 2673 - 2730

## Overview
convert_to_base_unit converts human-friendly unit values (like "kB", "min") to PostgreSQL's internal base units for configuration parameters.

## Definition
static bool convert_to_base_unit(double value, const char *unit, int base_unit, double *base_value)

## Detailed Description
This static function performs unit conversion for PostgreSQL configuration parameters that accept human-readable units. It supports both memory units (bytes, kB, MB, GB, etc.) and time units (ms, s, min, h, d) by converting them to the system's base units.

The conversion process includes:
1. Parsing and extracting the unit string from the input (ignoring trailing whitespace)
2. Selecting the appropriate conversion table (memory or time) based on the base_unit flag
3. Searching the conversion table for a matching unit and base_unit combination
4. Applying the conversion multiplier to calculate the base value
5. Implementing special rounding logic for fractional values to the nearest smaller unit

The function includes robust input validation and handles fractional values intelligently by rounding them to meaningful boundaries.

## Parameters / Member Variables
- : The numeric value to convert
- : String containing the unit to convert from (may have trailing spaces)
- : Integer flag indicating the target base unit type (contains GUC_UNIT_MEMORY for memory units)
- : Pointer to double where the converted value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - isspace: Standard C library function for whitespace detection
  - strcmp: String comparison function
  - rint: Round to nearest integer function
  - memory_unit_conversion_table: Global table for memory unit conversions
  - time_unit_conversion_table: Global table for time unit conversions
- Called from (representative examples):
  - parse_int: Integer parameter parsing in src/backend/utils/misc/guc.c:2921
  - parse_real: Real number parameter parsing in src/backend/utils/misc/guc.c:2994

## Notes and Other Information
- Static function, only accessible within guc.c
- Maximum unit string length is limited by MAX_UNIT_LEN constant
- Supports both memory units (determined by GUC_UNIT_MEMORY flag) and time units
- Implements intelligent rounding for fractional values (e.g., "30.1GB" rounds to nearest MB)
- Returns false for unrecognized units or malformed input
- Essential for PostgreSQL's user-friendly configuration parameter syntax
- Handles trailing whitespace in unit strings gracefully
- Conversion tables are searched linearly, with exact string matching required
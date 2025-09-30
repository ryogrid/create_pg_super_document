# convert_to_base_unit

## Location
[src/backend/utils/misc/guc.c:2673-2730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2673-L2730)

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
  - [parse_int](../p/parse_int.md): Integer parameter parsing in src/backend/utils/misc/guc.c:2921
  - [parse_real](../p/parse_real.md): Real number parameter parsing in src/backend/utils/misc/guc.c:2994

## Notes and Other Information
- Static function, only accessible within guc.c
- Maximum unit string length is limited by MAX_UNIT_LEN constant
- Supports both memory units (determined by GUC_UNIT_MEMORY flag) and time units
- Implements intelligent rounding for fractional values (e.g., "30.1GB" rounds to nearest MB)
- Returns false for unrecognized units or malformed input
- Essential for PostgreSQL's user-friendly configuration parameter syntax
- Handles trailing whitespace in unit strings gracefully
- Conversion tables are searched linearly, with exact string matching required

## Simplified Source

```c
static bool
convert_to_base_unit(double value, const char *unit, int base_unit, double *base_value)
{
    char unitstr[MAX_UNIT_LEN + 1];
    int unitlen;
    const unit_conversion *table;
    int i;

    // Extract unit string, ignoring whitespace
    unitlen = 0;
    while (*unit != '\0' && !isspace(*unit) && unitlen < MAX_UNIT_LEN)
        unitstr[unitlen++] = *(unit++);
    unitstr[unitlen] = '\0';

    // Skip trailing whitespace
    while (isspace(*unit))
        unit++;
    if (*unit != '\0')
        return false;  // Invalid unit format

    // Select conversion table based on unit type
    if (base_unit & GUC_UNIT_MEMORY)
        table = memory_unit_conversion_table;
    else
        table = time_unit_conversion_table;

    // Search for matching unit and base_unit in conversion table
    for (i = 0; *table[i].unit; i++)
    {
        if (base_unit == table[i].base_unit && strcmp(unitstr, table[i].unit) == 0)
        {
            double converted_value = value * table[i].multiplier;

            // Round fractional values to next smaller unit if available
            if (*table[i + 1].unit && base_unit == table[i + 1].base_unit)
                converted_value = rint(converted_value / table[i + 1].multiplier) * table[i + 1].multiplier;

            *base_value = converted_value;
            return true;
        }
    }
    return false;
}
```
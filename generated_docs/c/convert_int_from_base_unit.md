# convert_int_from_base_unit

## Location
[src/backend/utils/misc/guc.c:2731-2772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2731-L2772)

## Overview
convert_int_from_base_unit converts PostgreSQL internal base unit values back to human-friendly units for display, selecting the largest unit that represents the value without loss.

## Definition
static void convert_int_from_base_unit(int64 base_value, int base_unit, int64 *value, const char **unit)

## Detailed Description
This static function performs the reverse operation of convert_to_base_unit, converting internal base unit values back to human-readable units for display purposes. The function implements an intelligent unit selection algorithm that chooses the greatest unit that can represent the value exactly without fractional loss.

Key features include:
1. Selection of appropriate conversion table (memory or time) based on base_unit flags
2. Iteration through conversion table entries ordered from largest to smallest units
3. Selection of the first unit that divides the base value evenly (no remainder)
4. Fallback to base unit if no larger unit provides exact representation

For example, with memory units: 1024 KB converts to 1 MB, but 1025 KB remains as 1025 KB since MB conversion would lose precision.

## Parameters / Member Variables
- : The value in base units to convert from
- : Integer flag indicating the base unit type (contains GUC_UNIT_MEMORY for memory units)
- : Pointer to int64 where the converted value will be stored
- : Pointer to const char* where the selected unit string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - rint: Round to nearest integer function
  - memory_unit_conversion_table: Global table for memory unit conversions
  - time_unit_conversion_table: Global table for time unit conversions
  - Assert: Debug assertion macro
- Called from (representative examples):
  - [ShowGUCOption](../S/ShowGUCOption.md): Display GUC option values in src/backend/utils/misc/guc.c:5507

## Notes and Other Information
- Static function, only accessible within guc.c
- Assumes conversion tables are ordered from greatest to smallest units
- Uses modulo operation to ensure exact conversion without loss
- Returns the first unit that provides exact division (no remainder)
- Essential for user-friendly display of configuration parameter values
- Always sets a valid unit (Assert ensures *unit != NULL)
- Handles both memory and time units through the same interface
- Part of PostgreSQL's bidirectional unit conversion system for configuration display

## Simplified Source

```c
// Simplified version of convert_int_from_base_unit
static void convert_int_from_base_unit(int64 base_value, int base_unit,
                                       int64 *value, const char **unit) {
    const unit_conversion *table;
    int i;

    *unit = NULL;

    // Select appropriate conversion table based on unit type
    if (base_unit & GUC_UNIT_MEMORY)
        table = memory_unit_conversion_table;
    else
        table = time_unit_conversion_table;

    // Find the largest unit that divides evenly into the base value
    for (i = 0; *table[i].unit; i++) {
        if (base_unit == table[i].base_unit) {
            // Check if this unit conversion results in a whole number
            if (table[i].multiplier <= 1.0 ||
                base_value % (int64) table[i].multiplier == 0) {
                // Convert to the human-friendly unit
                *value = (int64) rint(base_value / table[i].multiplier);
                *unit = table[i].unit;
                break;
            }
        }
    }

    Assert(*unit != NULL);
}
```

Key simplifications made:
- Preserved the core algorithm logic for unit selection
- Kept the essential modulo check for exact division
- Maintained the table lookup structure
- Simplified comments to focus on main functionality
- Retained all critical parameters and return logic
- Kept Assert for validation but simplified error handling context